#ifndef IMAGINE_MAPREDUCE_MAPPER_H
#define IMAGINE_MAPREDUCE_MAPPER_H

#include "MapRunner.h"
#include "ThreadPool.h"
#include "MapReduceUtil.h"
#include "MapTaskHandler.h"
#include "MapTaskService.h"
#include "LineRecordReader.h"
#include "TextOutputFormat.h"
#include "StringPartitioner.h"
#include "HeartBeatMessage.pb.h"
#include "RetrieveSplitFileService.h"

#include <memory.h>
#include <fstream>

namespace Imagine_MapReduce
{

namespace Internal
{

template <typename reader_key, typename reader_value, typename key, typename value>
class MapTaskHandler;

template <typename reader_key, typename reader_value, typename key, typename value>
class MapTaskService;

} // namespace Internal

template <typename reader_key, typename reader_value, typename key, typename value>
class Mapper
{
 public:
    Mapper();

    Mapper(const std::string& profile_name);

    Mapper(const YAML::Node& config);

    ~Mapper();

    void Init(const std::string& profile_name);

    void Init(const YAML::Node& config);

    void InitLoop(const YAML::Node& config);

    Mapper<reader_key, reader_value, key, value>* AddNewMapTaskHandler(std::shared_ptr<Internal::MapTaskHandler<reader_key, reader_value, key, value>> handler) const;

    void SetDefault();

    std::vector<std::string> GetFile(const std::vector<std::string> &input) const;

    std::shared_ptr<RecordReader<reader_key, reader_value>> GenerateRecordReader(InputSplit *split, int split_id) const;

    MapRunner<reader_key, reader_value, key, value>* GenerateMapRunner(int split_id, int split_num, const std::string& file_name, const std::string &master_ip, const std::string &master_port) const;

    Imagine_Rpc::Stub* GenerateNewStub() const;

    MapTimerCallback<reader_key, reader_value> GetTimerCallback() const;

    Mapper<reader_key, reader_value, key, value>* SetDefaultRecordReader();

    Mapper<reader_key, reader_value, key, value>* SetDefaultMapFunction();

    Mapper<reader_key, reader_value, key, value>* SetDefaultTimerCallback();

    Mapper<reader_key, reader_value, key, value>* SetDefaultOutputFormat();

    Mapper<reader_key, reader_value, key, value>* SetDefaultPartitioner();

    // Mapper定时进行任务进度汇报
    static void DefaultTimerCallback(std::shared_ptr<Imagine_Rpc::Stub>& stub, std::shared_ptr<RecordReader<reader_key, reader_value>>& reader);

    void loop();

  private:
    // 配置文件字段
    std::string ip_;
    std::string port_;
    size_t map_task_thread_num_;
    size_t max_map_task_;
    bool singleton_log_mode_;

    Logger* logger_;

 private:
    MapCallback<reader_key, reader_value, key, value> map_;             // 提供给用户自定义的map函数
    MapTimerCallback<reader_key, reader_value> timer_callback_;         // 默认注册DefaultTimerCallback, 定时向Master发送任务进度
    RecordReader<reader_key, reader_value> *record_reader_;             // split迭代器类型
    OutputFormat<key, value> *output_format_;                           // 输出类型
    Imagine_Rpc::RpcServer *rpc_server_;                                // rpc服务器对象
    pthread_t *rpc_server_thread_;                                      // 用于开启RPC服务的主线程
    Partitioner<key> *partitioner_;                                     // partition对象类型
    Imagine_Rpc::Stub* stub_;                                           // stub原型
    ThreadPool<std::shared_ptr<Internal::MapTaskHandler<reader_key, reader_value, key, value>>>* thread_pool_;
};

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper()
                                            : record_reader_(nullptr), output_format_(nullptr), partitioner_(nullptr)
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper(const std::string& profile_name) : Mapper()
{
    Init(profile_name);
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper(const YAML::Node& config) : Mapper()
{
    Init(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::~Mapper()
{
    delete rpc_server_;
    delete rpc_server_thread_;

    delete record_reader_;
    delete output_format_;
    delete partitioner_;
    delete stub_;
    delete thread_pool_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::Init(const std::string& profile_name)
{
    if (profile_name == "") {
        throw std::exception();
    }

    YAML::Node config = YAML::LoadFile(profile_name);
    Init(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::Init(const YAML::Node& config)
{
    ip_ = config["ip"].as<std::string>();
    port_ = config["port"].as<std::string>();
    map_task_thread_num_ = config["map_task_thread_num"].as<size_t>();
    max_map_task_ = config["max_map_task"].as<size_t>();
    singleton_log_mode_ = config["singleton_log_mode"].as<bool>();

    if (singleton_log_mode_) {
        logger_ = SingletonLogger::GetInstance();
    } else {
        logger_ = new NonSingletonLogger();
        Logger::SetInstance(logger_);
    }

    logger_->Init(config);

    InitLoop(config);
    stub_ = new Imagine_Rpc::Stub(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::InitLoop(const YAML::Node& config)
{
    rpc_server_thread_ = new pthread_t;
    if (!rpc_server_thread_) {
        throw std::exception();
    }

    try {
        thread_pool_ = new ThreadPool<std::shared_ptr<Internal::MapTaskHandler<reader_key, reader_value, key, value>>>(map_task_thread_num_, max_map_task_); // 初始化线程池
    } catch (...) {
        throw std::exception();
    }

    if (record_reader_ == nullptr) {
        SetDefaultRecordReader();
    }
    if (map_ == nullptr) {
        SetDefaultMapFunction();
    }
    if (output_format_ == nullptr) {
        SetDefaultOutputFormat();
    }
    if (timer_callback_ == nullptr) {
        SetDefaultTimerCallback();
    }
    if (partitioner_ == nullptr) {
        SetDefaultPartitioner();
    }

    rpc_server_ = new Imagine_Rpc::RpcServer(config);
    rpc_server_->RegisterService(new Internal::MapTaskService<reader_key, reader_value, key, value>(this));
    rpc_server_->RegisterService(new Internal::RetrieveSplitFileService());
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::AddNewMapTaskHandler(std::shared_ptr<Internal::MapTaskHandler<reader_key, reader_value, key, value>> handler) const
{
    thread_pool_->PutTask(handler);

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::shared_ptr<RecordReader<reader_key, reader_value>> Mapper<reader_key, reader_value, key, value>::GenerateRecordReader(InputSplit *split, int split_id) const
{
    std::shared_ptr<RecordReader<reader_key, reader_value>> new_record_reader = record_reader_->CreateRecordReader(split, split_id);
    new_record_reader->SetServer(rpc_server_);

    return new_record_reader;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::GenerateMapRunner(int split_id, int split_num, const std::string& file_name, const std::string &master_ip, const std::string &master_port) const
{
    return new MapRunner<reader_key, reader_value, key, value>(split_id, split_num, file_name, ip_, port_, master_ip, master_port, map_, partitioner_, output_format_, rpc_server_);
}

template <typename reader_key, typename reader_value, typename key, typename value>
Imagine_Rpc::Stub* Mapper<reader_key, reader_value, key, value>::GenerateNewStub() const
{
    return new Imagine_Rpc::Stub(*stub_);
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapTimerCallback<reader_key, reader_value> Mapper<reader_key, reader_value, key, value>::GetTimerCallback() const
{
    return timer_callback_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::SetDefault()
{
    SetDefaultMapFunction();
    SetDefaultOutputFormat();
    SetDefaultTimerCallback();
    SetDefaultPartitioner();
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::vector<std::string> Mapper<reader_key, reader_value, key, value>::GetFile(const std::vector<std::string> &input) const
{
    std::vector<std::string> output;
    std::string content;
    IMAGINE_MAPREDUCE_LOG("get file %s", &input[0][0]);
    int fd = open(&input[0][0], O_RDWR);
    while (1) {
        char buffer[1024];
        int ret = read(fd, buffer, 1024);
        for (int i = 0; i < ret; i++)
        {
            content.push_back(buffer[i]);
        }
        if (ret != 1024)
            break;
    }
    close(fd);
    if (content.size()) {
        output.push_back(content);
    }

    return output;
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::SetDefaultRecordReader()
{
    record_reader_ = new LineRecordReader();

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::SetDefaultMapFunction()
{
    map_ = [](int offset, const std::string &line_text) -> std::pair<std::string, int>
    {
        return std::make_pair(line_text, 1);
    };

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::SetDefaultTimerCallback()
{
    timer_callback_ = DefaultTimerCallback;
    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::SetDefaultPartitioner()
{
    partitioner_ = new StringPartitioner();

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::SetDefaultOutputFormat()
{
    output_format_ = new TextOutputFormat();

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::loop()
{
    pthread_create(
        rpc_server_thread_, nullptr, [](void *argv) -> void *
        {
            Imagine_Rpc::RpcServer* rpc_server = (Imagine_Rpc::RpcServer*)argv;
            rpc_server->Start();

            return nullptr;
        }, 
        this->rpc_server_);
    pthread_detach(*rpc_server_thread_);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::DefaultTimerCallback(std::shared_ptr<Imagine_Rpc::Stub>& stub, std::shared_ptr<RecordReader<reader_key, reader_value>>& reader)
{
    IMAGINE_MAPREDUCE_LOG("Reader use count is %d, reader ptr is %p", reader.use_count(), reader.get());
    if (reader.use_count() == 1) {
        reader->GetServer()->RemoveTimer(reader->GetTimerId());
        reader.reset();
        stub->CloseConnection();
        stub.reset();
        IMAGINE_MAPREDUCE_LOG("This Mapper Task Over!");
        return;
    }
    Internal::HeartBeatRequestMessage request_msg;
    Internal::HeartBeatResponseMessage response_msg;
    MapReduceUtil::GenerateHeartBeatProcessMessage(&request_msg, Internal::Identity::Mapper, reader->GetFileName(), reader->GetSplitId(), reader->GetProgress());
    IMAGINE_MAPREDUCE_LOG("Task Progress Message is Sending!");
    stub->CallConnectServer(&request_msg, &response_msg);

    if (response_msg.status_() == Internal::Status::Ok) {
    } else {
        throw std::exception();
    }
}

} // namespace Imagine_MapReduce

#endif