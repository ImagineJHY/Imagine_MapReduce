#ifndef IMAGINE_MAPREDUCE_MAPPER_H
#define IMAGINE_MAPREDUCE_MAPPER_H

#include "Imagine_Rpc/RpcServer.h"
#include "Imagine_Rpc/RpcClient.h"
#include "MapReduceUtil.h"
#include "RecordReader.h"
#include "LineRecordReader.h"
#include "MapRunner.h"
#include "OutputFormat.h"
#include "TextOutputFormat.h"
#include "common_definition.h"
#include "Partitioner.h"
#include "StringPartitioner.h"
#include "MapTaskService.h"
#include "HeartBeatMessage.pb.h"
#include "RetrieveSplitFileService.h"

#include <memory.h>
#include <fstream>

namespace Imagine_MapReduce
{

namespace Internal
{

template <typename reader_key, typename reader_value, typename key, typename value>
class MapTaskService;

} // namespace Internal

template <typename reader_key, typename reader_value, typename key, typename value>
class Mapper
{
 public:
    Mapper();

    Mapper(std::string profile_name);

    Mapper(YAML::Node config);

    ~Mapper();

    void Init(std::string profile_name);

    void Init(YAML::Node config);

    void InitLoop(YAML::Node config);

    void SetDefault();

    // Rpc通信调用
    std::vector<std::string> Map(const std::vector<std::string> &input);

    std::vector<std::string> GetFile(const std::vector<std::string> &input);

    std::shared_ptr<RecordReader<reader_key, reader_value>> GenerateRecordReader(InputSplit *split, int split_id);

    MapRunner<reader_key, reader_value, key, value>* GenerateMapRunner(int split_id, int split_num, const std::string file_name, const std::string &master_ip, const std::string &master_port);

    Imagine_Rpc::Stub* GenerateNewStub();

    MAPTIMER GetTimerCallback();

    bool SetDefaultRecordReader();

    bool SetDefaultMapFunction();

    bool SetDefaultTimerCallback();

    bool SetDefaultOutputFormat();

    bool SetDefaultPartitioner();

    static void DefaultTimerCallback(std::shared_ptr<Imagine_Rpc::Stub>& stub, std::shared_ptr<RecordReader<reader_key, reader_value>>& reader);

    void loop();

  private:
    std::string ip_;
    std::string port_;
    std::string zookeeper_ip_;
    std::string zookeeper_port_;
    // size_t reducer_num_;
    // size_t split_size_;
    size_t thread_num_;
    std::string log_name_;
    std::string log_path_;
    size_t max_log_file_size_;
    bool async_log_;
    bool singleton_log_mode_;
    std::string log_title_;
    bool log_with_timestamp_;

    std::string profile_path_;
    std::string rpc_profile_name_;

 private:
    MAP map_;                                                           // 提供给用户自定义的map函数
    MAPTIMER timer_callback_;

    // MapReduceUtil::RecordReader record_reader;                       // 提供给用户自定义的recordread函数
    RecordReader<reader_key, reader_value> *record_reader_;       // split迭代器类型
    OutputFormat<key, value> *output_format_;
    std::vector<InputSplit *> splits_;

    pthread_t *map_threads_;

    Imagine_Rpc::RpcServer *rpc_server_;
    pthread_t *rpc_server_thread_;
    Imagine_Tool::Logger* logger_;

    Partitioner<key> *partitioner_;

    Imagine_Rpc::Stub* stub_;
};

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper()
                                            : record_reader_(nullptr), output_format_(nullptr), partitioner_(nullptr)
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper(std::string profile_name) : Mapper()
{
    Init(profile_name);
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper(YAML::Node config) : Mapper()
{
    Init(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::~Mapper()
{
    delete rpc_server_;
    delete rpc_server_thread_;
    delete[] map_threads_;

    delete record_reader_;
    delete output_format_;
    delete partitioner_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::Init(std::string profile_name)
{
    if (profile_name == "") {
        throw std::exception();
    }

    YAML::Node config = YAML::LoadFile(profile_name);
    Init(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::Init(YAML::Node config)
{
    ip_ = config["ip"].as<std::string>();
    port_ = config["port"].as<std::string>();
    zookeeper_ip_ = config["zookeeper_ip"].as<std::string>();
    zookeeper_port_ = config["zookeeper_port"].as<std::string>();
    // reducer_num_ = config["reducer_num"].as<size_t>();
    // split_size_ = config["split_size"].as<size_t>();
    thread_num_ = config["thread_num"].as<size_t>();
    log_name_ = config["log_name"].as<std::string>();
    log_path_ = config["log_path"].as<std::string>();
    max_log_file_size_ = config["max_log_file_size"].as<size_t>();
    async_log_ = config["async_log"].as<bool>();
    singleton_log_mode_ = config["singleton_log_mode"].as<bool>();
    log_title_ = config["log_title"].as<std::string>();
    log_with_timestamp_ = config["log_with_timestamp"].as<bool>();

    if (singleton_log_mode_) {
        logger_ = Imagine_Tool::SingletonLogger::GetInstance();
    } else {
        logger_ = new Imagine_Tool::NonSingletonLogger();
        Imagine_Tool::Logger::SetInstance(logger_);
    }

    logger_->Init(config);

    InitLoop(config);
    stub_ = new Imagine_Rpc::Stub(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::InitLoop(YAML::Node config)
{
    rpc_server_thread_ = new pthread_t;
    if (!rpc_server_thread_) {
        throw std::exception();
    }

    if (record_reader_ == nullptr) {
        record_reader_ = new LineRecordReader();
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
std::shared_ptr<RecordReader<reader_key, reader_value>> Mapper<reader_key, reader_value, key, value>::GenerateRecordReader(InputSplit *split, int split_id)
{
    std::shared_ptr<RecordReader<reader_key, reader_value>> new_record_reader = record_reader_->CreateRecordReader(split, split_id);
    new_record_reader->SetServer(rpc_server_);

    return new_record_reader;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* Mapper<reader_key, reader_value, key, value>::GenerateMapRunner(int split_id, int split_num, const std::string file_name, const std::string &master_ip, const std::string &master_port)
{
    return new MapRunner<reader_key, reader_value, key, value>(split_id, split_num, file_name, ip_, port_, master_ip, master_port, map_, partitioner_, output_format_, rpc_server_);
}

template <typename reader_key, typename reader_value, typename key, typename value>
Imagine_Rpc::Stub* Mapper<reader_key, reader_value, key, value>::GenerateNewStub()
{
    return new Imagine_Rpc::Stub(*stub_);
}

template <typename reader_key, typename reader_value, typename key, typename value>
MAPTIMER Mapper<reader_key, reader_value, key, value>::GetTimerCallback()
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
std::vector<std::string> Mapper<reader_key, reader_value, key, value>::GetFile(const std::vector<std::string> &input)
{
    std::vector<std::string> output;
    std::string content;
    LOG_INFO("get file %s", &input[0][0]);
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
bool Mapper<reader_key, reader_value, key, value>::SetDefaultRecordReader()
{
    record_reader_ = new LineRecordReader();
    return true;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool Mapper<reader_key, reader_value, key, value>::SetDefaultMapFunction()
{
    // map=DefaultMapFunction;
    map_ = [](int offset, const std::string &line_text) -> std::pair<std::string, int>
    {
        return std::make_pair(line_text, 1);
    };

    return true;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool Mapper<reader_key, reader_value, key, value>::SetDefaultTimerCallback()
{
    timer_callback_ = DefaultTimerCallback;
    return true;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool Mapper<reader_key, reader_value, key, value>::SetDefaultPartitioner()
{
    partitioner_ = new StringPartitioner();

    return true;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool Mapper<reader_key, reader_value, key, value>::SetDefaultOutputFormat()
{
    output_format_ = new TextOutputFormat();

    return true;
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
    LOG_INFO("Reader use count is %d, reader ptr is %p", reader.use_count(), reader.get());
    if (reader.use_count() == 1) {
        reader->GetServer()->RemoveTimer(reader->GetTimerId());
        reader.reset();
        stub->CloseConnection();
        stub.reset();
        LOG_INFO("This Mapper Task Over!");
        return;
    }
    Internal::HeartBeatRequestMessage request_msg;
    Internal::HeartBeatResponseMessage response_msg;
    MapReduceUtil::GenerateHeartBeatProcessMessage(&request_msg, Internal::Identity::Mapper, reader->GetFileName(), reader->GetSplitId(), reader->GetProgress());
    LOG_INFO("Task Progress Message is Sending!");
    stub->CallConnectServer(&request_msg, &response_msg);

    if (response_msg.status_() == Internal::Status::Ok) {
    } else {
        throw std::exception();
    }
}

} // namespace Imagine_MapReduce

#endif