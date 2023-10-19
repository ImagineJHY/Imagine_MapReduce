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

#include <memory.h>
#include <fstream>

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value, typename key, typename value>
class Mapper
{
 public:
    Mapper();

    Mapper(std::string profile_name);

    Mapper(const std::string &ip, const std::string &port, RecordReader<reader_key, reader_value> *record_reader = nullptr, MAP map = nullptr, Partitioner<key> *partitioner = nullptr, OutputFormat<key, value> *output_format = nullptr, MAPTIMER timer_callback = nullptr, const std::string &keeper_ip = "", const std::string &keeper_port = "");

    ~Mapper();

    void Init(std::string profile_name);

    void InitProfilePath(std::string profile_name);

    void GenerateSubmoduleProfile(YAML::Node config);

    void SetDefault();

    // Rpc通信调用
    std::vector<std::string> Map(const std::vector<std::string> &input);

    std::vector<std::string> GetFile(const std::vector<std::string> &input);

    bool SetDefaultRecordReader();

    bool SetDefaultMapFunction();

    bool SetDefaultTimerCallback();

    bool SetDefaultOutputFormat();

    bool SetDefaultPartitioner();

    static void DefaultTimerCallback(int sockfd, std::shared_ptr<RecordReader<reader_key, reader_value>> reader);

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
};

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper()
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper(std::string profile_name)
{
    Init(profile_name);

    rpc_server_ = new Imagine_Rpc::RpcServer(rpc_profile_name_);

    rpc_server_->Callee("Map", std::bind(&Mapper::Map, this, std::placeholders::_1));
    rpc_server_->Callee("GetFile", std::bind(&Mapper::GetFile, this, std::placeholders::_1));
}

template <typename reader_key, typename reader_value, typename key, typename value>
Mapper<reader_key, reader_value, key, value>::Mapper(const std::string &ip, const std::string &port, RecordReader<reader_key, reader_value> *record_reader, MAP map, Partitioner<key> *partitioner, OutputFormat<key, value> *output_format, MAPTIMER timer_callback, const std::string &keeper_ip, const std::string &keeper_port)
                                            : ip_(ip), port_(port), zookeeper_ip_(keeper_ip), zookeeper_port_(keeper_port), timer_callback_(timer_callback), record_reader_(record_reader), output_format_(output_format), partitioner_(partitioner)
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

    rpc_server_ = new Imagine_Rpc::RpcServer(ip_, port_, zookeeper_ip_, zookeeper_port_);

    rpc_server_->Callee("Map", std::bind(&Mapper::Map, this, std::placeholders::_1));
    rpc_server_->Callee("GetFile", std::bind(&Mapper::GetFile, this, std::placeholders::_1));
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

    InitProfilePath(profile_name);

    GenerateSubmoduleProfile(config);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::InitProfilePath(std::string profile_name)
{
    size_t idx = profile_name.find_last_of("/");
    profile_path_ = profile_name.substr(0, idx + 1);
    rpc_profile_name_ = profile_path_ + "generate_Mapper_submodule_RPC_profile.yaml";
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::GenerateSubmoduleProfile(YAML::Node config)
{
    std::ofstream fout(rpc_profile_name_.c_str());
    config["log_name"] = "imagine_rpc_log.log";
    config["max_channel_num"] = 10000;
    fout << config;
    fout.close();
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
std::vector<std::string> Mapper<reader_key, reader_value, key, value>::Map(const std::vector<std::string> &input)
{
    /*
    数据格式：
        1.目标文件名(路径)
        2.split大小
        3.MapReduceMaster的ip
        4.MapReduceMaster的port
        5.目标文件ip(若在本地则省略)
        6.目标文件port(若在本地则省略)
    注:对于split的跨行问题:到达split时多读一行(多读一个完整的\r\n进来),并且让除第一块意外的每个Mapper都跳过第一行数据(split起始位置为0不跳行,反之跳行)
    */

    // 获取split数据
    std::vector<InputSplit *> splits = MapReduceUtil::DefaultReadSplitFunction(input[0], MapReduceUtil::StringToInt(input[1])); // 传入目标文件名和split大小
    // map(MapReduceUtil::DefaultReadSplitFunction(input[2]));
    // 将数据按要求转换成kv数据
    // std::unordered_map<std::string,std::string> kv_map=record_reader(splits);
    map_threads_ = new pthread_t[splits.size()];
    for (size_t i = 0; i < splits.size(); i++) {
        std::shared_ptr<RecordReader<reader_key, reader_value>> new_record_reader = record_reader_->CreateRecordReader(splits[i], i + 1);
        // new_record_reader->SetOutputFileName("split"+MapReduceUtil::IntToString(i+1)+".txt");
        MapRunner<reader_key, reader_value, key, value> *runner = new MapRunner<reader_key, reader_value, key, value>(i + 1, splits.size(), input[0], ip_, port_, input[2], input[3], map_, partitioner_, output_format_, rpc_server_);
        runner->SetRecordReader(new_record_reader);
        runner->SetTimerCallback(timer_callback_);
        pthread_create(
            map_threads_ + i, nullptr, [](void *argv) -> void *
            {
                MapRunner<reader_key, reader_value, key, value> *runner = (MapRunner<reader_key, reader_value, key, value> *)argv;
                MAP map = runner->GetMap();
                std::shared_ptr<RecordReader<reader_key, reader_value>> reader = runner->GetRecordReader();
                // OutputFormat<key, value> *output_format = runner->GetOutPutFormat();

                // 连接Master
                int sockfd;
                long long timerfd;
                if (Imagine_Rpc::RpcClient::ConnectServer(runner->GetMasterIp(), runner->GetMasterPort(), &sockfd)) {
                    std::vector<std::string> parameters;
                    parameters.push_back("Mapper");
                    parameters.push_back(runner->GetFileName());
                    parameters.push_back(MapReduceUtil::IntToString(runner->GetId()));
                    parameters.push_back("Start");
                    if (Imagine_Rpc::RpcClient::Call("MapReduceCenter", parameters, &sockfd)[0] == "Receive") {
                        timerfd = runner->GetRpcServer()->SetTimer(2.0, 0.0, std::bind(runner->GetTimerCallback(), sockfd, reader));
                    } else {
                        throw std::exception();
                    }
                }

                runner->StartSpillingThread();
                sleep(1);
                while (reader->NextKeyValue()) {
                    runner->WriteToBuffer(map(reader->GetCurrentKey(), reader->GetCurrentValue()));
                }

                runner->CompleteMapping(); // buffer在spill线程中销毁

                runner->GetRpcServer()->RemoveTimer(timerfd);
                std::vector<std::string> parameters;
                parameters.push_back("Mapper");
                parameters.push_back(runner->GetFileName());
                parameters.push_back(MapReduceUtil::IntToString(runner->GetId()));
                parameters.push_back("Finish");
                parameters.push_back(runner->GetMapperIp());
                parameters.push_back(runner->GetMapperPort());
                parameters.push_back(MapReduceUtil::IntToString(runner->GetSplitNum()));
                std::vector<std::string> &shuffle = runner->GetShuffleFile();
                parameters.insert(parameters.end(), shuffle.begin(), shuffle.end());
                // printf("%s : working is finish!\n",&split_file_name[0]);
                Imagine_Rpc::RpcClient::Call("MapReduceCenter", parameters, &sockfd);

                delete runner;

                close(sockfd);
                // close(fd);

                return nullptr;
            },
            runner);
        pthread_detach(*(map_threads_ + i));
    }

    return Imagine_Rpc::Rpc::Deserialize("");
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
    // timer_callback=[](int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader){
    //     printf("this is timer callback!\n");
    //     if(reader.use_count()==1){
    //     printf("mapper已完成!\n");
    //     return;
    //     }
    //     //printf("sockfd is %d\n",sockfd);
    //     std::string method="MapReduceCenter";
    //     std::vector<std::string> parameters;
    //     parameters.push_back("Mapper");
    //     parameters.push_back(reader->GetOutputFileName());
    //     parameters.push_back("Process");
    //     parameters.push_back(MapReduceUtil::DoubleToString(reader->GetProgress()));

    //     std::vector<std::string> recv_=Imagine_Rpc::RpcClient::Call(method,parameters,&sockfd);
    //     if(recv_.size()&&recv_[0]=="Receive"){
    //         printf("connect ok!\n");
    //     }else{
    //         printf("connect error!\n");
    //     }
    // }
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
            rpc_server->loop();

            return nullptr;
        }, 
        this->rpc_server_);
    pthread_detach(*rpc_server_thread_);
}

template <typename reader_key, typename reader_value, typename key, typename value>
void Mapper<reader_key, reader_value, key, value>::DefaultTimerCallback(int sockfd, std::shared_ptr<RecordReader<reader_key, reader_value>> reader)
{
    // printf("this is timer callback!\n");
    if (reader.use_count() == 1) {
        LOG_INFO("This Mapper Task Over!");
        return;
    }
    // printf("sockfd is %d\n",sockfd);
    std::string method = "MapReduceCenter";
    std::vector<std::string> parameters;
    parameters.push_back("Mapper");
    parameters.push_back(reader->GetFileName());
    parameters.push_back(MapReduceUtil::IntToString(reader->GetSplitId()));
    parameters.push_back("Process");
    parameters.push_back(MapReduceUtil::DoubleToString(reader->GetProgress()));

    std::vector<std::string> temp_recv = Imagine_Rpc::RpcClient::Call(method, parameters, &sockfd);
    if (temp_recv.size() && temp_recv[0] == "Receive") {
        // printf("connect ok!\n");
    } else {
        LOG_INFO("connect error!");
    }
}

} // namespace Imagine_MapReduce

#endif