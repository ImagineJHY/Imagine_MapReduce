#include "Imagine_MapReduce/MapReduceMaster.h"

#include "Imagine_MapReduce/log_macro.h"
#include "Imagine_MapReduce/MapReduceUtil.h"
#include "Imagine_MapReduce/HeartBeatService.h"
#include "Imagine_MapReduce/MapTaskMessage.pb.h"
#include "Imagine_MapReduce/TaskCompleteService.h"
#include "Imagine_MapReduce/ReduceTaskMessage.pb.h"
#include "Imagine_MapReduce/StartReduceMessage.pb.h"

#include <fstream>

namespace Imagine_MapReduce
{

MapReduceMaster::MapReduceMaster()
{
    rpc_server_thread_ = new pthread_t;
    if (!rpc_server_thread_) {
        throw std::exception();
    }

    rpc_server_ = new Imagine_Rpc::RpcServer();
}

MapReduceMaster::MapReduceMaster(const std::string& profile_name)
{
    Init(profile_name);
}

MapReduceMaster::MapReduceMaster(const YAML::Node& config)
{
    Init(config);
}

MapReduceMaster::~MapReduceMaster()
{
    delete rpc_server_thread_;
    delete rpc_server_;
}

void MapReduceMaster::Init(const std::string& profile_name)
{
    if (profile_name == "") {
        throw std::exception();
    }

    YAML::Node config = YAML::LoadFile(profile_name);
    Init(config);
}

void MapReduceMaster::Init(const YAML::Node& config)
{
    ip_ = config["ip"].as<std::string>();
    port_ = config["port"].as<std::string>();
    reducer_num_ = config["reducer_num"].as<size_t>();
    split_size_ = config["split_size"].as<size_t>();

    YAML::Node file_list = config["file_list"];
    for(int i = 0; i < file_list.size(); i++) {
        file_list_.push_back(file_list[i].as<std::string>());
    }

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

void MapReduceMaster::InitLoop(const YAML::Node& config)
{
    rpc_server_thread_ = new pthread_t;
    if (!rpc_server_thread_) {
        throw std::exception();
    }

    rpc_server_ = new Imagine_Rpc::RpcServer(config);

    rpc_server_->RegisterService(new Internal::TaskCompleteService(this));
    rpc_server_->RegisterService(new Internal::HeartBeatService());
}

MapReduceMaster* MapReduceMaster::MapReduce(const std::vector<std::string> &file_list, const size_t reducer_num)
{
    Internal::MapTaskRequestMessage request_msg;
    Internal::MapTaskResponseMessage response_msg;
    Imagine_Rpc::Stub* stub = GenerateNewStub();
    stub->SetServiceName(INTERNAL_MAP_TASK_SERVICE_NAME)->SetMethodName(INTERNAL_MAP_TASK_METHOD_NAME);

    MapReduceUtil::GenerateMapTaskMessage(&request_msg, file_list[0], split_size_, ip_, port_);

    for (int i = 1; i <= reducer_num; i++) {
        ReducerNode *reducer_node = new ReducerNode;
        files_.push_back(file_list[0]);
        reducer_node->AddFile(file_list[0]);
        reducer_map_.insert(std::make_pair(i, reducer_node));
    }

    stub->Call(&request_msg, &response_msg);
    
    delete stub;

    return this;
}

const MapReduceMaster* MapReduceMaster::StartReducer(const std::string &reducer_ip, const std::string &reducer_port) const
{
    IMAGINE_MAPREDUCE_LOG("Start Reduce!");
    Internal::StartReduceRequestMessage request_msg;
    Internal::StartReduceResponseMessage response_msg;
    Imagine_Rpc::Stub* stub = GenerateNewStub();
    stub->SetServiceName(INTERNAL_START_REDUCE_SERVICE_NAME)->SetMethodName(INTERNAL_START_REDUCE_METHOD_NAME)->SetServerIp(reducer_ip)->SetServerPort(reducer_port);
    MapReduceUtil::GenerateStartReduceMessage(&request_msg, ip_, port_, files_);
    stub->Call(&request_msg, &response_msg);

    delete stub;

    return this;
}

const MapReduceMaster* MapReduceMaster::ConnReducer(size_t split_num, const std::string &file_name, const std::string &split_file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &reducer_ip, const std::string &reducer_port) const
{
    Internal::ReduceTaskRequestMessage request_msg;
    Internal::ReduceTaskResponseMessage response_msg;
    Imagine_Rpc::Stub* stub = GenerateNewStub();
    stub->SetServiceName(INTERNAL_REDUCE_TASK_SERVICE_NAME)->SetMethodName(INTERNAL_REDUCE_TASK_METHOD_NAME)->SetServerIp(reducer_ip)->SetServerPort(reducer_port);
    MapReduceUtil::GenerateReduceTaskMessage(&request_msg, file_name, split_file_name, split_num, mapper_ip, mapper_port, ip_, port_);
    stub->Call(&request_msg, &response_msg);

    delete stub;

    return this;
}

void MapReduceMaster::loop()
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

MapReduceMaster::ReducerNode* MapReduceMaster::FindReducerNode(int idx) const
{
    if (reducer_map_.find(idx) == reducer_map_.end()) {
        return nullptr;
    }

    return reducer_map_.find(idx)->second;
}

MapReduceMaster* MapReduceMaster::SetTaskFile(const std::vector<std::string> &files)
{
    files_.clear();
    for (size_t i = 0; i < files.size(); i++) {
        files_.push_back(files[i]);
    }

    return this;
}

Imagine_Rpc::Stub* MapReduceMaster::GenerateNewStub() const
{
    return new Imagine_Rpc::Stub(*stub_);
}

size_t MapReduceMaster::GetReducerNum() const
{
    return reducer_num_;
}

} // namespace Imagine_MapReduce