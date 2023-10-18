#include "Imagine_Rpc/RpcClient.h"
#include "Imagine_MapReduce/MapReduceMaster.h"
#include "Imagine_MapReduce/MapReduceUtil.h"

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

MapReduceMaster::MapReduceMaster(std::string profile_name)
{
    Init(profile_name);

    rpc_server_thread_ = new pthread_t;
    if (!rpc_server_thread_) {
        throw std::exception();
    }

    rpc_server_ = new Imagine_Rpc::RpcServer(rpc_profile_name_);

    rpc_server_->Callee("MapReduceCenter", std::bind(&MapReduceMaster::MapReduceCenter, this, std::placeholders::_1));
}

MapReduceMaster::MapReduceMaster(const std::string &ip, const std::string &port, const std::string &keeper_ip, const std::string &keeper_port, const size_t reducer_num)
                                 : ip_(ip), port_(port), zookeeper_ip_(keeper_ip), zookeeper_port_(keeper_port), reducer_num_(reducer_num)
{
    int temp_port = MapReduceUtil::StringToInt(port_);
    if (temp_port < 0) {
        throw std::exception();
    }

    rpc_server_thread_ = new pthread_t;
    if (!rpc_server_thread_) {
        throw std::exception();
    }

    // files.push_back("bbb.txt");

    rpc_server_ = new Imagine_Rpc::RpcServer(ip_, port_, zookeeper_ip_, zookeeper_port_);

    rpc_server_->Callee("MapReduceCenter", std::bind(&MapReduceMaster::MapReduceCenter, this, std::placeholders::_1));
}

MapReduceMaster::~MapReduceMaster()
{
    delete rpc_server_thread_;
    delete rpc_server_;
}

void MapReduceMaster::Init(std::string profile_name)
{
    if (profile_name == "") {
        throw std::exception();
    }

    YAML::Node config = YAML::LoadFile(profile_name);
    ip_ = config["ip"].as<std::string>();
    port_ = config["port"].as<std::string>();
    zookeeper_ip_ = config["zookeeper_ip"].as<std::string>();
    zookeeper_port_ = config["zookeeper_port"].as<std::string>();
    reducer_num_ = config["reducer_num"].as<size_t>();
    split_size_ = config["split_size"].as<size_t>();

    YAML::Node file_list = config["file_list"];
    for(int i = 0; i < file_list.size(); i++) {
        file_list_.push_back(file_list[i].as<std::string>());
    }

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

void MapReduceMaster::InitProfilePath(std::string profile_name)
{
    size_t idx = profile_name.find_last_of("/");
    profile_path_ = profile_name.substr(0, idx + 1);
    rpc_profile_name_ = profile_path_ + "generate_Master_submodule_RPC_profile.yaml";
}

void MapReduceMaster::GenerateSubmoduleProfile(YAML::Node config)
{
    int fd = open(rpc_profile_name_.c_str(), O_RDWR | O_CREAT);
    config.remove(config["reducer_num"]);
    config.remove(config["split_size"]);
    config.remove(config["file_list"]);
    config["log_name"] = "imagine_rpc_log.log";
    config["max_channel_num"] = 10000;
    write(fd, config.as<std::string>().c_str(), config.as<std::string>().size());
    close(fd);
}

bool MapReduceMaster::MapReduce(const std::vector<std::string> &file_list, const size_t reducer_num, const size_t split_size)
{
    std::string method = "Map";
    std::vector<std::string> parameters;

    for (size_t i = 0; i < file_list.size(); i++) {
        files_.push_back(file_list[i]);
        parameters.push_back(file_list[i]);
    }
    parameters.push_back(MapReduceUtil::IntToString(split_size));
    parameters.push_back(ip_);
    parameters.push_back(port_);

    for (int i = 1; i <= reducer_num; i++) {
        ReducerNode *reducer_node = new ReducerNode;
        reducer_node->files_.push_back(file_list[0]);
        reducer_map_.insert(std::make_pair(i, reducer_node));
    }

    Imagine_Rpc::RpcClient::Caller(method, parameters, zookeeper_ip_, zookeeper_port_);

    return true;
}

bool MapReduceMaster::StartReducer(const std::string &reducer_ip, const std::string &reducer_port)
{
    /*
    信息格式
        -1.文件总数
        -2.文件名列表
        -3.本机ip
        -4.本机port
    */

    std::string method_name = "Register";
    std::vector<std::string> parameters;
    parameters.push_back(MapReduceUtil::IntToString(files_.size()));
    for (size_t i = 0; i < files_.size(); i++) {
        parameters.push_back(files_[i]);
    }
    // printf("master_ip is %s , master_port is %s\n",&ip[0],&port[0]);
    parameters.push_back(ip_);
    parameters.push_back(port_);

    std::vector<std::string> output = Imagine_Rpc::RpcClient::Call(method_name, parameters, reducer_ip, reducer_port);

    return true;
}

// bool MapReduceMaster::StartMapper(const std::string& file_name,const std::string& mapper_ip, const std::string& mapper_port)
// {
//     std::string method_name="Reduce";
//     std::vector<std::string> parameters;
//     if(file_name.size()){
//         parameters.push_back("GetFile");
//         parameters.push_back(mapper_ip);
//         parameters.push_back(mapper_port);
//     }else{
//         parameters.push_back("Start");
//         parameters.push_back(ip);
//         parameters.push_back(port);
//     }
// }

bool MapReduceMaster::ConnReducer(const std::string &split_num, const std::string &aim_file_name, const std::string &split_file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &reducer_ip, const std::string &reducer_port)
{
    /*
    参数说明
        -1.split_num:文件被分成的split数
        -2.file_name:split前的文件名
        -3.split_file_name:当前要获取的split文件名
        -4.mapper_ip:文件所在的mapper的ip
        -5.mapper_port:文件所在的mapper的port
        -6.master_ip:master的ip
        -7.master_port:master的port
    */
    std::string method_name = "Reduce";
    std::vector<std::string> parameters;
    parameters.push_back(split_num);
    parameters.push_back(aim_file_name);
    parameters.push_back(split_file_name);
    parameters.push_back(mapper_ip);
    parameters.push_back(mapper_port);
    parameters.push_back(ip_);
    parameters.push_back(port_);
    Imagine_Rpc::RpcClient::Call(method_name, parameters, reducer_ip, reducer_port);

    return true;
}

std::vector<std::string> MapReduceMaster::MapReduceCenter(const std::vector<std::string> &message)
{
    /*
    信息格式:
    -身份信息:Mapper/Reducer
    */
    LOG_INFO("this is MapReduceCenter!");

    std::vector<std::string> send_msg;
    if (message[0] == "Mapper") {
        send_msg.push_back(ProcessMapperMessage(message));
    } else if (message[0] == "Reducer") {
        send_msg.push_back(ProcessReducerMessage(message));
    } else {
        throw std::exception();
    }

    return send_msg;
}

std::string MapReduceMaster::ProcessMapperMessage(const std::vector<std::string> &message)
{
    /*
    信息格式:
    -身份信息:Mapper
    -mapper名
        -处理的文件名
        -splitID
    -类型
        -Process:正在执行，尚未完成
        -Start:任务开始
        -Finish:任务完成
    -信息内容:
        -Process:
            -进度百分比(保留两位小数)
        -Start:无信息
        -Finish:
            -mapperIp
            -mapperPort
            -split数目
            -shuffle文件名列表
    */
    const std::string &aim_file_name = message[1];
    const std::string &split_id = message[2];
    const std::string &message_type = message[3];

    if (message_type == "Process") {
        const std::string &process_percent = message[4];
        LOG_INFO("Mapper : file %s split %s processing is %s !", &aim_file_name[0], &split_id[0], &process_percent[0]);
    } else if (message_type == "Start") {
        LOG_INFO("Mapper : file %s split %s processing is starting !", &aim_file_name[0], &split_id[0]);
    } else if (message_type == "Finish") {
        LOG_INFO("Mapper : file %s split %s processing is finish !", &aim_file_name[0], &split_id[0]);

        const std::string &mapper_ip = message[4];
        const std::string &mapper_port = message[5];
        const std::string &split_num = message[6];
        std::vector<std::string> shuffle_list;
        for (size_t i = 7; i < message.size(); i++) {
            shuffle_list.push_back(std::move(message[i]));
        }
        if (static_cast<size_t>(reducer_num_) != shuffle_list.size()) {
            throw std::exception();
        }
        for (int i = 1; i <= reducer_num_; i++) {
            const std::string &shuffle_name = shuffle_list[i - 1];
            std::unordered_map<int, ReducerNode *>::iterator it = reducer_map_.find(i);
            if (it == reducer_map_.end()) {
                throw std::exception();
            }

            if (!(it->second->is_ready_.load())) {
                // reducer未就绪
                pthread_mutex_lock(it->second->reducer_lock_);
                if (!(it->second->is_ready_.load())) {
                    // 再次确认
                    LOG_INFO("Searching Reducer!");
                    Imagine_Rpc::RpcClient::CallerOne("Reduce", zookeeper_ip_, zookeeper_port_, it->second->ip_, it->second->port_); // 获取一个reducer
                    LOG_INFO("GET REDUCER IP:%s, PORT:%s", &(it->second->ip_[0]), &(it->second->port_[0]));
                    StartReducer(it->second->ip_, it->second->port_);                                           // 启动reducer与master的连接
                    it->second->is_ready_.store(true);
                    pthread_mutex_unlock(it->second->reducer_lock_);
                } else {
                    pthread_mutex_unlock(it->second->reducer_lock_);
                }
            }
            ConnReducer(split_num, aim_file_name, shuffle_name, mapper_ip, mapper_port, it->second->ip_, it->second->port_);
        }
    }

    return "Receive";
}

std::string MapReduceMaster::ProcessReducerMessage(const std::vector<std::string> &message)
{
    if (message[2] == "Process") {
        LOG_INFO("reducer's processing is %s", &message[3][0]);
        return "";
    }

    return "";
}

void MapReduceMaster::loop()
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

bool MapReduceMaster::SetTaskFile(std::vector<std::string> &files)
{
    files_.clear();
    for (size_t i = 0; i < files.size(); i++) {
        files_.push_back(files[i]);
    }

    return true;
}

} // namespace Imagine_MapReduce