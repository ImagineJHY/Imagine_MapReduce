#ifndef IMAGINE_MAPREDUCE_REDUCER_H
#define IMAGINE_MAPREDUCE_REDUCER_H

#include "Imagine_Rpc/RpcServer.h"
#include "Imagine_Rpc/RpcClient.h"
#include "Imagine_Rpc/Stub.h"
#include "MapReduceUtil.h"
#include "common_definition.h"
#include "StartReduceService.h"
#include "ReduceTaskService.h"
#include "RetrieveSplitFileMessage.pb.h"

#include <fcntl.h>
#include <atomic>
#include <fstream>

namespace Imagine_MapReduce
{

namespace Internal
{

template <typename key, typename value>
class StartReduceService;

} // namespace Internal

template <typename key, typename value>
class Reducer
{
 public:
    class MasterNode
    {
     public:
        MasterNode() : receive_all_(false), memory_merge_(true), disk_merge_(true), memory_file_size_(0) {}

        void MemoryMerge()
        {
            pthread_mutex_lock(memory_list_lock_);

            std::vector<std::string> merge_list;
            int merge_num = memory_file_list_.size();
            for (auto it = memory_file_list_.begin(); it != memory_file_list_.end(); it++) {
                merge_list.push_back(std::move(*it));
            }
            memory_file_list_.clear();

            std::string merge_name = "memory_merge_" + MapReduceUtil::IntToString(memory_merge_id_++) + ".txt";
            int fd = open(&merge_name[0], O_CREAT | O_RDWR, 0777);
            int *idxs = new int[merge_num];

            std::priority_queue<KVReader *, std::vector<KVReader *>, KVReaderCmp> heap;
            for (int i = 0; i < merge_num; i++) {
                std::string memory_key;
                std::string memory_value;
                if (MapReduceUtil::ReadKVReaderFromMemory(merge_list[i], idxs[i], memory_key, memory_value))
                    heap.push(new KVReader(memory_key, memory_value, i));
            }
            while (heap.size()) {
                KVReader *next_kv = heap.top();
                heap.pop();
                std::string memory_key;
                std::string memory_value;
                if (MapReduceUtil::ReadKVReaderFromMemory(merge_list[next_kv->reader_idx_], idxs[next_kv->reader_idx_], memory_key, memory_value))
                    heap.push(new KVReader(memory_key, memory_value, next_kv->reader_idx_));
                MapReduceUtil::WriteKVReaderToDisk(fd, next_kv);
                delete next_kv;
            }

            delete[] idxs;
            close(fd);

            pthread_mutex_unlock(memory_list_lock_);

            pthread_mutex_lock(disk_list_lock_);
            disk_file_list_.push_back(merge_name);
            pthread_mutex_unlock(disk_list_lock_);
        }

        void DiskMerge()
        {
            pthread_mutex_lock(disk_list_lock_);

            std::vector<std::string> merge_list;
            int merge_num = disk_file_list_.size();
            for (auto it = disk_file_list_.begin(); it != disk_file_list_.end(); it++) {
                merge_list.push_back(std::move(*it));
            }
            disk_file_list_.clear();

            int *fds = new int[merge_num];
            for (int i = 0; i < merge_num; i++) {
                fds[i] = open(&merge_list[i][0], O_RDWR);
            }

            std::string merge_name = "disk_merge_" + MapReduceUtil::IntToString(disk_merge_id_++) + ".txt";
            MapReduceUtil::MergeKVReaderFromDisk(fds, merge_num, merge_name);

            disk_file_list_.push_back(merge_name);

            for (int i = 0; i < merge_num; i++) {
                close(fds[i]);
                remove(&merge_list[i][0]);
            }
            delete[] fds;

            pthread_mutex_unlock(disk_list_lock_);
        }

     public:
        int count_ = 0;                                                                  // 计数,用于判断是否可以开始执行Reduce
        int file_num_;
        std::unordered_map<std::string, std::unordered_map<std::string, int>> files_;    // 存储源文件名到splits的映射

        std::atomic<bool> receive_all_;                                                  // 标识所有文件是否全部接收完毕
        std::atomic<bool> memory_merge_;                                                 // 标识memory是否退出merge
        std::atomic<bool> disk_merge_;                                                   // 标识disk是否退出merge
        int memory_merge_id_;
        int disk_merge_id_;
        pthread_t *memory_thread_;
        pthread_t *disk_thread_;
        pthread_mutex_t *memory_list_lock_;
        pthread_mutex_t *disk_list_lock_;
        std::atomic<int> memory_file_size_;                                              // 存储内存中储存的文件的总大小
        // int memory_file_size_;                                                        // 存储内存中储存的文件的总大小
        std::list<std::string> memory_file_list_;                                        // 存储split文件内容
        std::list<std::string> disk_file_list_;                                          // 存储磁盘中储存的文件的文件名
    };

 public:
    Reducer();

    Reducer(std::string profile_name);

    Reducer(YAML::Node config);

    ~Reducer();

    void Init(std::string profile_name);

    void Init(YAML::Node config);

    void InitLoop(YAML::Node config);

    void SetDefault();

    void loop();

    std::vector<std::string> Register(const std::vector<std::string> &input);

    Reducer<key, value>* RegisterMaster(std::pair<std::string, std::string> master_node, const std::vector<std::string>& file_list);

    std::vector<std::string> Reduce(const std::vector<std::string> &input);

    void ReceiveSplitFileData(const std::pair<std::string, std::string>& master_pair, const std::pair<std::string, std::string>& mapper_pair, const std::string& file_name, const std::string& split_file_name, size_t split_num);

    void StartMergeThread(MasterNode *master_node);

    bool WriteToDisk(const std::string &file_name, const std::string &file_content);

    bool SetDefaultReduceFunction();

    Imagine_Rpc::Stub* GenerateNewStub();

 private:
    std::string ip_;
    std::string port_;
    std::string zookeeper_ip_;
    std::string zookeeper_port_;
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

    ReduceCallback reduce_;

    Imagine_Rpc::RpcServer *rpc_server_;
    Imagine_Tool::Logger* logger_;

    pthread_mutex_t *map_lock_;

    std::unordered_map<std::pair<std::string, std::string>, MasterNode *, HashPair, EqualPair> master_map_;

    Imagine_Rpc::Stub* stub_;
};

template <typename key, typename value>
Reducer<key, value>::Reducer()
{
}

template <typename key, typename value>
Reducer<key, value>::Reducer(std::string profile_name)
{
    Init(profile_name);
}

template <typename key, typename value>
Reducer<key, value>::Reducer(YAML::Node config)
{
    Init(config);
}

template <typename key, typename value>
Reducer<key, value>::~Reducer()
{
    delete rpc_server_;
    delete map_lock_;
}

template <typename key, typename value>
void Reducer<key, value>::Init(std::string profile_name)
{
    if (profile_name == "") {
        throw std::exception();
    }

    YAML::Node config = YAML::LoadFile(profile_name);
    Init(config);
}

template <typename key, typename value>
void Reducer<key, value>::Init(YAML::Node config)
{
    ip_ = config["ip"].as<std::string>();
    port_ = config["port"].as<std::string>();
    zookeeper_ip_ = config["zookeeper_ip"].as<std::string>();
    zookeeper_port_ = config["zookeeper_port"].as<std::string>();
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

template <typename key, typename value>
void Reducer<key, value>::InitLoop(YAML::Node config)
{
    map_lock_ = new pthread_mutex_t;
    if (pthread_mutex_init(map_lock_, nullptr) != 0) {
        throw std::exception();
    }

    rpc_server_ = new Imagine_Rpc::RpcServer(config);
    rpc_server_->RegisterService(new Internal::StartReduceService<key, value>(this));
    rpc_server_->RegisterService(new Internal::ReduceTaskService<key, value>(this));
}

template <typename key, typename value>
void Reducer<key, value>::SetDefault()
{
    SetDefaultReduceFunction();
}

template <typename key, typename value>
void Reducer<key, value>::loop()
{
    rpc_server_->Start();
}

template <typename key, typename value>
Reducer<key, value>* Reducer<key, value>::RegisterMaster(std::pair<std::string, std::string> master_pair, const std::vector<std::string>& file_list)
{
    pthread_mutex_lock(map_lock_);
    if (master_map_.find(master_pair) != master_map_.end()) {
        // 重复注册
        throw std::exception();
    }
    MasterNode *new_master = new MasterNode;
    new_master->file_num_ = file_list.size();
    for (int i = 0; i < file_list.size(); i++) {
        std::unordered_map<std::string, int> temp_map;
        new_master->files_.insert(std::make_pair(file_list[i], temp_map));
    }
    new_master->memory_thread_ = new pthread_t;
    new_master->disk_thread_ = new pthread_t;
    new_master->memory_list_lock_ = new pthread_mutex_t;
    new_master->disk_list_lock_ = new pthread_mutex_t;
    if (pthread_mutex_init(new_master->memory_list_lock_, nullptr) != 0) {
        throw std::exception();
    }
    if (pthread_mutex_init(new_master->disk_list_lock_, nullptr) != 0) {
        throw std::exception();
    }

    StartMergeThread(new_master); // 开启merge线程

    master_map_.insert(std::make_pair(master_pair, new_master));LOG_INFO("???????????");
    // rpc_server->SetTimer();
    pthread_mutex_unlock(map_lock_);LOG_INFO("???????????");

    return this;
}

template <typename key, typename value>
std::vector<std::string> Reducer<key, value>::Reduce(const std::vector<std::string> &input)
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
    LOG_INFO("this is Reduce Method !");
    // for(int i=0;i<input.size();i++)printf("%s\n",&input[i][0]);
    int split_num = MapReduceUtil::StringToInt(input[0]);
    std::string file_name = input[1];
    std::string split_name = input[2];
    std::string mapper_ip = input[3];
    std::string mapper_port = input[4];
    std::pair<std::string, std::string> master_pair = std::make_pair(input[5], input[6]);

    pthread_mutex_lock(map_lock_);
    typename std::unordered_map<std::pair<std::string, std::string>, MasterNode *, HashPair, EqualPair>::iterator master_it = master_map_.find(master_pair); // 找到MasterNode*
    if (master_it == master_map_.end()) {
        // 该master没有register
        throw std::exception();
    }
    MasterNode *master_node = master_it->second;
    std::unordered_map<std::string, std::unordered_map<std::string, int>>::iterator file_it = master_node->files_.find(file_name); // 找到对应的file
    if (file_it == master_it->second->files_.end()) {
        // 错误的文件名
        throw std::exception();
        // std::unordered_map<std::string,int> temp_map;
        // auto temp_pair=it->second->files.insert(std::make_pair(file_name,temp_map));
        // if(temp_pair->second)file_it=temp_pair->first;
        // else throw std::exception();
    }
    std::unordered_map<std::string, int>::iterator split_it = file_it->second.find(split_name);
    if (split_it != file_it->second.end()) {
        // 重复接收同一个split文件
        throw std::exception();
    }
    pthread_mutex_unlock(map_lock_);

    // 从mapper获取split文件
    std::string method_name = "GetFile";
    std::vector<std::string> parameters;
    parameters.push_back(split_name);

    pthread_mutex_lock(master_node->memory_list_lock_);
    // master_node->memory_file_list_.push_front(Imagine_Rpc::RpcClient::Call(method_name, parameters, mapper_ip, mapper_port)[0]);
    if ((*master_node->memory_file_list_.begin()).size()) {
        LOG_INFO("split file %s content : %s", &split_name[0], &(*master_node->memory_file_list_.begin())[0]);
    } else {
        LOG_INFO("split file %s content : NoContent!", &split_name[0]);
    }
    master_node->memory_file_size_ += master_node->memory_file_list_.front().size();
    pthread_mutex_unlock(master_node->memory_list_lock_);

    pthread_mutex_lock(map_lock_);
    split_it = file_it->second.find(split_name);
    if (split_it != file_it->second.end()) {
        // 重复接收同一个split文件
        throw std::exception();
    }
    file_it->second.insert(std::make_pair(split_name, 1));
    master_it = master_map_.find(master_pair);
    file_it = master_it->second->files_.find(file_name);
    if (file_it->second.size() == static_cast<size_t>(split_num)) {
        master_it->second->count_++;
    }

    if (static_cast<size_t>(master_it->second->count_) == master_it->second->files_.size()) {
        // 所有文件接收完毕,可以开始执行
        master_node->receive_all_.store(true);
        while (master_node->disk_merge_.load());
        LOG_INFO("TaskOver!");
    }
    pthread_mutex_unlock(map_lock_);

    std::vector<std::string> output;
    output.push_back("Receive!\n");

    return output;
}

template <typename key, typename value>
void Reducer<key, value>::ReceiveSplitFileData(const std::pair<std::string, std::string>& master_pair, const std::pair<std::string, std::string>& mapper_pair, const std::string& file_name, const std::string& split_file_name, size_t split_num)
{
    pthread_mutex_lock(map_lock_);
    typename std::unordered_map<std::pair<std::string, std::string>, MasterNode *, HashPair, EqualPair>::iterator master_it = master_map_.find(master_pair); // 找到MasterNode*
    if (master_it == master_map_.end()) {
        // 该master没有register
        LOG_INFO("NO Register Master Error!");
        throw std::exception();
    }
    MasterNode *master_node = master_it->second;
    std::unordered_map<std::string, std::unordered_map<std::string, int>>::iterator file_it = master_node->files_.find(file_name); // 找到对应的file
    if (file_it == master_it->second->files_.end()) {
        // 错误的文件名
        LOG_INFO("File name Error! Get File Name %s", file_name.c_str());
        throw std::exception();
    }
    std::unordered_map<std::string, int>::iterator split_it = file_it->second.find(split_file_name);
    if (split_it != file_it->second.end()) {
        // 重复接收同一个split文件
        LOG_INFO("Repeat Split File Error! Get Split File Name %s", split_file_name.c_str());
        throw std::exception();
    }
    pthread_mutex_unlock(map_lock_);

    // 从mapper获取split文件
    Internal::RetrieveSplitFileRequestMessage request_msg;
    Internal::RetrieveSplitFileResponseMessage response_msg;
    Imagine_Rpc::Stub* stub = GenerateNewStub();
    stub->SetServiceName(INTERNAL_RETRIEVE_SPLIT_FILE_SERVICE_NAME)->SetMethodName(INTERNAL_RETRIEVE_SPLIT_FILE_METHOD_NAME)->SetServerIp(mapper_pair.first)->SetServerPort(mapper_pair.second);
    MapReduceUtil::GenerateRetrieveSplitFileMessage(&request_msg, split_file_name);
    stub->Call(&request_msg, &response_msg);

    delete stub;

    pthread_mutex_lock(master_node->memory_list_lock_);
    master_node->memory_file_list_.push_front(response_msg.split_file_content_());
    if ((*master_node->memory_file_list_.begin()).size()) {
        LOG_INFO("split file %s content : %s", &split_file_name[0], &(*master_node->memory_file_list_.begin())[0]);
    } else {
        LOG_INFO("split file %s content : NoContent!", &split_file_name[0]);
    }
    master_node->memory_file_size_ += master_node->memory_file_list_.front().size();
    pthread_mutex_unlock(master_node->memory_list_lock_);

    pthread_mutex_lock(map_lock_);
    split_it = file_it->second.find(split_file_name);
    if (split_it != file_it->second.end()) {
        // 重复接收同一个split文件
        LOG_INFO("Repeat Retrieve Split File Error! Get Split File Name %s", split_file_name.c_str());
        throw std::exception();
    }
    file_it->second.insert(std::make_pair(split_file_name, 1));
    master_it = master_map_.find(master_pair);
    file_it = master_it->second->files_.find(file_name);
    if (file_it->second.size() == split_num) {
        master_it->second->count_++;
    }

    if (static_cast<size_t>(master_it->second->count_) == master_it->second->files_.size()) {
        // 所有文件接收完毕,可以开始执行
        LOG_INFO("Receive All Split File! Start Finally Merge!");
        master_node->receive_all_.store(true);
        while (master_node->disk_merge_.load());
        LOG_INFO("TaskOver!");
    }
    pthread_mutex_unlock(map_lock_);
}

template <typename key, typename value>
void Reducer<key, value>::StartMergeThread(MasterNode *master_node)
{
    // 此函数调用时机在Register的map_lock中,故不需要加锁
    //  std::list<std::string>& memory_list=master_node->memory_file_list_;
    //  std::list<std::string>& disk_list=master_node->disk_file_list_;
    pthread_create(
        master_node->memory_thread_, nullptr, [](void *argv) -> void *
        {
            MasterNode* master_node = (MasterNode*)argv;
            while (!(master_node->receive_all_.load())) {
                if (master_node->memory_file_size_ >= DEFAULT_MEMORY_MERGE_SIZE) {
                    master_node->MemoryMerge();
                }
            }
            LOG_INFO("Memory Merge Over! total size is %ld", master_node->memory_file_size_.load());
            master_node->MemoryMerge();
            master_node->memory_merge_.store(false);

            return nullptr; 
        },
        master_node);

    pthread_detach(*(master_node->memory_thread_));

    pthread_create(
        master_node->disk_thread_, nullptr, [](void *argv) -> void *
        {

            MasterNode* master_node = (MasterNode*)argv;
            while (master_node->memory_merge_.load()) {
                if (master_node->disk_file_list_.size() >= DEFAULT_DISK_MERGE_NUM) {
                    master_node->DiskMerge();
                }
            }
            master_node->DiskMerge();
            master_node->disk_merge_.store(false);

            return nullptr; 
        },
        master_node);
    pthread_detach(*(master_node->disk_thread_));
}

template <typename key, typename value>
bool Reducer<key, value>::WriteToDisk(const std::string &file_name, const std::string &file_content)
{
    int fd = open(&file_name[0], O_CREAT | O_RDWR, 0777);
    write(fd, &file_content[0], file_content.size());

    return true;
}

template <typename key, typename value>
bool Reducer<key, value>::SetDefaultReduceFunction()
{
    reduce_ = MapReduceUtil::DefaultReduceFunction;

    return true;
}

template <typename key, typename value>
Imagine_Rpc::Stub* Reducer<key, value>::GenerateNewStub()
{
    return new Imagine_Rpc::Stub(*stub_);
}

} // namespace Imagine_MapReduce

#endif