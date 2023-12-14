#ifndef IMAGINE_MAPREDUCE_REDUCER_H
#define IMAGINE_MAPREDUCE_REDUCER_H

#include "log_macro.h"
#include "common_macro.h"
#include "MapReduceUtil.h"
#include "common_typename.h"
#include "ReduceTaskService.h"
#include "StartReduceService.h"
#include "RetrieveSplitFileMessage.pb.h"

#include <atomic>
#include <fcntl.h>
#include <fstream>

namespace Imagine_MapReduce
{

namespace Internal
{

template <typename key, typename value>
class StartReduceService;

template <typename key, typename value>
class ReduceTaskService;

} // namespace Internal

template <typename key, typename value>
class Reducer
{
 public:
    class MasterNode
    {
     public:
        MasterNode() : count_(0), receive_all_(false), memory_merge_(true), disk_merge_(true), memory_file_size_(0)
        {
        }

        MasterNode(const std::vector<std::string>& file_list) : count_(0), receive_all_(false), memory_merge_(true), disk_merge_(true), memory_file_size_(0)
        {
            file_num_ = file_list.size();
            for (size_t i = 0; i < file_num_; i++) {
                std::unordered_map<std::string, int> temp_map;
                files_.insert(std::make_pair(file_list[i], temp_map));
            }

            memory_thread_ = new pthread_t;
            disk_thread_ = new pthread_t;
            memory_list_lock_ = new pthread_mutex_t;
            disk_list_lock_ = new pthread_mutex_t;

            if (pthread_mutex_init(memory_list_lock_, nullptr) != 0) {
                throw std::exception();
            }
            if (pthread_mutex_init(disk_list_lock_, nullptr) != 0) {
                throw std::exception();
            }
        }

        MasterNode* AddFileToMemoryList(const std::string& file_content, const std::string& split_file_name = "")
        {
            pthread_mutex_lock(memory_list_lock_);
            memory_file_list_.push_front(file_content);
            memory_file_size_ += memory_file_list_.front().size();

            if (memory_file_list_.begin()->size()) {
                IMAGINE_MAPREDUCE_LOG("split file %s content : %s", split_file_name.c_str(), memory_file_list_.begin()->c_str());
            } else {
                IMAGINE_MAPREDUCE_LOG("split file %s content : NoContent!", split_file_name.c_str());
            }
            pthread_mutex_unlock(memory_list_lock_);

            return this;
        }

        // 一个文件的split集齐
        MasterNode* ReceiveFullFile()
        {
            count_++;

            return this;
        }

        size_t ReceivedFileNum() const
        {
            return count_;
        }

        size_t GetFileNum() const { return file_num_; }

        std::unordered_map<std::string, std::unordered_map<std::string, int>>::iterator FindFileIterator(const std::string& file_name)
        {
            auto it = files_.find(file_name);
            if (it == files_.end()) {
                IMAGINE_MAPREDUCE_LOG("File name Error! Get File Name %s", file_name.c_str());
                throw std::exception();
                return nullptr;
            }

            return it;
        }

        const MasterNode* LockMemoryList() const
        {
            pthread_mutex_lock(memory_list_lock_);

            return this;
        }

        const MasterNode* UnLockMemoryList() const
        {
            pthread_mutex_unlock(memory_list_lock_);

            return this;
        }

        const MasterNode* LockDiskList() const
        {
            pthread_mutex_lock(disk_list_lock_);

            return this;
        }

        const MasterNode* UnLockDiskList() const
        {
            pthread_mutex_unlock(disk_list_lock_);

            return this;
        }

        pthread_t* GetMemoryThreadPtr() const { return memory_thread_; }

        pthread_t* GetDiskThreadPtr() const { return disk_thread_; }

        bool IsReceiveAll() const
        {
            return receive_all_.load();
        }

        MasterNode* SetReceiveAllStat(bool flag)
        {
            receive_all_.store(flag);

            return this;
        }

        size_t GetMemoryFileSize() const { return memory_file_size_.load(); }

        bool GetMemoryMergeStat() const { return memory_merge_.load(); }

        size_t GetDiskFileNum() const 
        {
            pthread_mutex_lock(disk_list_lock_);
            size_t file_num = disk_file_list_.size();
            pthread_mutex_unlock(disk_list_lock_);

            return file_num;
        }

        MasterNode* SetMemoryMergeStat(bool flag)
        {
            memory_merge_.store(flag);

            return this;
        }

        bool GetDiskMergeStat() const { return disk_merge_.load(); }

        MasterNode* SetDiskMergeStat(bool flag)
        {
            disk_merge_.store(flag);

            return this;
        }

        void MemoryMerge()
        {
            pthread_mutex_lock(memory_list_lock_);

            std::vector<std::string> merge_list;
            int merge_num = memory_file_list_.size();
            for (auto it = memory_file_list_.begin(); it != memory_file_list_.end(); it++) {
                merge_list.push_back(std::move(*it));
            }
            memory_file_list_.clear();
            memory_file_size_.store(0);

            std::string merge_name = "memory_merge_" + MapReduceUtil::IntToString(memory_merge_id_++) + ".txt";
            int fd = open(&merge_name[0], O_CREAT | O_RDWR, 0777);
            int *idxs = new int[merge_num];

            std::priority_queue<KVReader *, std::vector<KVReader *>, KVReaderCmp> heap;
            for (int i = 0; i < merge_num; i++) {
                std::string memory_key;
                std::string memory_value;
                idxs[i] = 0;
                if (MapReduceUtil::ReadKVReaderFromMemory(merge_list[i], idxs[i], memory_key, memory_value))
                    heap.push(new KVReader(memory_key, memory_value, i));
            }
            while (heap.size()) {
                KVReader *next_kv = heap.top();
                heap.pop();
                std::string memory_key;
                std::string memory_value;
                if (MapReduceUtil::ReadKVReaderFromMemory(merge_list[next_kv->reader_idx_], idxs[next_kv->reader_idx_], memory_key, memory_value)) {
                    heap.push(new KVReader(memory_key, memory_value, next_kv->reader_idx_));
                }
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

     private:
        size_t count_;                                                                      // 计数,用于判断是否可以开始执行Reduce
        size_t file_num_;                                                                   // master要处理的文件总数
        std::unordered_map<std::string, std::unordered_map<std::string, int>> files_;       // 存储源文件名到splits的映射
        std::atomic<bool> receive_all_;                                                     // 标识所有文件是否全部接收完毕
        std::atomic<bool> memory_merge_;                                                    // 标识memory是否退出merge
        std::atomic<bool> disk_merge_;                                                      // 标识disk是否退出merge
        int memory_merge_id_;                                                               // memory merge的自增Id
        int disk_merge_id_;                                                                 // disk merge的自增Id
        pthread_t *memory_thread_;                                                          // memory merge的线程  
        pthread_t *disk_thread_;                                                            // disk merge的线程
        pthread_mutex_t *memory_list_lock_;                                                 // memory_file_list_的锁
        pthread_mutex_t *disk_list_lock_;                                                   // disk_file_list_的锁
        std::atomic<size_t> memory_file_size_;                                              // 存储内存中储存的文件的总大小
        std::list<std::string> memory_file_list_;                                           // 存储split文件内容
        std::list<std::string> disk_file_list_;                                             // 存储磁盘中储存的文件的文件名
    };

 public:
    Reducer();

    Reducer(const std::string& profile_name);

    Reducer(const YAML::Node& config);

    ~Reducer();

    void Init(const std::string& profile_name);

    void Init(const YAML::Node& config);

    void InitLoop(const YAML::Node& config);

    void loop();

    std::vector<std::string> Register(const std::vector<std::string> &input);

    Reducer<key, value>* RegisterMaster(std::pair<std::string, std::string> master_node, const std::vector<std::string>& file_list);

    void ReceiveSplitFileData(const std::pair<std::string, std::string>& master_pair, const std::pair<std::string, std::string>& mapper_pair, const std::string& file_name, const std::string& split_file_name, size_t split_num);

    void StartMergeThread(MasterNode *master_node);

    Reducer<key, value>* WriteToDisk(const std::string &file_name, const std::string &file_content);

    Imagine_Rpc::Stub* GenerateNewStub() const;

 private:
    // 配置文件字段
    bool singleton_log_mode_;
    Logger* logger_;

 private:
    ReduceCallback reduce_;                                                                                             // 用户定义的Reduce函数(暂未启用)
    Imagine_Rpc::RpcServer *rpc_server_;                                                                                // rpc服务器对象
    pthread_mutex_t *map_lock_;                                                                                         // master_map_的锁
    std::unordered_map<std::pair<std::string, std::string>, MasterNode *, HashPair, EqualPair> master_map_;             // 所有请求Reduce任务的Master集合
    Imagine_Rpc::Stub* stub_;                                                                                           // stub原型
};

template <typename key, typename value>
Reducer<key, value>::Reducer()
{
}

template <typename key, typename value>
Reducer<key, value>::Reducer(const std::string& profile_name)
{
    Init(profile_name);
}

template <typename key, typename value>
Reducer<key, value>::Reducer(const YAML::Node& config)
{
    Init(config);
}

template <typename key, typename value>
Reducer<key, value>::~Reducer()
{
    delete rpc_server_;
    delete map_lock_;
    delete stub_;
}

template <typename key, typename value>
void Reducer<key, value>::Init(const std::string& profile_name)
{
    if (profile_name == "") {
        throw std::exception();
    }

    YAML::Node config = YAML::LoadFile(profile_name);
    Init(config);
}

template <typename key, typename value>
void Reducer<key, value>::Init(const YAML::Node& config)
{
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

template <typename key, typename value>
void Reducer<key, value>::InitLoop(const YAML::Node& config)
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
    MasterNode *new_master = new MasterNode(file_list);

    StartMergeThread(new_master); // 开启merge线程

    master_map_.insert(std::make_pair(master_pair, new_master));
    pthread_mutex_unlock(map_lock_);

    return this;
}

template <typename key, typename value>
void Reducer<key, value>::ReceiveSplitFileData(const std::pair<std::string, std::string>& master_pair, const std::pair<std::string, std::string>& mapper_pair, const std::string& file_name, const std::string& split_file_name, size_t split_num)
{
    pthread_mutex_lock(map_lock_);
    typename std::unordered_map<std::pair<std::string, std::string>, MasterNode *, HashPair, EqualPair>::iterator master_it = master_map_.find(master_pair); // 找到MasterNode*
    if (master_it == master_map_.end()) {
        // 该master没有register
        IMAGINE_MAPREDUCE_LOG("NO Register Master Error!");
        throw std::exception();
    }
    MasterNode *master_node = master_it->second;
    std::unordered_map<std::string, std::unordered_map<std::string, int>>::iterator file_it = master_node->FindFileIterator(file_name); // 找到对应的file
    std::unordered_map<std::string, int>::iterator split_it = file_it->second.find(split_file_name);
    if (split_it != file_it->second.end()) {
        // 重复接收同一个split文件
        IMAGINE_MAPREDUCE_LOG("Repeat Split File Error! Get Split File Name %s", split_file_name.c_str());
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

    master_node->AddFileToMemoryList(response_msg.split_file_content_(), split_file_name);

    pthread_mutex_lock(map_lock_);
    file_it->second.insert(std::make_pair(split_file_name, 1));
    if (file_it->second.size() == split_num) {
        master_node->ReceiveFullFile();
    }

    if (master_node->ReceivedFileNum() == master_node->GetFileNum()) {
        // 所有文件接收完毕,可以开始执行
        IMAGINE_MAPREDUCE_LOG("Receive All Split File! Start Finally Merge!");
        master_node->SetReceiveAllStat(true);
        while (master_node->GetDiskMergeStat());
        IMAGINE_MAPREDUCE_LOG("TaskOver!");
    }
    pthread_mutex_unlock(map_lock_);
}

template <typename key, typename value>
void Reducer<key, value>::StartMergeThread(MasterNode *master_node)
{
    // 此函数调用时机在Register的map_lock中,故不需要加锁
    pthread_create(
        master_node->GetMemoryThreadPtr(), nullptr, [](void *argv) -> void *
        {
            MasterNode* master_node = (MasterNode*)argv;
            while (!(master_node->IsReceiveAll())) {
                if (master_node->GetMemoryFileSize() >= DEFAULT_MEMORY_MERGE_SIZE) {
                    master_node->MemoryMerge();
                }
            }
            IMAGINE_MAPREDUCE_LOG("Memory Merge Over! total size is %ld", master_node->GetMemoryFileSize());
            master_node->MemoryMerge();
            master_node->SetMemoryMergeStat(false);

            return nullptr; 
        },
        master_node);

    pthread_detach(*(master_node->GetMemoryThreadPtr()));

    pthread_create(
        master_node->GetDiskThreadPtr(), nullptr, [](void *argv) -> void *
        {

            MasterNode* master_node = (MasterNode*)argv;
            while (master_node->GetMemoryMergeStat()) {
                if (master_node->GetDiskFileNum() >= DEFAULT_DISK_MERGE_NUM) {
                    master_node->DiskMerge();
                }
            }
            master_node->DiskMerge();
            master_node->SetDiskMergeStat(false);

            return nullptr; 
        },
        master_node);
    pthread_detach(*(master_node->GetDiskThreadPtr()));
}

template <typename key, typename value>
Reducer<key, value>* Reducer<key, value>::WriteToDisk(const std::string &file_name, const std::string &file_content)
{
    int fd = open(file_name.c_str(), O_CREAT | O_RDWR, 0777);
    write(fd, file_content.c_str(), file_content.size());

    return this;
}

template <typename key, typename value>
Imagine_Rpc::Stub* Reducer<key, value>::GenerateNewStub() const
{
    return new Imagine_Rpc::Stub(*stub_);
}

} // namespace Imagine_MapReduce

#endif