#ifndef IMAGINE_MAPREDUCE_REDUCER_H
#define IMAGINE_MAPREDUCE_REDUCER_H

#include <fcntl.h>
#include <atomic>
// #include<list>

#include <RpcServer.h>
#include <RpcClient.h>

#include "MapReduceUtil.h"
#include "Callbacks.h"

namespace Imagine_MapReduce
{

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
    Reducer(const std::string &ip, const std::string &port, const std::string &keeper_ip = "", const std::string &keeper_port = "", ReduceCallback reduce = nullptr);

    ~Reducer();

    void loop();

    std::vector<std::string> Register(const std::vector<std::string> &input);

    std::vector<std::string> Reduce(const std::vector<std::string> &input);

    void StartMergeThread(MasterNode *master_node);

    bool WriteToDisk(const std::string &file_name, const std::string &file_content);

    bool SetDefaultReduceFunction();

 private:
    const std::string ip_;
    const std::string port_;

    const std::string keeper_ip_;
    const std::string keeper_port_;

    ReduceCallback reduce_;

    RpcServer *rpc_server_;

    pthread_mutex_t *map_lock_;

    std::unordered_map<std::pair<std::string, std::string>, MasterNode *, HashPair, EqualPair> master_map_;
};

template <typename key, typename value>
Reducer<key, value>::Reducer(const std::string &ip, const std::string &port, const std::string &keeper_ip, const std::string &keeper_port, ReduceCallback reduce)
                             : ip_(ip), port_(port), keeper_ip_(keeper_ip), keeper_port_(keeper_port), reduce_(reduce)
{
    if (reduce_ == nullptr) {
        SetDefaultReduceFunction();
    }

    map_lock_ = new pthread_mutex_t;
    if (pthread_mutex_init(map_lock_, nullptr) != 0) {
        throw std::exception();
    }

    rpc_server_ = new RpcServer(ip_, port_, keeper_ip_, keeper_port_);
    rpc_server_->Callee("Reduce", std::bind(&Reducer::Reduce, this, std::placeholders::_1));
    rpc_server_->Callee("Register", std::bind(&Reducer::Register, this, std::placeholders::_1));
}

template <typename key, typename value>
Reducer<key, value>::~Reducer()
{
    delete rpc_server_;
    delete map_lock_;
}

template <typename key, typename value>
void Reducer<key, value>::loop()
{
    rpc_server_->loop();
}

template <typename key, typename value>
std::vector<std::string> Reducer<key, value>::Register(const std::vector<std::string> &input)
{
    /*
    信息格式
        -1.文件总数
        -2.文件名列表
        -3.master_ip
        -4.master_port
    */
    printf("This is Register Method !\n");
    // for(int i=0;i<input.size();i++)printf("%s\n",&input[i][0]);
    int new_master_file_num = MapReduceUtil::StringToInt(input[0]);
    std::pair<std::string, std::string> new_master_pair = std::make_pair(input[new_master_file_num + 1], input[new_master_file_num + 2]);
    pthread_mutex_lock(map_lock_);
    if (master_map_.find(new_master_pair) != master_map_.end()) {
        // 重复注册
        throw std::exception();
    }
    MasterNode *new_master = new MasterNode;
    new_master->file_num_ = new_master_file_num;
    for (int i = 0; i < new_master_file_num; i++) {
        std::unordered_map<std::string, int> temp_map;
        new_master->files_.insert(std::make_pair(input[i + 1], temp_map));
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

    master_map_.insert(std::make_pair(new_master_pair, new_master));
    // rpc_server->SetTimer();
    pthread_mutex_unlock(map_lock_);
    std::vector<std::string> output;
    output.push_back("Receive!");

    return output;
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
    printf("this is Reduce Method !\n");
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
    std::unordered_map<std::string, std::unordered_map<std::string, int>>::iterator file_it = master_node->files.find(file_name); // 找到对应的file
    if (file_it == master_it->second->files.end()) {
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

    pthread_mutex_lock(master_node->memory_list_lock);
    master_node->memory_file_list.push_front(RpcClient::Call(method_name, parameters, mapper_ip, mapper_port)[0]);
    if ((*master_node->memory_file_list.begin()).size()) {
        printf("split file %s content : \n%s\n", &split_name[0], &(*master_node->memory_file_list.begin())[0]);
    } else {
        printf("split file %s content : NoContent!\n", &split_name[0]);
    }
    master_node->memory_file_size += master_node->memory_file_list.front().size();
    pthread_mutex_unlock(master_node->memory_list_lock);

    pthread_mutex_lock(map_lock_);
    split_it = file_it->second.find(split_name);
    if (split_it != file_it->second.end()) {
        // 重复接收同一个split文件
        throw std::exception();
    }
    file_it->second.insert(std::make_pair(split_name, 1));
    master_it = master_map_.find(master_pair);
    file_it = master_it->second->files.find(file_name);
    if (file_it->second.size() == split_num) {
        master_it->second->count++;
    }

    if (master_it->second->count == master_it->second->files.size()) {
        // 所有文件接收完毕,可以开始执行
        master_node->receive_all.store(true);
        while (master_node->disk_merge.load());
        printf("TaskOver!\n");
    }
    pthread_mutex_unlock(map_lock_);

    std::vector<std::string> output;
    output.push_back("Receive!\n");

    return output;
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
            MasterNode* master_node_ = (MasterNode*)argv;
            while (!(master_node->receive_all_.load())) {
                if (master_node->memory_file_size_ >= DEFAULT_MEMORY_MERGE_SIZE) {
                    master_node->MemoryMerge();
                }
            }
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
            master_node->disk_merge.store(false);

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

} // namespace Imagine_MapReduce

#endif