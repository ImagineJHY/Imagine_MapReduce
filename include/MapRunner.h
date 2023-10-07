#ifndef IMAGINE_MAPREDUCE_MAPRUNNER_H
#define IMAGINE_MAPREDUCE_MAPRUNNER_H

#include <string.h>
#include <queue>

#include "RecordReader.h"
#include "Mapper.h"
#include "OutputFormat.h"
#include "KVBuffer.h"
#include "Callbacks.h"
#include "Partitioner.h"

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value, typename key, typename value>
class Mapper;

template <typename reader_key, typename reader_value, typename key, typename value>
class MapRunner
{
 public:
    class SpillReader
    {
     public:
        SpillReader(std::string spill_key, std::string spill_value, int fd) : spill_key_(spill_key), spill_value_(spill_value), fd_(fd) {}

     public:
        std::string spill_key_;
        std::string spill_value_;
        int fd_;
    };

    class SpillReaderCmp
    {
     public:
        bool operator()(SpillReader *a, SpillReader *b)
        {
            return strcmp(&a->spill_key_[0], &b->spill_key_[0]) < 0 ? true : false;
        }
    };

 public:
    // MapRunner(int split_id_, int split_num_, const std::string file_name_, int partition_num_=DEFAULT_PARTITION_NUM):split_id(split_id_),split_num(split_num_),file_name(file_name_),partition_num(partition_num_)
    // {
    //     buffer=new KVBuffer(partition_num,split_id_,spill_files);
    //     Init();
    // }

    MapRunner(int split_id, int split_num, const std::string file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &master_ip, const std::string &master_port, MAP map, Partitioner<key> *partitioner, OutputFormat<key, value> *output_format, RpcServer *rpc_server, int partition_num = DEFAULT_PARTITION_NUM)
        : split_id_(split_id), split_num_(split_num), partition_num_(partition_num), file_name_(file_name), master_ip_(master_ip), master_port_(master_port), mapper_ip_(mapper_ip), mapper_port_(mapper_port), output_format_(output_format), partitioner_(partitioner), map_(map), rpc_server_(rpc_server)
    {
        buffer_ = new KVBuffer(partition_num_, split_id_, spill_files_);
        Init();
    }

    ~MapRunner() {}

    void Init()
    {
        spilling_thread_ = new pthread_t;
        if (!spilling_thread_) {
            throw std::exception();
        }
    }

    bool SetIp(const std::string &ip)
    {
        master_ip_ = ip;
        return true;
    }

    bool SetPort(const std::string &port)
    {
        master_port_ = port;
        return true;
    }

    bool SetRecordReader(std::shared_ptr<RecordReader<reader_key, reader_value>> record_reader)
    {
        record_reader_ = record_reader;
        return true;
    }

    bool SetMapFunction(MAP map)
    {
        map_ = map;
        return true;
    }

    bool SetTimerCallback(MAPTIMER timer_callback)
    {
        timer_callback_ = timer_callback;
        return true;
    }

    bool SetRpcServer(RpcServer *rpc_server)
    {
        rpc_server_ = rpc_server;
        return true;
    }

    int GetSplitNum() { return split_num_; }

    std::string GetFileName() { return file_name_; }

    std::string GetMapperIp() { return mapper_ip_; }

    std::string GetMapperPort() { return mapper_port_; }

    std::string GetMasterIp() { return master_ip_; }

    std::string GetMasterPort() { return master_port_; }

    std::shared_ptr<RecordReader<reader_key, reader_value>> GetRecordReader() { return record_reader_; }

    MAP GetMap() { return map_; }

    MAPTIMER GetTimerCallback() { return timer_callback_; }

    RpcServer *GetRpcServer() { return rpc_server_; }

    OutputFormat<key, value> *GetOutPutFormat() { return output_format_; }

    int GetId() { return split_id_; }

    bool WriteToBuffer(const std::pair<key, value> &content)
    {
        std::pair<char *, char *> pair = output_format_->ToString(content);
        bool flag = buffer_->WriteToBuffer(pair, partitioner_->Partition(content.first));
        delete[] pair.first;
        delete[] pair.second;
        return flag;
    }

    bool StartSpillingThread()
    {
        pthread_create(
            spilling_thread_, nullptr, [](void *argv) -> void *
            {
                KVBuffer* buffer = (KVBuffer*)argv;
                buffer->Spilling();

                delete buffer;

                return nullptr;
            },
            buffer_);
        pthread_detach(*spilling_thread_);

        return true;
    }

    bool CompleteMapping()
    {
        // 溢写缓冲区全部内容
        buffer_->SpillBuffer(); // 保证全部spill完毕
        Combine();

        return true;
    }

    bool Combine() // 合并spill文件到shuffle文件
    {
        /*
        Spill过程、Shuffle过程以及Merge过程中,对于用户要写的每一对key,value:
        -" "作为key和value的分隔符
        -"\r\n"作为每一对数据的分隔符
        */
        printf("Start Combining!\n");
        const int spill_num = spill_files_[0].size();
        for (int i = 0; i < partition_num_; i++) {
            std::string shuffle_file = "split" + MapReduceUtil::IntToString(split_id_) + "_shuffle_" + MapReduceUtil::IntToString(i + 1) + ".txt";
            shuffle_files_.push_back(shuffle_file);
            int shuffle_fd = open(&shuffle_file[0], O_CREAT | O_RDWR, 0777);
            int fds[spill_num];
            std::priority_queue<SpillReader *, std::vector<SpillReader *>, SpillReaderCmp> heap;
            // printf("spill_num is %d\n",spill_num);
            for (int j = 0; j < spill_num; j++) {
                fds[j] = open(&spill_files_[i][j][0], O_RDWR);
                std::string spill_key;
                std::string spill_value;
                if (SpillRead(fds[j], spill_key, spill_value)) {
                    heap.push(new SpillReader(spill_key, spill_value, fds[j]));
                }
            }

            while (heap.size()) {
                SpillReader *next = heap.top();
                heap.pop();
                std::string spill_key;
                std::string spill_value;
                if (SpillRead(next->fd_, spill_key, spill_value)) {
                    heap.push(new SpillReader(spill_key, spill_value, next->fd_));
                }
                write(shuffle_fd, &next->spill_key_[0], next->spill_key_.size());
                char c = ' ';
                write(shuffle_fd, &c, 1);
                write(shuffle_fd, &next->spill_value_[0], next->spill_value_.size());
                char cc[] = "\r\n";
                write(shuffle_fd, cc, 2);

                delete next;
            }

            for (int j = 0; j < spill_num; j++) {
                close(fds[j]);
                remove(&spill_files_[i][j][0]);
            }
            close(shuffle_fd);
        }

        return true;
    }

    bool SpillRead(int fd, std::string &spill_key, std::string &spill_value) // 从spill文件读取kv对
    {
        // 假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
        bool flag = false;
        char c;
        int ret = read(fd, &c, 1);
        if (!ret) {
            return false; // 文件读空
        }

        spill_key.push_back(c);
        while (1) {
            // printf("here\n");
            read(fd, &c, 1);
            if (c == ' ') {
                break;
            }
            spill_key.push_back(c);
        }
        while (1) {
            read(fd, &c, 1);
            spill_value.push_back(c);
            if (c == '\n' && flag) {
                spill_value.pop_back();
                spill_value.pop_back();
                return true;
            }
            if (c == '\r') {
                flag = true;
            } else {
                flag = false;
            }
        }
    }

    std::vector<std::string> &GetShuffleFile() { return shuffle_files_; }

 private:
    const int split_id_;             // split的id
    const int split_num_;
    const int partition_num_;        // 分区数(shuffle数目)
    const std::string file_name_;    // 源文件名

    const std::string master_ip_;    // master的ip
    const std::string master_port_;  // master的port
    const std::string mapper_ip_;    // mapper的ip
    const std::string mapper_port_;  // mapper的port

    std::shared_ptr<RecordReader<reader_key, reader_value>> record_reader_;
    OutputFormat<key, value> *output_format_;
    Partitioner<key> *partitioner_;
    MAP map_;
    MAPTIMER timer_callback_;

    RpcServer *rpc_server_;

    // 缓冲区
    KVBuffer *buffer_;
    pthread_t *spilling_thread_;

    std::vector<std::vector<std::string>> spill_files_;
    std::vector<std::string> shuffle_files_;
};

} // namespace Imagine_MapReduce

#endif