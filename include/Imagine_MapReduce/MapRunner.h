#ifndef IMAGINE_MAPREDUCE_MAPRUNNER_H
#define IMAGINE_MAPREDUCE_MAPRUNNER_H

#include "KVBuffer.h"
#include "log_macro.h"
#include "Partitioner.h"
#include "MapReduceUtil.h"
#include "common_typename.h"

#include "Imagine_Rpc/Imagine_Rpc.h"

#include <queue>
#include <string.h>

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value>
class RecordReader;

template <typename key, typename value>
class OutputFormat;

template <typename reader_key, typename reader_value, typename key, typename value>
class MapRunner
{
 public:
    class SpillReader
    {
     public:
        SpillReader(const std::string& spill_key, const std::string& spill_value, int fd) : spill_key_(spill_key), spill_value_(spill_value), fd_(fd)
        {
        }

     private:
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
    MapRunner(int split_id, int split_num, const std::string file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &master_ip, const std::string &master_port, MapCallback<reader_key, reader_value, key, value> map, Partitioner<key> *partitioner, OutputFormat<key, value> *output_format, Imagine_Rpc::RpcServer *rpc_server, int partition_num = DEFAULT_PARTITION_NUM);

    ~MapRunner();

    void Init();

    MapRunner<reader_key, reader_value, key, value>* SetThread(pthread_t* thread);

    MapRunner<reader_key, reader_value, key, value>* SetIp(const std::string &ip);

    MapRunner<reader_key, reader_value, key, value>* SetPort(const std::string &port);

    MapRunner<reader_key, reader_value, key, value>* SetRecordReader(std::shared_ptr<RecordReader<reader_key, reader_value>> record_reader);

    MapRunner<reader_key, reader_value, key, value>* SetMapFunction(MapCallback<reader_key, reader_value, key, value> map);

    MapRunner<reader_key, reader_value, key, value>* SetTimerCallback(MapTimerCallback<reader_key, reader_value> timer_callback);

    MapRunner<reader_key, reader_value, key, value>* SetRpcServer(Imagine_Rpc::RpcServer *rpc_server);

    MapRunner<reader_key, reader_value, key, value>* SetHeartBeatStub(Imagine_Rpc::Stub* stub);

    MapRunner<reader_key, reader_value, key, value>* SetCompleteStub(Imagine_Rpc::Stub* stub);

    int GetSplitNum() const;

    std::string GetFileName() const;

    std::string GetMapperIp() const;

    std::string GetMapperPort() const;

    std::string GetMasterIp() const;

    std::string GetMasterPort() const;

    std::shared_ptr<RecordReader<reader_key, reader_value>> GetRecordReader() const;

    MapCallback<reader_key, reader_value, key, value> GetMap() const;

    MapTimerCallback<reader_key, reader_value> GetTimerCallback() const;

    Imagine_Rpc::RpcServer *GetRpcServer() const;

    OutputFormat<key, value> *GetOutPutFormat() const;

    int GetId() const;

    std::shared_ptr<Imagine_Rpc::Stub> GetHeartBeatStub() const;

    std::shared_ptr<Imagine_Rpc::Stub> GetCompleteStub() const;

    bool WriteToBuffer(const std::pair<key, value> &content);

    bool StartSpillingThread();

    MapRunner<reader_key, reader_value, key, value>* CompleteMapping();

    MapRunner<reader_key, reader_value, key, value>* Combine(); // 合并spill文件到shuffle文件

    bool SpillRead(int fd, std::string &spill_key, std::string &spill_value) const; // 从spill文件读取kv对

    const std::vector<std::string> &GetShuffleFile() const;

 private:
    const int split_id_;             // split的id
    const int split_num_;
    const int partition_num_;        // 分区数(shuffle数目)
    const std::string file_name_;    // 源文件名

    const std::string master_ip_;    // master的ip
    const std::string master_port_;  // master的port
    const std::string mapper_ip_;    // mapper的ip
    const std::string mapper_port_;  // mapper的port

    pthread_t* thread_;

    std::shared_ptr<RecordReader<reader_key, reader_value>> record_reader_;
    OutputFormat<key, value> *output_format_;
    Partitioner<key> *partitioner_;
    MapCallback<reader_key, reader_value, key, value> map_;
    MapTimerCallback<reader_key, reader_value> timer_callback_;

    Imagine_Rpc::RpcServer *rpc_server_;

    // 缓冲区
    KVBuffer *buffer_;
    pthread_t *spilling_thread_;

    std::vector<std::vector<std::string>> spill_files_;
    std::vector<std::string> shuffle_files_;

    std::shared_ptr<Imagine_Rpc::Stub> heartbeat_stub_;
    std::shared_ptr<Imagine_Rpc::Stub> complete_stub_;
};

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>::MapRunner(int split_id, int split_num, const std::string file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &master_ip, const std::string &master_port, MapCallback<reader_key, reader_value, key, value> map, Partitioner<key> *partitioner, OutputFormat<key, value> *output_format, Imagine_Rpc::RpcServer *rpc_server, int partition_num)
    : split_id_(split_id), split_num_(split_num), partition_num_(partition_num), file_name_(file_name), master_ip_(master_ip), master_port_(master_port), mapper_ip_(mapper_ip), mapper_port_(mapper_port), output_format_(output_format), partitioner_(partitioner), map_(map), rpc_server_(rpc_server)
{
    buffer_ = new KVBuffer(partition_num_, split_id_, spill_files_);
    Init();
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>::~MapRunner()
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
void MapRunner<reader_key, reader_value, key, value>::Init()
{
    spilling_thread_ = new pthread_t;
    if (!spilling_thread_) {
        throw std::exception();
    }
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetThread(pthread_t* thread)
{
    thread_ = thread;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetIp(const std::string &ip)
{
    master_ip_ = ip;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetPort(const std::string &port)
{
    master_port_ = port;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetRecordReader(std::shared_ptr<RecordReader<reader_key, reader_value>> record_reader)
{
    record_reader_ = record_reader;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetMapFunction(MapCallback<reader_key, reader_value, key, value> map)
{
    map_ = map;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetTimerCallback(MapTimerCallback<reader_key, reader_value> timer_callback)
{
    timer_callback_ = timer_callback;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetRpcServer(Imagine_Rpc::RpcServer *rpc_server)
{
    rpc_server_ = rpc_server;

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetHeartBeatStub(Imagine_Rpc::Stub* stub)
{
    heartbeat_stub_.reset(stub);

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::SetCompleteStub(Imagine_Rpc::Stub* stub)
{
    complete_stub_.reset(stub);

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
int MapRunner<reader_key, reader_value, key, value>::GetSplitNum() const
{
    return split_num_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::string MapRunner<reader_key, reader_value, key, value>::GetFileName() const
{
    return file_name_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::string MapRunner<reader_key, reader_value, key, value>::GetMapperIp() const
{
    return mapper_ip_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::string MapRunner<reader_key, reader_value, key, value>::GetMapperPort() const
{
    return mapper_port_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::string MapRunner<reader_key, reader_value, key, value>::GetMasterIp() const 
{
    return master_ip_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::string MapRunner<reader_key, reader_value, key, value>::GetMasterPort() const 
{
    return master_port_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::shared_ptr<RecordReader<reader_key, reader_value>> MapRunner<reader_key, reader_value, key, value>::GetRecordReader() const
{
    return record_reader_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapCallback<reader_key, reader_value, key, value> MapRunner<reader_key, reader_value, key, value>::GetMap() const
{
    return map_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapTimerCallback<reader_key, reader_value> MapRunner<reader_key, reader_value, key, value>::GetTimerCallback() const
{
    return timer_callback_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
Imagine_Rpc::RpcServer *MapRunner<reader_key, reader_value, key, value>::GetRpcServer() const
{
    return rpc_server_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
OutputFormat<key, value> *MapRunner<reader_key, reader_value, key, value>::GetOutPutFormat() const
{
    return output_format_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
int MapRunner<reader_key, reader_value, key, value>::GetId() const
{
    return split_id_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::shared_ptr<Imagine_Rpc::Stub> MapRunner<reader_key, reader_value, key, value>::GetHeartBeatStub() const
{
    return heartbeat_stub_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
std::shared_ptr<Imagine_Rpc::Stub> MapRunner<reader_key, reader_value, key, value>::GetCompleteStub() const
{
    return complete_stub_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool MapRunner<reader_key, reader_value, key, value>::WriteToBuffer(const std::pair<key, value> &content)
{
    std::pair<char *, char *> pair = output_format_->ToString(content);
    bool flag = buffer_->WriteToBuffer(pair, partitioner_->Partition(content.first));
    
    delete[] pair.first;
    delete[] pair.second;

    return flag;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool MapRunner<reader_key, reader_value, key, value>::StartSpillingThread()
{
    pthread_create(
        spilling_thread_, nullptr, [](void *argv) -> void *
        {
            KVBuffer* buffer = (KVBuffer*)argv;
            buffer->Spilling();

            while (!(buffer->IsDeleteConditionSatisfy()));

            delete buffer;

            return nullptr;
        },
        buffer_);
    pthread_detach(*spilling_thread_);

    return true;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::CompleteMapping()
{
    // 溢写缓冲区全部内容
    buffer_->SpillBuffer(); // 保证全部spill完毕
    Combine();

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapRunner<reader_key, reader_value, key, value>* MapRunner<reader_key, reader_value, key, value>::Combine() // 合并spill文件到shuffle文件
{
    /*
    Spill过程、Shuffle过程以及Merge过程中,对于用户要写的每一对key,value:
    -" "作为key和value的分隔符
    -"\r\n"作为每一对数据的分隔符
    */
    IMAGINE_MAPREDUCE_LOG("Start Combining!");
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

    return this;
}

template <typename reader_key, typename reader_value, typename key, typename value>
bool MapRunner<reader_key, reader_value, key, value>::SpillRead(int fd, std::string &spill_key, std::string &spill_value) const // 从spill文件读取kv对
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

template <typename reader_key, typename reader_value, typename key, typename value>
const std::vector<std::string> &MapRunner<reader_key, reader_value, key, value>::GetShuffleFile() const 
{
    return shuffle_files_;
}

} // namespace Imagine_MapReduce

#endif