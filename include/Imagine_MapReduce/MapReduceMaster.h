#ifndef IMAGINE_MAPREDUCE_MASTER_H
#define IMAGINE_MAPREDUCE_MASTER_H

#include "Imagine_Muduo/EventLoop.h"
#include "Imagine_Rpc/RpcServer.h"
#include "common_definition.h"

#include <atomic>

/*
MapReduce通信格式
    -第一个字段表示身份(Mapper、Reducer、MapReduceMaster)
    -第二个字段表示消息类型
        -start:任务开始
        -process:任务进度
        -finish:任务完成
        -receive:收到信息
        -sendfile:要求mapper将文件传输给reducer
    -后面字段表示消息内容(任务进度信息或任务完成的地址或文件传输地址)
*/

namespace Imagine_MapReduce
{

class MapReduceMaster
{
 public:
    class ReducerNode
    {
     public:
        ReducerNode() : ip_(""), port_(""), is_ready_(false)
        {
            reducer_lock_ = new pthread_mutex_t;
            pthread_mutex_init(reducer_lock_, nullptr);
        }

     public:
        std::string ip_;
        std::string port_;
        std::vector<std::string> files_;

        pthread_mutex_t *reducer_lock_;
        std::atomic<bool> is_ready_;
    };

 public:
    MapReduceMaster();

    MapReduceMaster(std::string profile_name);

    MapReduceMaster(YAML::Node config);

    MapReduceMaster(const std::string &ip, const std::string &port, const std::string &keeper_ip, const std::string &keeper_port, const size_t reducer_num = DEFAULT_REDUCER_NUM);

    ~MapReduceMaster();

    void Init(std::string profile_name);

    void Init(YAML::Node config);

    void InitLoop(YAML::Node config);

    void InitProfilePath(std::string profile_name);

    void GenerateSubmoduleProfile(YAML::Node config);

    // 目前只支持单文件处理,因为要区分不同文件则不同Mapper应该对应在不同文件的机器
    bool MapReduce(const std::vector<std::string> &file_list, const size_t reducer_num = DEFAULT_REDUCER_NUM, const size_t split_size = DEFAULT_READ_SPLIT_SIZE);

    // 向Reducer发送一个预热信息,注册当前MapReduceMaster,并开启心跳
    bool StartReducer(const std::string &reducer_ip, const std::string &reducer_port);

    bool ConnMapper();

    bool ConnReducer(const std::string &split_num, const std::string &file_name, const std::string &split_file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &reducer_ip, const std::string &reducer_port);

    std::vector<std::string> MapReduceCenter(const std::vector<std::string> &message);

    std::string ProcessMapperMessage(const std::vector<std::string> &message);

    std::string ProcessReducerMessage(const std::vector<std::string> &message);

    void loop();

    bool SetTaskFile(std::vector<std::string> &files);

 private:
    std::string ip_;
    std::string port_;
    std::string zookeeper_ip_;
    std::string zookeeper_port_;
    size_t reducer_num_;
    size_t split_size_;
    std::vector<std::string> file_list_;
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
    // 用于接收mapper和reducer的task进度信息
    Imagine_Rpc::RpcServer *rpc_server_;
    pthread_t *rpc_server_thread_;
    Imagine_Tool::Logger* logger_;

    std::vector<std::string> files_;                        // 需要mapper处理的所有文件的文件名

    std::unordered_map<int, ReducerNode *> reducer_map_;    // reducer对应的ip和port信息
};

} // namespace Imagine_MapReduce

#endif