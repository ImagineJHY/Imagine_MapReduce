#ifndef IMAGINE_MAPREDUCE_MASTER_H
#define IMAGINE_MAPREDUCE_MASTER_H

#include<atomic>

#include<Imagine_Muduo/Imagine_Muduo/EventLoop.h>
#include<Imagine_Rpc/Imagine_Rpc/RpcServer.h>

#include"Callbacks.h"

using namespace Imagine_Rpc;

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

namespace Imagine_MapReduce{

class MapReduceMaster
{

public:
    class ReducerNode{
        public:
            ReducerNode():ip(""),port(""),is_ready(false)
            {
                reducer_lock=new pthread_mutex_t;
            }

        public:
            std::string ip;
            std::string port;
            std::vector<std::string> files;

            pthread_mutex_t* reducer_lock;
            std::atomic<bool> is_ready;
    };

public:

    MapReduceMaster(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_, const std::string& keeper_port_, const int reducer_num_=DEFAULT_REDUCER_NUM);

    ~MapReduceMaster();

    //目前只支持单文件处理,因为要区分不同文件则不同Mapper应该对应在不同文件的机器
    bool MapReduce(const std::vector<std::string>& file_list, const int reducer_num_=DEFAULT_REDUCER_NUM, const int split_size=DEFAULT_READ_SPLIT_SIZE);

    //向Reducer发送一个预热信息,注册当前MapReduceMaster,并开启心跳
    bool StartReducer(const std::string& reducer_ip, const std::string& reducer_port);

    bool ConnMapper();

    bool ConnReducer(const std::string& split_num, const std::string& file_name, const std::string& split_file_name, const std::string& mapper_ip, const std::string& mapper_port, const std::string& reducer_ip, const std::string& reducer_port);

    std::vector<std::string> MapReduceCenter(const std::vector<std::string>& message);

    std::string ProcessMapperMessage(const std::vector<std::string>& message);

    std::string ProcessReducerMessage(const std::vector<std::string>& message);

    void loop();

    bool SetTaskFile(std::vector<std::string>& files_);

private:
    const std::string ip;
    const std::string port;

    const std::string keeepr_ip;
    const std::string keeper_port;

    const int reducer_num;

    //用于接收mapper和reducer的task进度信息
    RpcServer* rpc_server;
    pthread_t* rpc_server_thread;

    std::vector<std::string> files;//需要mapper处理的所有文件的文件名

    std::unordered_map<int,ReducerNode*> reducer_map;//reducer对应的ip和port信息
};


}



#endif