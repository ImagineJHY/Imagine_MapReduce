#include"MapReduceMaster.h"

#include<Imagine_Rpc/Imagine_Rpc/RpcClient.h>

#include"MapReduceUtil.h"

using namespace Imagine_Rpc;
using namespace Imagine_MapReduce;

MapReduceMaster::MapReduceMaster(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_, const std::string& keeper_port_, const int reducer_num_):
    ip(ip_),port(port_),keeepr_ip(keeper_ip_),keeper_port(keeper_port_),reducer_num(reducer_num_)
{
    int temp_port=MapReduceUtil::StringToInt(port_);
    if(temp_port<0){
        throw std::exception();
    }
    
    rpc_server_thread=new pthread_t;
    if(!rpc_server_thread){
        throw std::exception();
    }

    // files.push_back("bbb.txt");

    rpc_server=new RpcServer(ip,port,keeper_ip_,keeper_port_);

    rpc_server->Callee("MapReduceCenter",std::bind(&MapReduceMaster::MapReduceCenter,this,std::placeholders::_1));
}

MapReduceMaster::~MapReduceMaster()
{
    delete rpc_server_thread;
    delete rpc_server;
}

bool MapReduceMaster::MapReduce(const std::vector<std::string>& file_list, const int reducer_num, const int split_size)
{
    std::string method="Map";
    std::vector<std::string> parameters;

    for(int i=0;i<file_list.size();i++){
        files.push_back(file_list[i]);
        parameters.push_back(file_list[i]);
    }
    parameters.push_back(MapReduceUtil::IntToString(split_size));
    parameters.push_back(ip);
    parameters.push_back(port);

    for(int i=1;i<=reducer_num;i++){
        ReducerNode* reducer_node=new ReducerNode;
        reducer_node->files.push_back(file_list[0]);
        reducer_map.insert(std::make_pair(i,reducer_node));
    }

    RpcClient::Caller(method,parameters,keeepr_ip,keeper_port);
}

bool MapReduceMaster::StartReducer(const std::string& reducer_ip, const std::string& reducer_port)
{
    /*
    信息格式
        -1.文件总数
        -2.文件名列表
        -3.本机ip
        -4.本机port
    */
   
    std::string method_name="Register";
    std::vector<std::string> parameters;
    parameters.push_back(MapReduceUtil::IntToString(files.size()));
    for(int i=0;i<files.size();i++){
        parameters.push_back(files[i]);
    }
    // printf("master_ip is %s , master_port is %s\n",&ip[0],&port[0]);
    parameters.push_back(ip);
    parameters.push_back(port);

    std::vector<std::string> output=RpcClient::Call(method_name,parameters,reducer_ip,reducer_port);

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

bool MapReduceMaster::ConnReducer(const std::string& split_num, const std::string& aim_file_name, const std::string& split_file_name, const std::string& mapper_ip, const std::string& mapper_port, const std::string& reducer_ip, const std::string& reducer_port)
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
    std::string method_name="Reduce";
    std::vector<std::string> parameters;
    parameters.push_back(split_num);
    parameters.push_back(aim_file_name);
    parameters.push_back(split_file_name);
    parameters.push_back(mapper_ip);
    parameters.push_back(mapper_port);
    parameters.push_back(ip);
    parameters.push_back(port);
    RpcClient::Call(method_name,parameters,reducer_ip,reducer_port);

    return true;
}

std::vector<std::string> MapReduceMaster::MapReduceCenter(const std::vector<std::string>& message)
{
    /*
    信息格式:
    -身份信息:Mapper/Reducer
    */
    printf("this is MapReduceCenter!\n");

    std::vector<std::string> send_;
    if(message[0]=="Mapper"){
        send_.push_back(ProcessMapperMessage(message));
    }else if(message[0]=="Reducer"){
        send_.push_back(ProcessReducerMessage(message));
    }else throw std::exception();

    return send_;
}


std::string MapReduceMaster::ProcessMapperMessage(const std::vector<std::string>& message)
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
    const std::string& aim_file_name=message[1];
    const std::string& split_id=message[2];
    const std::string& message_type=message[3];

    if(message_type=="Process"){
        const std::string& process_percent=message[4];
        printf("Mapper : file %s split %s processing is %s !\n",&aim_file_name[0],&split_id[0],&process_percent[0]);
    }else if(message_type=="Start"){
        printf("Mapper : file %s split %s processing is starting !\n",&aim_file_name[0],&split_id[0]);
    }else if(message_type=="Finish"){
        printf("Mapper : file %s split %s processing is finish !\n",&aim_file_name[0],&split_id[0]);

        const std::string& mapper_ip=message[4];
        const std::string& mapper_port=message[5];
        const std::string& split_num=message[6];
        std::vector<std::string> shuffle_list;
        for(int i=7;i<message.size();i++){
            shuffle_list.push_back(std::move(message[i]));
        }
        if(reducer_num!=shuffle_list.size()){
            throw std::exception();
        }
        for(int i=1;i<=reducer_num;i++){
            const std::string& shuffle_name=shuffle_list[i-1];
            std::unordered_map<int,ReducerNode*>::iterator it=reducer_map.find(i);
            if(it==reducer_map.end()){
                throw std::exception();
            }

            if(!(it->second->is_ready.load())){
                //reducer未就绪
                pthread_mutex_lock(it->second->reducer_lock);
                if(!(it->second->is_ready.load())){
                    //再次确认
                    printf("Searching Reducer!\n");
                    RpcClient::CallerOne("Reduce",keeepr_ip,keeper_port,it->second->ip,it->second->port);//获取一个reducer
                    StartReducer(it->second->ip,it->second->port);//启动reducer与master的连接
                    it->second->is_ready.store(true);
                    pthread_mutex_unlock(it->second->reducer_lock);
                }else{
                    pthread_mutex_unlock(it->second->reducer_lock);
                }
            }
            ConnReducer(split_num,aim_file_name,shuffle_name,mapper_ip,mapper_port,it->second->ip,it->second->port);
        }
    }

    return "Receive";
}

std::string MapReduceMaster::ProcessReducerMessage(const std::vector<std::string>& message)
{    
    if(message[2]=="Process"){
        printf("reducer's processing is %s\n",&message[3][0]);
        return "";
    }

}

void MapReduceMaster::loop()
{
    pthread_create(rpc_server_thread,nullptr,[](void* argv)->void*{
        RpcServer* rpc_server=(RpcServer*)argv;
        rpc_server->loop();

        return nullptr;
    },this->rpc_server);
    pthread_detach(*rpc_server_thread);
}

bool MapReduceMaster::SetTaskFile(std::vector<std::string>& files_)
{
    files.clear();
    for(int i=0;i<files_.size();i++){
        files.push_back(files_[i]);
    }

    return true;
}