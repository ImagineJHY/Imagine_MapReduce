#ifndef IMAGINE_MAPREDUCE_MAPPER_H
#define IMAGINE_MAPREDUCE_MAPPER_H

#include<memory.h>

#include"Imagine_Rpc/Imagine_Rpc/RpcServer.h"
#include"MapReduceUtil.h"
#include"RecordReader.h"
#include"LineRecordReader.h"
#include"MapRunner.h"
#include"Imagine_Rpc/Imagine_Rpc/RpcClient.h"
#include"OutputFormat.h"
#include"TextOutputFormat.h"
#include"Callbacks.h"
#include"Partitioner.h"
#include"StringPartitioner.h"

namespace Imagine_MapReduce{

template<typename reader_key,typename reader_value,typename key,typename value>
class Mapper
{

public:

    Mapper(const std::string& ip_, const std::string& port_, RecordReader<reader_key,reader_value>* record_reader_=nullptr, MAP map_=nullptr, Partitioner<key>* partitioner_=nullptr, OutputFormat<key,value>* output_format_=nullptr, MAPTIMER timer_callback_=nullptr, const std::string& keeper_ip_="", const std::string& keeper_port_="");

    ~Mapper();

    //Rpc通信调用
    std::vector<std::string> Map(const std::vector<std::string>& input);

    std::vector<std::string> GetFile(const std::vector<std::string>& input);

    bool SetDefaultRecordReader();

    bool SetDefaultMapFunction();

    bool SetDefaultTimerCallback();

    bool SetDefaultOutputFormat();

    bool SetDefaultPartitioner();

    static void DefaultTimerCallback(int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader);

    void loop();

private:

    const std::string ip;
    const std::string port;

    const std::string keeper_ip;
    const std::string keeper_port;

    MAP map_;//提供给用户自定义的map函数
    MAPTIMER timer_callback;
    
    //MapReduceUtil::RecordReader record_reader;//提供给用户自定义的recordread函数
    RecordReader<reader_key,reader_value>* record_reader_class;//split迭代器类型
    OutputFormat<key,value>* output_format;
    std::vector<InputSplit*> splits;

    pthread_t* map_threads;


    RpcServer* rpc_server;
    pthread_t* rpc_server_thread;

    Partitioner<key>* partitioner;
};

template<typename reader_key,typename reader_value,typename key,typename value>
Mapper<reader_key,reader_value,key,value>::Mapper(const std::string& ip_, const std::string& port_, RecordReader<reader_key,reader_value>* record_reader_, MAP map_, Partitioner<key>* partitioner_, OutputFormat<key,value>* output_format_, MAPTIMER timer_callback_, const std::string& keeper_ip_, const std::string& keeper_port_):
    ip(ip_),port(port_),keeper_ip(keeper_ip_),keeper_port(keeper_port_)
{
    rpc_server_thread=new pthread_t;
    if(!rpc_server_thread){
        throw std::exception();
    }

    if(record_reader_)record_reader_class=record_reader_;
    else record_reader_class=new LineRecordReader();

    if(map_)map_=map_;
    else SetDefaultMapFunction();

    if(output_format_)output_format=output_format_;
    else SetDefaultOutputFormat();

    if(timer_callback_)timer_callback=timer_callback_;
    else SetDefaultTimerCallback();

    if(partitioner_)partitioner=partitioner_;
    else SetDefaultPartitioner();

    rpc_server=new RpcServer(ip,port,keeper_ip,keeper_port);

    rpc_server->Callee("Map",std::bind(&Mapper::Map,this,std::placeholders::_1));
    rpc_server->Callee("GetFile",std::bind(&Mapper::GetFile,this,std::placeholders::_1));
}

template<typename reader_key,typename reader_value,typename key,typename value>
Mapper<reader_key,reader_value,key,value>::~Mapper()
{
    delete rpc_server;
    delete rpc_server_thread;
    delete [] map_threads;

    delete record_reader_class;
    delete output_format;
    delete partitioner;
}

template<typename reader_key,typename reader_value,typename key,typename value>
std::vector<std::string> Mapper<reader_key,reader_value,key,value>::Map(const std::vector<std::string>& input)
{
    /*
    数据格式：
        1.目标文件名(路径)
        2.split大小
        3.MapReduceMaster的ip
        4.MapReduceMaster的port
        5.目标文件ip(若在本地则省略)
        6.目标文件port(若在本地则省略)
    注:对于split的跨行问题:到达split时多读一行(多读一个完整的\r\n进来),并且让除第一块意外的每个Mapper都跳过第一行数据(split起始位置为0不跳行,反之跳行)
    */

    //获取split数据
    std::vector<InputSplit*> splits=MapReduceUtil::DefaultReadSplitFunction(input[0],MapReduceUtil::StringToInt(input[1]));//传入目标文件名和split大小
    // map(MapReduceUtil::DefaultReadSplitFunction(input[2]));
    //将数据按要求转换成kv数据
    // std::unordered_map<std::string,std::string> kv_map=record_reader(splits);
    map_threads=new pthread_t[splits.size()];
    for(int i=0;i<splits.size();i++){
        std::shared_ptr<RecordReader<reader_key,reader_value>> new_record_reader=record_reader_class->CreateRecordReader(splits[i],i+1);
        // new_record_reader->SetOutputFileName("split"+MapReduceUtil::IntToString(i+1)+".txt");
        MapRunner<reader_key,reader_value,key,value>* runner=new MapRunner<reader_key,reader_value,key,value>(i+1,splits.size(),input[0],ip,port,input[2],input[3],map_,partitioner,output_format,rpc_server);
        runner->SetRecordReader(new_record_reader);
        runner->SetTimerCallback(timer_callback);
        pthread_create(map_threads+i,nullptr,[](void* argv)->void*{

            MapRunner<reader_key,reader_value,key,value>* runner=(MapRunner<reader_key,reader_value,key,value>*)argv;
            MAP map_=runner->GetMap();
            std::shared_ptr<RecordReader<reader_key,reader_value>> reader=runner->GetRecordReader();
            OutputFormat<key,value>* output_format=runner->GetOutPutFormat();

            //连接Master
            int sockfd;
            long long timerfd;
            if(RpcClient::ConnectServer(runner->GetMasterIp(),runner->GetMasterPort(),&sockfd)){
                std::vector<std::string> parameters;
                parameters.push_back("Mapper");
                parameters.push_back(runner->GetFileName());
                parameters.push_back(MapReduceUtil::IntToString(runner->GetId()));
                parameters.push_back("Start");
                if(RpcClient::Call("MapReduceCenter",parameters,&sockfd)[0]=="Receive"){
                    timerfd=runner->GetRpcServer()->SetTimer(2.0,0.0,std::bind(runner->GetTimerCallback(),sockfd,reader));
                }else{
                    throw std::exception();
                }
            }

            runner->StartSpillingThread();
            sleep(1);
            while(reader->NextKeyValue()){
                
                runner->WriteToBuffer(map_(reader->GetCurrentKey(),reader->GetCurrentValue()));
            }

            runner->CompleteMapping();//buffer在spill线程中销毁

            runner->GetRpcServer()->RemoveTimer(timerfd);
            std::vector<std::string> parameters;
            parameters.push_back("Mapper");
            parameters.push_back(runner->GetFileName());
            parameters.push_back(MapReduceUtil::IntToString(runner->GetId()));
            parameters.push_back("Finish");
            parameters.push_back(runner->GetMapperIp());
            parameters.push_back(runner->GetMapperPort());
            parameters.push_back(MapReduceUtil::IntToString(runner->GetSplitNum()));
            std::vector<std::string>& shuffle=runner->GetShuffleFile();
            parameters.insert(parameters.end(),shuffle.begin(),shuffle.end());
            //printf("%s : working is finish!\n",&split_file_name[0]);
            RpcClient::Call("MapReduceCenter",parameters,&sockfd);

            delete runner;

            close(sockfd);
            //close(fd);

            return nullptr;

        },runner);
        pthread_detach(*(map_threads+i));
    }

    return Rpc::Deserialize("");
}

template<typename reader_key,typename reader_value,typename key,typename value>
std::vector<std::string> Mapper<reader_key,reader_value,key,value>::GetFile(const std::vector<std::string>& input)
{
    std::vector<std::string> output;
    std::string content;
    printf("get file %s\n",&input[0][0]);
    int fd=open(&input[0][0],O_RDWR);
    while(1){
        char buffer[1024];
        int ret=read(fd,buffer,1024);
        for(int i=0;i<ret;i++){
            content.push_back(buffer[i]);
        }
        if(ret!=1024)break;
    }
    close(fd);
    if(content.size())output.push_back(content);

    return output;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultRecordReader()
{
    record_reader_class=new LineRecordReader();
    return true;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultMapFunction()
{
    // map=DefaultMapFunction;
    map_=[](int offset,const std::string& line_text)->std::pair<std::string,int>{
        return std::make_pair(line_text,1);
    };

    return true;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultTimerCallback()
{
    timer_callback=DefaultTimerCallback;
    return true;
    // timer_callback=[](int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader){
    //     printf("this is timer callback!\n");
    //     if(reader.use_count()==1){
    //     printf("mapper已完成!\n");
    //     return;
    //     }
    //     //printf("sockfd is %d\n",sockfd);
    //     std::string method="MapReduceCenter";
    //     std::vector<std::string> parameters;
    //     parameters.push_back("Mapper");
    //     parameters.push_back(reader->GetOutputFileName());
    //     parameters.push_back("Process");
    //     parameters.push_back(MapReduceUtil::DoubleToString(reader->GetProgress()));

    //     std::vector<std::string> recv_=RpcClient::Call(method,parameters,&sockfd);
    //     if(recv_.size()&&recv_[0]=="Receive"){
    //         printf("connect ok!\n");
    //     }else{
    //         printf("connect error!\n");
    //     }
    // }
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultPartitioner()
{
    partitioner=new StringPartitioner();
    return true;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultOutputFormat()
{
    output_format=new TextOutputFormat();
}

template<typename reader_key,typename reader_value,typename key,typename value>
void Mapper<reader_key,reader_value,key,value>::loop()
{
    pthread_create(rpc_server_thread,nullptr,[](void* argv)->void*{
        RpcServer* rpc_server=(RpcServer*)argv;
        rpc_server->loop();
    },this->rpc_server);
    pthread_detach(*rpc_server_thread);
}

template<typename reader_key,typename reader_value,typename key,typename value>
void Mapper<reader_key,reader_value,key,value>::DefaultTimerCallback(int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader)
{
    // printf("this is timer callback!\n");
    if(reader.use_count()==1){
        printf("This Mapper Task Over!\n");
        return;
    }
    //printf("sockfd is %d\n",sockfd);
    std::string method="MapReduceCenter";
    std::vector<std::string> parameters;
    parameters.push_back("Mapper");
    parameters.push_back(reader->GetFileName());
    parameters.push_back(MapReduceUtil::IntToString(reader->GetSplitId()));
    parameters.push_back("Process");
    parameters.push_back(MapReduceUtil::DoubleToString(reader->GetProgress()));

    std::vector<std::string> recv_=RpcClient::Call(method,parameters,&sockfd);
    if(recv_.size()&&recv_[0]=="Receive"){
        // printf("connect ok!\n");
    }else{
        printf("connect error!\n");
    }
}

}



#endif