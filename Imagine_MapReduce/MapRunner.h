#ifndef IMAGINE_MAPREDUCE_MAPRUNNER_H
#define IMAGINE_MAPREDUCE_MAPRUNNER_H

#include<string.h>
#include<queue>

#include"RecordReader.h"
#include"Mapper.h"
#include"OutputFormat.h"
#include"KVBuffer.h"
#include"Callbacks.h"
#include"Partitioner.h"

namespace Imagine_MapReduce{

template<typename reader_key,typename reader_value,typename key,typename value>
class Mapper;

template<typename reader_key,typename reader_value,typename key,typename value>
class MapRunner
{

public:
    class SpillReader{
        
        public:
            SpillReader(std::string key_, std::string value_, int fd_):spill_key(key_),spill_value(value_),fd(fd_){}

        public:
            std::string spill_key;
            std::string spill_value;
            int fd;
    };

    class SpillReaderCmp{
        public:
            bool operator()(SpillReader* a, SpillReader* b)
            {
                return strcmp(&a->spill_key[0],&b->spill_key[0])<0?true:false;
            }
    };

public:

    // MapRunner(int split_id_, int split_num_, const std::string file_name_, int partition_num_=DEFAULT_PARTITION_NUM):split_id(split_id_),split_num(split_num_),file_name(file_name_),partition_num(partition_num_)
    // {
    //     buffer=new KVBuffer(partition_num,split_id_,spill_files);
    //     Init();
    // }

    MapRunner(int split_id_, int split_num_, const std::string file_name_, const std::string& mapper_ip_, const std::string& mapper_port_, const std::string& master_ip_, const std::string& master_port_, MAP map_, Partitioner<key>* partitioner_, OutputFormat<key,value>* output_format_, RpcServer* rpc_server_, int partition_num_=DEFAULT_PARTITION_NUM)
        :split_id(split_id_),split_num(split_num_),file_name(file_name_),mapper_ip(mapper_ip_),mapper_port(mapper_port_),master_ip(master_ip_),master_port(master_port_),map(map_),partitioner(partitioner_),output_format(output_format_),rpc_server(rpc_server_),partition_num(partition_num_)
    {
        buffer=new KVBuffer(partition_num,split_id_,spill_files);
        Init();
    }

    ~MapRunner()
    {
    }

    void Init()
    {        
        spilling_thread=new pthread_t;
        if(!spilling_thread){
            throw std::exception();
        }
    }

    bool SetIp(const std::string& ip_){master_ip=ip_;return true;}

    bool SetPort(const std::string& port_){master_port=port_;return true;}

    bool SetRecordReader(std::shared_ptr<RecordReader<reader_key,reader_value>> record_reader_){record_reader=record_reader_;return true;}

    bool SetMapFunction(MAP map_){map=map_;return true;}

    bool SetTimerCallback(MAPTIMER timer_callback_){timer_callback=timer_callback_;return true;}

    bool SetRpcServer(RpcServer* rpc_server_){rpc_server=rpc_server_;return true;}

    int GetSplitNum(){return split_num;}

    std::string GetFileName(){return file_name;}

    std::string GetMapperIp(){return mapper_ip;}

    std::string GetMapperPort(){return mapper_port;}

    std::string GetMasterIp(){return master_ip;}

    std::string GetMasterPort(){return master_port;}

    std::shared_ptr<RecordReader<reader_key,reader_value>> GetRecordReader(){return record_reader;}

    MAP GetMap(){return map;}

    MAPTIMER GetTimerCallback(){return timer_callback;}

    RpcServer* GetRpcServer(){return rpc_server;}

    OutputFormat<key,value>* GetOutPutFormat(){return output_format;}

    int GetId(){return split_id;}

    bool WriteToBuffer(const std::pair<key,value>& content)
    {
        std::pair<char*,char*> pair_=output_format->ToString(content);
        bool flag=buffer->WriteToBuffer(pair_,partitioner->Partition(content.first));
        delete [] pair_.first;
        delete [] pair_.second;
        return flag;
    }

    bool StartSpillingThread()
    {
        pthread_create(spilling_thread,nullptr,[](void* argv)->void*{
            KVBuffer* buffer=(KVBuffer*)argv;
            buffer->Spilling();

            delete buffer;

            return nullptr;
        },buffer);
        pthread_detach(*spilling_thread);

        return true;
    }

    bool CompleteMapping()
    {
        //溢写缓冲区全部内容
        buffer->SpillBuffer();//保证全部spill完毕
        Combine();
    }

    bool Combine()//合并spill文件到shuffle文件
    {
        /*
        Spill过程、Shuffle过程以及Merge过程中,对于用户要写的每一对key,value:
        -" "作为key和value的分隔符
        -"\r\n"作为每一对数据的分隔符
        */
        printf("Start Combining!\n");
        const int spill_num=spill_files[0].size();
        for(int i=0;i<partition_num;i++){
            std::string shuffle_file="split"+MapReduceUtil::IntToString(split_id)+"_shuffle_"+MapReduceUtil::IntToString(i+1)+".txt";
            shuffle_files.push_back(shuffle_file);
            int shuffle_fd=open(&shuffle_file[0],O_CREAT|O_RDWR,0777);
            int fds[spill_num];
            std::priority_queue<SpillReader*,std::vector<SpillReader*>,SpillReaderCmp> heap_;
            // printf("spill_num is %d\n",spill_num);
            for(int j=0;j<spill_num;j++){
                fds[j]=open(&spill_files[i][j][0],O_RDWR);
                std::string key_;
                std::string value_;
                if(SpillRead(fds[j],key_,value_))heap_.push(new SpillReader(key_,value_,fds[j]));
            }

            while(heap_.size()){
                SpillReader* next_=heap_.top();
                heap_.pop();
                std::string key_;
                std::string value_;
                if(SpillRead(next_->fd,key_,value_))heap_.push(new SpillReader(key_,value_,next_->fd));
                write(shuffle_fd,&next_->spill_key[0],next_->spill_key.size());
                char c=' ';
                write(shuffle_fd,&c,1);
                write(shuffle_fd,&next_->spill_value[0],next_->spill_value.size());
                char cc[]="\r\n";
                write(shuffle_fd,cc,2);

                delete next_;
            }

            for(int j=0;j<spill_num;j++){
                close(fds[j]);
                remove(&spill_files[i][j][0]);
            }
            close(shuffle_fd);
        }

        return true;
    }

    bool SpillRead(int fd, std::string& key_, std::string& value_)//从spill文件读取kv对
    {
        //假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
        bool flag=false;
        char c;
        int ret=read(fd,&c,1);
        if(!ret)return false;//文件读空
        
        key_.push_back(c);
        while(1){
            // printf("here\n");
            read(fd,&c,1);
            if(c==' ')break;
            key_.push_back(c);
        }
        while(1){
            read(fd,&c,1);
            value_.push_back(c);
            if(c=='\n'&&flag){
                value_.pop_back();
                value_.pop_back();
                return true;
            }
            if(c=='\r')flag=true;
            else flag=false;
        }
    }

    std::vector<std::string>& GetShuffleFile(){return shuffle_files;}

private:

    const int split_id;//split的id
    const int split_num;
    const int partition_num;//分区数(shuffle数目)
    const std::string file_name;//源文件名

    const std::string master_ip;//master的ip
    const std::string master_port;//master的port
    const std::string mapper_ip;//mapper的ip
    const std::string mapper_port;//mapper的port

    std::shared_ptr<RecordReader<reader_key,reader_value>> record_reader;
    OutputFormat<key,value>* output_format;
    Partitioner<key>* partitioner;
    MAP map;
    MAPTIMER timer_callback;

    RpcServer* rpc_server;

    //缓冲区
    KVBuffer* buffer;
    pthread_t* spilling_thread;

    std::vector<std::vector<std::string>> spill_files;
    std::vector<std::string> shuffle_files;
    
};

}





#endif