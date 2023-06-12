#ifndef IMAGINE_MAPREDUCE_KVBUFFER_H
#define IMAGINE_MAPREDUCE_KVBUFFER_H

#include<fcntl.h>

#include"MapReduceUtil.h"
#include"Callbacks.h"

namespace Imagine_MapReduce{


class KVBuffer
{
public:
    class MetaIndex{
        public:
            MetaIndex(int key_idx_, int value_idx_, int key_len_, int value_len_, int partition_idx_, char* buffer_, int border_)
            :key_idx(key_idx_),value_idx(value_idx_),key_len(key_len_),value_len(value_len_),partition_idx(partition_idx_),buffer(buffer_),border(border_)
            {
            }

            ~MetaIndex(){}

            char* GetKey(char* key)const
            {
                if(key_idx+key_len-1>=border){
                    memcpy(key,buffer+key_idx,border-key_idx);
                    memcpy(key+border-key_idx,buffer,key_len-(border-key_idx));
                }else{
                    memcpy(key,buffer+key_idx,key_len);
                }
                return key;
            }

            int GetKeyLen()const
            {
                return key_len;
            }

            char* GetValue(char* value)const
            {
                if(value_idx+value_len-1>=border){
                    memcpy(value,buffer+value_idx,border-value_idx);
                    memcpy(value+border-value_idx,buffer,value_len-(border-value_idx));
                }else{
                    memcpy(value,buffer+value_idx,value_len);
                }
                return value;
            }

            int GetValueLen()const
            {
                return value_len;
            }

            int GetPartition()const
            {
                return partition_idx;
            }

            static MetaIndex GetMetaIndex(char* buffer_, int meta_idx_, int border_)
            {
                char meta_info[DEFAULT_META_SIZE];
                if(meta_idx_+DEFAULT_META_SIZE>border_){
                    //分两次读
                    memcpy(meta_info,buffer_+meta_idx_,border_-meta_idx_);
                    memcpy(meta_info+border_-meta_idx_,buffer_,DEFAULT_META_SIZE-(border_-meta_idx_));
                }else{
                    memcpy(meta_info,buffer_+meta_idx_,DEFAULT_META_SIZE);
                }
                int key_idx;
                memcpy(&key_idx,meta_info,sizeof(key_idx));
                int value_idx;
                memcpy(&value_idx,meta_info+sizeof(key_idx),sizeof(value_idx));
                int key_len=key_idx<value_idx?value_idx-key_idx:border_-key_idx+value_idx;
                int value_len;
                memcpy(&value_len,meta_info+sizeof(key_idx)+sizeof(value_idx),sizeof(value_len));
                int partition_idx;
                memcpy(&partition_idx,meta_info+sizeof(key_idx)+sizeof(value_idx)+sizeof(value_len),sizeof(partition_idx));
                // printf("key idx is %d, value idx is %d\n",key_idx,value_idx);
                // printf("key len is %d, value len is %d。。。。。。。。。。。。。。。。。\n",key_len,value_len);
                return MetaIndex(key_idx,value_idx,key_len,value_len,partition_idx,buffer_,border_);
            }

        private:

            int key_idx;
            int value_idx;
            int key_len;
            int value_len;
            int partition_idx;

            char* buffer;
            int border;
    };

    class MetaCmp{
        public:
            bool operator()(const MetaIndex& a, const MetaIndex& b)
            {
                char* key_a=new char[a.GetKeyLen()+1];
                char* key_b=new char[b.GetKeyLen()+1];
                a.GetKey(key_a);
                b.GetKey(key_b);
                key_a[a.GetKeyLen()]='\0';
                key_b[b.GetKeyLen()]='\0';
                
                bool flag;
                flag=strcmp(key_a,key_b)<0?true:false;

                delete [] key_a;
                delete [] key_b;

                return flag;
            }
    };

public:

    KVBuffer(int partition_num_, int split_id_, std::vector<std::vector<std::string>>& spill_files_)
        :buffer_size(DEFAULT_SPLIT_BUFFER_SIZE),spill_size(DEFAULT_SPILL_SIZE),partition_num(partition_num_),split_id(split_id_),spill_files(spill_files_)
    {

        buffer_lock=new pthread_mutex_t;
        if(pthread_mutex_init(buffer_lock,nullptr)!=0){
            throw std::exception();
        }

        spill_lock=new pthread_mutex_t;
        if(pthread_mutex_init(spill_lock,nullptr)!=0){
            throw std::exception();
        }

        spill_cond=new pthread_cond_t;
        if(pthread_cond_init(spill_cond,nullptr)!=0){
            throw std::exception();
        }

        buffer=new char[buffer_size];
        // equator=kv_idx=kv_border=meta_border=0;
        // meta_idx=buffer_size-1;
        meta_idx=equator=kv_border=meta_border=0;
        kv_idx=1;
        is_spilling=false;

        spill_id=1;
        spill_buffer=false;
        quit=false;

        spill_files.resize(partition_num);
    }

    ~KVBuffer()
    {
        printf("delete buffer\n");
        delete [] buffer;
        delete buffer_lock;
        delete spill_lock;
        delete spill_cond;
    }

    bool WriteToBuffer(const std::pair<char*,char*>& content, int partition_idx);

    bool Spilling();

    bool WriteJudgementWithSpilling(const std::pair<char*,char*>& content)
    {
        if((kv_idx<=kv_border?kv_border-kv_idx:buffer_size-kv_idx+kv_border)<strlen((char*)(content.first))+strlen((char*)(content.second))||(meta_border<=meta_idx?meta_idx-meta_border:meta_idx+buffer_size-meta_border)<DEFAULT_META_SIZE){
            return false;
        }

        return true;
    }

    bool WriteJudgementWithoutSpilling(const std::pair<char*,char*>& content)
    {
        if(meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1>strlen((char*)(content.first))+strlen((char*)(content.second))+DEFAULT_META_SIZE){
            return true;
        }

        return false;
    }

    bool SpillBuffer()
    {
        spill_buffer=true;
        while(!quit){
            //pthread_cond_signal(spill_cond);
            pthread_cond_broadcast(spill_cond);
        }

        return true;
    }


private:

    const int partition_num;//分区数
    const int split_id;
    int spill_id;//自增字段(第几次spilling)

    char* buffer;
    const int buffer_size;
    int equator;
    int kv_idx;//kv指针的位置
    int meta_idx;//索引指针的位置
    bool is_spilling;//是否在溢写
    int kv_border;//溢写时的kv边界
    int meta_border;//溢写时的索引边界

    const double spill_size;//设置发生spill空闲空间比

    pthread_mutex_t* buffer_lock;

    pthread_cond_t* spill_cond;
    pthread_mutex_t* spill_lock;

    std::vector<std::vector<std::string>>& spill_files;

    bool spill_buffer;
    bool quit;
};

}




#endif