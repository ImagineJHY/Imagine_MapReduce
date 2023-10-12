#include"KVBuffer.h"

using namespace Imagine_MapReduce;

bool KVBuffer::WriteToBuffer(const std::pair<char*,char*>& content, int partition_idx)
{
    bool flag=true;
    int key_len_=strlen(content.first);
    int value_len_=strlen(content.second);
    int key_point_;
    int value_point_;
    char* key_=content.first;
    char* value_=content.second;
    char meta_info_[DEFAULT_META_SIZE];
    while(flag){
        //逻辑上保证kv_idx!=meta_idx恒成立,即永远不允许写满
        //kv_idx始终向上增长,meta_idx始终向下增长
        // printf("key is %d,value is %d!!!!!!!!!!!!!!!!!!!!!!!\n",key_len,value_len);
        pthread_mutex_lock(buffer_lock);
        if(is_spilling?WriteJudgementWithSpilling(content):WriteJudgementWithoutSpilling(content)){
            //空间足够写入
            flag=false;
            key_point_=kv_idx;
            memcpy(meta_info_,&key_point_,sizeof(key_point_));
            memcpy(meta_info_+sizeof(key_point_)+sizeof(value_point_),&value_len_,sizeof(value_len_));
            memcpy(meta_info_+sizeof(key_point_)+sizeof(value_point_)+sizeof(value_len_),&partition_idx,sizeof(partition_idx));
            if(meta_idx<kv_idx&&(buffer_size-kv_idx<key_len_+value_len_||meta_idx+1<DEFAULT_META_SIZE)){
                //kv或meta不能一次写完
                if(buffer_size-kv_idx<key_len_+value_len_){
                    //两次写入kv,一次写完meta
                    if(buffer_size-kv_idx>=key_len_){
                        //能完整写入key
                        memcpy(buffer+kv_idx,key_,key_len_);
                        kv_idx=(kv_idx+key_len_)%buffer_size;

                        value_point_=(kv_idx+key_len_)%buffer_size;
                        memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));

                        memcpy(buffer+kv_idx,value_,buffer_size-kv_idx);
                        memcpy(buffer,value_+buffer_size-kv_idx,value_len_-(buffer_size-kv_idx));
                        kv_idx=value_len_-(buffer_size-kv_idx);
                    }else{
                        //不能完整写入key
                        memcpy(buffer+kv_idx,key_,buffer_size-kv_idx);
                        memcpy(buffer,key_+buffer_size-kv_idx,key_len_-(buffer_size-kv_idx));
                        kv_idx=key_len_-(buffer_size-kv_idx);

                        value_point_=kv_idx;
                        memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));

                        memcpy(buffer+kv_idx,value_,value_len_);
                        kv_idx+=value_len_;
                    }
                    //写meta
                    memcpy(buffer+meta_idx-DEFAULT_META_SIZE+1,meta_info_,strlen(meta_info_));
                    meta_idx-=DEFAULT_META_SIZE;
                    if(meta_idx+1==kv_idx)meta_idx=kv_idx;//写满
                }else if(meta_idx+1<DEFAULT_META_SIZE){
                    //两次写入meta,一次写完kv
                    memcpy(buffer+kv_idx,key_,key_len_);
                    kv_idx+=key_len_;

                    value_point_=kv_idx;
                    memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));

                    memcpy(buffer+kv_idx,value_,value_len_);
                    kv_idx+=value_len_;
                    //写meta
                    memcpy(buffer,meta_info_+DEFAULT_META_SIZE-(meta_idx+1),meta_idx+1);
                    memcpy(buffer+buffer_size-DEFAULT_META_SIZE+(meta_idx+1),meta_info_,DEFAULT_META_SIZE-(meta_idx+1));
                    meta_idx=buffer_size-(DEFAULT_META_SIZE-(meta_idx+1))-1;
                    if(meta_idx+1==kv_idx)meta_idx=kv_idx;//写满
                }
            }else{
                memcpy(buffer+kv_idx,key_,key_len_);
                kv_idx+=key_len_;

                //printf("valuepoint is %d\n",kv_idx);
                value_point_=kv_idx;
                memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));

                memcpy(buffer+kv_idx,value_,value_len_);
                kv_idx=(kv_idx+value_len_)%buffer_size;

                memcpy(buffer+meta_idx-DEFAULT_META_SIZE+1,meta_info_,DEFAULT_META_SIZE);
                meta_idx-=DEFAULT_META_SIZE;
                if(meta_idx==-1){
                    if(kv_idx==0)meta_idx=0;
                    else meta_idx=buffer_size-1;
                }else if(meta_idx<kv_idx)meta_idx=kv_idx;//已满
            }
        }else{
            //主动休眠
        }
        //printf("content is key-%s,value-%s\n",content.first,content.second);
        pthread_mutex_unlock(buffer_lock);
        if((meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1)*1.0/buffer_size<=spill_size){
            pthread_cond_signal(spill_cond);
        }
    }

    return true;
}

bool KVBuffer::Spilling()
{
    while(!spill_buffer||(meta_idx<kv_idx?kv_idx-meta_idx:buffer_size-(meta_idx-kv_idx))!=1){
        pthread_mutex_lock(spill_lock);

        pthread_mutex_lock(buffer_lock);
        //Spill结束,保证spilling在临界区改变值
        is_spilling=false;
        pthread_mutex_unlock(buffer_lock);

        while((!spill_buffer)&&(meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1)*1.0/buffer_size>spill_size){
            pthread_cond_wait(spill_cond,spill_lock);
        }
        //开始spill
        pthread_mutex_unlock(spill_lock);
        pthread_mutex_lock(buffer_lock);
        is_spilling=true;
        int old_equator=equator;
        int old_kv_idx=kv_idx;
        int old_meta_idx=meta_idx%buffer_size;
        equator=((meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1)/2+kv_idx)%buffer_size;
        kv_border=(meta_idx+1)%buffer_size;
        meta_border=kv_idx-1;
        if(meta_border<0)meta_border=buffer_size-1;
        kv_idx=(equator+1)%buffer_size;
        meta_idx=equator;
        pthread_mutex_unlock(buffer_lock);
        std::priority_queue<MetaIndex,std::vector<MetaIndex>,MetaCmp> meta_queue;
        while((old_meta_idx<=old_equator?old_equator-old_meta_idx:buffer_size-old_meta_idx+old_equator)>=DEFAULT_META_SIZE){
            // printf("buffer_size is %d,meta_idx is %d,equator is %d\n",buffer_size,old_meta_idx,old_equator);
            meta_queue.push(MetaIndex::GetMetaIndex(buffer,(old_meta_idx+1)%buffer_size,buffer_size));
            old_meta_idx=(old_meta_idx+DEFAULT_META_SIZE)%buffer_size;
        }

        // std::string file_name="split_"+MapReduceUtil::IntToString(split_id)+"_spill_"+MapReduceUtil::IntToString(spill_id)+"_shuffle_";
        std::string file_name="spill_"+MapReduceUtil::IntToString(spill_id)+"_split_"+MapReduceUtil::IntToString(split_id)+"_shuffle_";
        int fds[partition_num];
        for(int i=0;i<partition_num;i++){
            spill_files[i].push_back(file_name+MapReduceUtil::IntToString(i+1)+".txt");
            fds[i]=open(&(spill_files[i].back()[0]),O_CREAT|O_RDWR,0777);
        }

        while(meta_queue.size()){
            MetaIndex top_meta=meta_queue.top();
            meta_queue.pop();
            int key_len=top_meta.GetKeyLen();
            char* key=new char[key_len];
            int value_len=top_meta.GetValueLen();
            char* value=new char[value_len];
            top_meta.GetKey(key);
            top_meta.GetValue(value);

            int fd=fds[top_meta.GetPartition()-1];
            write(fd,key,key_len);
            char c=' ';
            write(fd,&c,1);
            write(fd,value,value_len);
            char cc[]="\r\n";
            write(fd,cc,2);

            delete [] key;
            delete [] value;
        }
        // spill_file_name.push_back(file_name);//装入临时文件名
        spill_id++;

        for(int i=0;i<partition_num;i++)close(fds[i]);
        //尝试唤醒
    }
    quit=true;
}