#ifndef IMAGINE_MAPREDUCE_KVBUFFER_H
#define IMAGINE_MAPREDUCE_KVBUFFER_H

#include <fcntl.h>

#include "MapReduceUtil.h"
#include "Callbacks.h"

namespace Imagine_MapReduce
{

class KVBuffer
{
 public:
    class MetaIndex
    {
     public:
        MetaIndex(int key_idx, int value_idx, int key_len, int value_len, int partition_idx, char *buffer, int border)
            : key_idx_(key_idx), value_idx_(value_idx), key_len_(key_len), value_len_(value_len), partition_idx_(partition_idx), buffer_(buffer), border_(border) {}

        ~MetaIndex() {}

        char *GetKey(char *key) const
        {
            if (key_idx_ + key_len_ - 1 >= border_) {
                memcpy(key, buffer_ + key_idx_, border_ - key_idx_);
                memcpy(key + border_ - key_idx_, buffer_, key_len_ - (border_ - key_idx_));
            } else {
                memcpy(key, buffer_ + key_idx_, key_len_);
            }

            return key;
        }

        int GetKeyLen() const
        {
            return key_len_;
        }

        char *GetValue(char *value) const
        {
            if (value_idx_ + value_len_ - 1 >= border_) {
                memcpy(value, buffer_ + value_idx_, border_ - value_idx_);
                memcpy(value + border_ - value_idx_, buffer_, value_len_ - (border_ - value_idx_));
            } else {
                memcpy(value, buffer_ + value_idx_, value_len_);
            }

            return value;
        }

        int GetValueLen() const
        {
            return value_len_;
        }

        int GetPartition() const
        {
            return partition_idx_;
        }

        static MetaIndex GetMetaIndex(char *buffer, int meta_idx, int border)
        {
            char meta_info[DEFAULT_META_SIZE];
            if (meta_idx + DEFAULT_META_SIZE > border) {
                // 分两次读
                memcpy(meta_info, buffer + meta_idx, border - meta_idx);
                memcpy(meta_info + border - meta_idx, buffer, DEFAULT_META_SIZE - (border - meta_idx));
            } else {
                memcpy(meta_info, buffer + meta_idx, DEFAULT_META_SIZE);
            }
            int key_idx;
            memcpy(&key_idx, meta_info, sizeof(key_idx));
            int value_idx;
            memcpy(&value_idx, meta_info + sizeof(key_idx), sizeof(value_idx));
            int key_len = key_idx < value_idx ? value_idx - key_idx : border - key_idx + value_idx;
            int value_len;
            memcpy(&value_len, meta_info + sizeof(key_idx) + sizeof(value_idx), sizeof(value_len));
            int partition_idx;
            memcpy(&partition_idx, meta_info + sizeof(key_idx) + sizeof(value_idx) + sizeof(value_len), sizeof(partition_idx));
            // printf("key idx is %d, value idx is %d\n",key_idx,value_idx);
            // printf("key len is %d, value len is %d。。。。。。。。。。。。。。。。。\n",key_len,value_len);
            return MetaIndex(key_idx, value_idx, key_len, value_len, partition_idx, buffer, border);
        }

     private:
        int key_idx_;
        int value_idx_;
        int key_len_;
        int value_len_;
        int partition_idx_;

        char *buffer_;
        int border_;
    };

    class MetaCmp
    {
     public:
        bool operator()(const MetaIndex &a, const MetaIndex &b)
        {
            char *key_a = new char[a.GetKeyLen() + 1];
            char *key_b = new char[b.GetKeyLen() + 1];
            a.GetKey(key_a);
            b.GetKey(key_b);
            key_a[a.GetKeyLen()] = '\0';
            key_b[b.GetKeyLen()] = '\0';

            bool flag;
            flag = strcmp(key_a, key_b) < 0 ? true : false;

            delete[] key_a;
            delete[] key_b;

            return flag;
        }
    };

 public:
    KVBuffer(int partition_num, int split_id, std::vector<std::vector<std::string>> &spill_files)
        : partition_num_(partition_num), split_id_(split_id), buffer_size_(DEFAULT_SPLIT_BUFFER_SIZE), spill_size_(DEFAULT_SPILL_SIZE), spill_files_(spill_files)
    {
        buffer_lock_ = new pthread_mutex_t;
        if (pthread_mutex_init(buffer_lock_, nullptr) != 0) {
            throw std::exception();
        }

        spill_lock_ = new pthread_mutex_t;
        if (pthread_mutex_init(spill_lock_, nullptr) != 0) {
            throw std::exception();
        }

        spill_cond_ = new pthread_cond_t;
        if (pthread_cond_init(spill_cond_, nullptr) != 0) {
            throw std::exception();
        }

        buffer_ = new char[buffer_size_];
        // equator=kv_idx=kv_border=meta_border=0;
        // meta_idx=buffer_size-1;
        meta_idx_ = equator_ = kv_border_ = meta_border_ = 0;
        kv_idx_ = 1;
        is_spilling_ = false;

        spill_id_ = 1;
        spill_buffer_ = false;
        quit_ = new bool(false);
        first_spilling_ = false;
        delete_this_ = false;

        spill_files_.resize(partition_num_);
    }

    ~KVBuffer()
    {
        printf("delete buffer\n");
        delete[] buffer_;
        delete buffer_lock_;
        delete spill_lock_;
        delete spill_cond_;
    }

    bool WriteToBuffer(const std::pair<char *, char *> &content, int partition_idx);

    bool Spilling();

    bool WriteJudgementWithSpilling(const std::pair<char *, char *> &content)
    {
        if (static_cast<size_t>(kv_idx_ <= kv_border_ ? kv_border_ - kv_idx_ : buffer_size_ - kv_idx_ + kv_border_) < strlen((char *)(content.first)) + strlen((char *)(content.second)) || 
            (meta_border_ <= meta_idx_ ? meta_idx_ - meta_border_ : meta_idx_ + buffer_size_ - meta_border_) < DEFAULT_META_SIZE) {
            return false;
        }

        return true;
    }

    bool WriteJudgementWithoutSpilling(const std::pair<char *, char *> &content)
    {
        if (static_cast<size_t>(meta_idx_ < kv_idx_ ? buffer_size_ - (kv_idx_ - meta_idx_ - 1) : meta_idx_ - kv_idx_ + 1) > strlen((char *)(content.first)) + strlen((char *)(content.second)) + DEFAULT_META_SIZE) {
            return true;
        }

        return false;
    }

    bool SpillBuffer()
    {
        spill_buffer_ = true;
        while (!(*quit_)) {
            // pthread_cond_signal(spill_cond);
            pthread_cond_broadcast(spill_cond_);
        }
        delete quit_;
        delete_this_ = true;

        return true;
    }

    bool IsDeleteConditionSatisfy()
    {
        return delete_this_;
    }

 private:
    const int partition_num_;            // 分区数
    const int split_id_;
    int spill_id_;                       // 自增字段(第几次spilling)

    char *buffer_;
    const int buffer_size_;
    int equator_;
    int kv_idx_;                         // kv指针的位置
    int meta_idx_;                       // 索引指针的位置
    bool is_spilling_;                   // 是否在溢写
    int kv_border_;                      // 溢写时的kv边界
    int meta_border_;                    // 溢写时的索引边界

    const double spill_size_;            // 设置发生spill空闲空间比

    pthread_mutex_t *buffer_lock_;

    pthread_cond_t *spill_cond_;
    pthread_mutex_t *spill_lock_;

    std::vector<std::vector<std::string>> &spill_files_;

    bool spill_buffer_;
    bool* quit_;
    bool first_spilling_;
    public: bool delete_this_;
};

} // namespace Imagine_MapReduce

#endif