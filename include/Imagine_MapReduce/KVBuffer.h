#ifndef IMAGINE_MAPREDUCE_KVBUFFER_H
#define IMAGINE_MAPREDUCE_KVBUFFER_H

#include "common_macro.h"

#include <queue>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

namespace Imagine_MapReduce
{

class KVBuffer
{
 public:
    class MetaIndex
    {
     public:
        MetaIndex(int key_idx, int value_idx, int key_len, int value_len, int partition_idx, char *buffer, int border)
            : key_idx_(key_idx), value_idx_(value_idx), key_len_(key_len), value_len_(value_len), partition_idx_(partition_idx), buffer_(buffer), border_(border)
        {
        }

        ~MetaIndex()
        {
        }

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
    KVBuffer(int partition_num, int split_id, std::vector<std::vector<std::string>> &spill_files);

    ~KVBuffer();

    bool WriteToBuffer(const std::pair<char *, char *> &content, int partition_idx);

    bool Spilling();

    bool WriteJudgementWithSpilling(const std::pair<char *, char *> &content) const;

    bool WriteJudgementWithoutSpilling(const std::pair<char *, char *> &content) const;

    bool SpillBuffer();

    bool IsDeleteConditionSatisfy() const;

 private:
    void Spill();

 private:
    const int partition_num_;                                   // 分区数
    const int split_id_;                                        // splitId
    int spill_id_;                                              // 自增字段(第几次spilling)
    char *buffer_;                                              // 缓冲区
    const int buffer_size_;                                     // 缓冲区大小
    int equator_;                                               // 赤道位置
    int kv_idx_;                                                // kv指针的位置
    int meta_idx_;                                              // 索引指针的位置
    bool is_spilling_;                                          // 是否在溢写
    int kv_border_;                                             // 溢写时的kv边界
    int meta_border_;                                           // 溢写时的索引边界
    const double spill_size_;                                   // 设置发生spill空闲空间比
    pthread_mutex_t *buffer_lock_;                              // 缓冲区的锁
    pthread_cond_t *spill_cond_;                                // 溢写条件变量
    pthread_mutex_t *spill_lock_;                               // 溢写条件变量的锁
    std::vector<std::vector<std::string>> &spill_files_;        // spill的文件, 也是作为传出参数
    bool spill_buffer_;                                         // 是否溢写的标识符
    bool* quit_;                                                // 是否缓冲区全部数据处理完毕, 可以退出
    bool first_spilling_;                                       // 是否触发过至少一次溢写
    bool delete_this_;                                          // 标识缓冲区可以删除(已清空)
};

} // namespace Imagine_MapReduce

#endif