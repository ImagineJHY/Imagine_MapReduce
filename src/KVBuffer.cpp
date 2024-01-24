#include "Imagine_MapReduce/KVBuffer.h"

#include "Imagine_MapReduce/log_macro.h"
#include "Imagine_MapReduce/common_macro.h"
#include "Imagine_MapReduce/MapReduceUtil.h"

namespace Imagine_MapReduce
{

KVBuffer::KVBuffer(int partition_num, int split_id, std::vector<std::vector<std::string>> &spill_files)
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

KVBuffer::~KVBuffer()
{
    IMAGINE_MAPREDUCE_LOG("delete buffer");
    delete[] buffer_;
    delete buffer_lock_;
    delete spill_lock_;
    delete spill_cond_;
}

bool KVBuffer::WriteToBuffer(const std::pair<char *, char *> &content, int partition_idx)
{
    IMAGINE_MAPREDUCE_LOG("Write KV to Buffer, key is %s, value is %s", content.first, content.second);
    bool flag = true;
    int key_len = strlen(content.first);
    int value_len = strlen(content.second);
    int key_point;
    int value_point;
    char *key = content.first;
    char *value = content.second;
    char meta_info[DEFAULT_META_SIZE];
    while (flag) {
        // 逻辑上保证kv_idx!=meta_idx恒成立,即永远不允许写满
        // kv_idx始终向上增长,meta_idx始终向下增长
        pthread_mutex_lock(buffer_lock_);
        if (is_spilling_ ? WriteJudgementWithSpilling(content) : WriteJudgementWithoutSpilling(content)) {
            // 空间足够写入
            flag = false;
            key_point = kv_idx_;
            memcpy(meta_info, &key_point, sizeof(key_point));
            memcpy(meta_info + sizeof(key_point) + sizeof(value_point), &value_len, sizeof(value_len));
            memcpy(meta_info + sizeof(key_point) + sizeof(value_point) + sizeof(value_len), &partition_idx, sizeof(partition_idx));
            if (meta_idx_ < kv_idx_ && (buffer_size_ - kv_idx_ < key_len + value_len || meta_idx_ + 1 < DEFAULT_META_SIZE)) {
                // kv或meta不能一次写完
                if (buffer_size_ - kv_idx_ < key_len + value_len) {
                    // 两次写入kv,一次写完meta
                    if (buffer_size_ - kv_idx_ >= key_len) {
                        // 能完整写入key
                        memcpy(buffer_ + kv_idx_, key, key_len);
                        kv_idx_ = (kv_idx_ + key_len) % buffer_size_;

                        value_point = (kv_idx_ + key_len) % buffer_size_;
                        memcpy(meta_info + sizeof(key_point), &value_point, sizeof(value_point));

                        memcpy(buffer_ + kv_idx_, value, buffer_size_ - kv_idx_);
                        memcpy(buffer_, value + buffer_size_ - kv_idx_, value_len - (buffer_size_ - kv_idx_));
                        kv_idx_ = value_len - (buffer_size_ - kv_idx_);
                    }
                    else {
                        // 不能完整写入key
                        memcpy(buffer_ + kv_idx_, key, buffer_size_ - kv_idx_);
                        memcpy(buffer_, key + buffer_size_ - kv_idx_, key_len - (buffer_size_ - kv_idx_));
                        kv_idx_ = key_len - (buffer_size_ - kv_idx_);

                        value_point = kv_idx_;
                        memcpy(meta_info + sizeof(key_point), &value_point, sizeof(value_point));

                        memcpy(buffer_ + kv_idx_, value, value_len);
                        kv_idx_ += value_len;
                    }
                    // 写meta
                    memcpy(buffer_ + meta_idx_ - DEFAULT_META_SIZE + 1, meta_info, strlen(meta_info));
                    meta_idx_ -= DEFAULT_META_SIZE;
                    if (meta_idx_ + 1 == kv_idx_) {
                        meta_idx_ = kv_idx_; // 写满
                    }
                } else if (meta_idx_ + 1 < DEFAULT_META_SIZE) {
                    // 两次写入meta,一次写完kv
                    memcpy(buffer_ + kv_idx_, key, key_len);
                    kv_idx_ += key_len;

                    value_point = kv_idx_;
                    memcpy(meta_info + sizeof(key_point), &value_point, sizeof(value_point));

                    memcpy(buffer_ + kv_idx_, value, value_len);
                    kv_idx_ += value_len;
                    // 写meta
                    memcpy(buffer_, meta_info + DEFAULT_META_SIZE - (meta_idx_ + 1), meta_idx_ + 1);
                    memcpy(buffer_ + buffer_size_ - DEFAULT_META_SIZE + (meta_idx_ + 1), meta_info, DEFAULT_META_SIZE - (meta_idx_ + 1));
                    meta_idx_ = buffer_size_ - (DEFAULT_META_SIZE - (meta_idx_ + 1)) - 1;
                    if (meta_idx_ + 1 == kv_idx_) {
                        meta_idx_ = kv_idx_; // 写满
                    }
                }
            } else {
                memcpy(buffer_ + kv_idx_, key, key_len);
                kv_idx_ += key_len;

                value_point = kv_idx_;
                memcpy(meta_info + sizeof(key_point), &value_point, sizeof(value_point));

                memcpy(buffer_ + kv_idx_, value, value_len);
                kv_idx_ = (kv_idx_ + value_len) % buffer_size_;

                memcpy(buffer_ + meta_idx_ - DEFAULT_META_SIZE + 1, meta_info, DEFAULT_META_SIZE);
                meta_idx_ -= DEFAULT_META_SIZE;
                if (meta_idx_ == -1) {
                    if (kv_idx_ == 0) {
                        meta_idx_ = 0;
                    } else {
                        meta_idx_ = buffer_size_ - 1;
                    }
                } else if (meta_idx_ < kv_idx_) {
                    meta_idx_ = kv_idx_; // 已满
                }
            }
        } else {
            // 主动休眠
        }
        pthread_mutex_unlock(buffer_lock_);
        if ((meta_idx_ < kv_idx_ ? buffer_size_ - (kv_idx_ - meta_idx_ - 1) : meta_idx_ - kv_idx_ + 1) * 1.0 / buffer_size_ <= spill_size_) {
            pthread_cond_signal(spill_cond_);
        }
    }

    return true;
}

bool KVBuffer::Spilling()
{
    pthread_mutex_lock(buffer_lock_);
    int written_size = meta_idx_ < kv_idx_ ? kv_idx_ - meta_idx_ : buffer_size_ - (meta_idx_ - kv_idx_);
    // TODO : first_spilling_无意义, written_size也保证了缓冲区的清空, 原本逻辑无bug, 新逻辑下也可删除written_size
    pthread_mutex_unlock(buffer_lock_);
    while (!first_spilling_ || !spill_buffer_ || written_size != 1) {
        first_spilling_ = true;
        pthread_mutex_lock(spill_lock_);

        pthread_mutex_lock(buffer_lock_);
        // Spill结束,保证spilling在临界区改变值(可以改为atomic类型)
        is_spilling_ = false;
        pthread_mutex_unlock(buffer_lock_);

        while ((!spill_buffer_) && (meta_idx_ < kv_idx_ ? buffer_size_ - (kv_idx_ - meta_idx_ - 1) : meta_idx_ - kv_idx_ + 1) * 1.0 / buffer_size_ > spill_size_) {
            pthread_cond_wait(spill_cond_, spill_lock_);
        }
        // 开始spill
        pthread_mutex_unlock(spill_lock_);
        Spill();
        // 尝试唤醒


        pthread_mutex_lock(buffer_lock_);
        written_size = meta_idx_ < kv_idx_ ? kv_idx_ - meta_idx_ : buffer_size_ - (meta_idx_ - kv_idx_);
        pthread_mutex_unlock(buffer_lock_);
    }
    IMAGINE_MAPREDUCE_LOG("Start Last Spilling Process!");
    Spill();
    *quit_ = true;

    return true;
}

void KVBuffer::Spill()
{
    
    pthread_mutex_lock(buffer_lock_);
    is_spilling_ = true;
    int old_equator = equator_;
    // int old_kv_idx = kv_idx_;
    int old_meta_idx = meta_idx_ % buffer_size_;
    equator_ = ((meta_idx_ < kv_idx_ ? buffer_size_ - (kv_idx_ - meta_idx_ - 1) : meta_idx_ - kv_idx_ + 1) / 2 + kv_idx_) % buffer_size_;
    kv_border_ = (meta_idx_ + 1) % buffer_size_;
    meta_border_ = kv_idx_ - 1;
    if (meta_border_ < 0) {
        meta_border_ = buffer_size_ - 1;
    }
    kv_idx_ = (equator_ + 1) % buffer_size_;
    meta_idx_ = equator_;
    pthread_mutex_unlock(buffer_lock_);
    std::priority_queue<MetaIndex, std::vector<MetaIndex>, MetaCmp> meta_queue;
    while ((old_meta_idx <= old_equator ? old_equator - old_meta_idx : buffer_size_ - old_meta_idx + old_equator) >= DEFAULT_META_SIZE) {
        // printf("buffer_size is %d,meta_idx is %d,equator is %d\n",buffer_size,old_meta_idx,old_equator);
        meta_queue.push(MetaIndex::GetMetaIndex(buffer_, (old_meta_idx + 1) % buffer_size_, buffer_size_));
        old_meta_idx = (old_meta_idx + DEFAULT_META_SIZE) % buffer_size_;
    }
    if (meta_queue.size() == 0) return;
    // std::string file_name="split_"+MapReduceUtil::IntToString(split_id)+"_spill_"+MapReduceUtil::IntToString(spill_id)+"_shuffle_";
    std::string file_name = "spill_" + MapReduceUtil::IntToString(spill_id_) + "_split_" + MapReduceUtil::IntToString(split_id_) + "_shuffle_";
    int fds[partition_num_];
    for (int i = 0; i < partition_num_; i++) {
        spill_files_[i].push_back(file_name + MapReduceUtil::IntToString(i + 1) + ".txt");
        fds[i] = open(&(spill_files_[i].back()[0]), O_CREAT | O_RDWR, 0777);
    }

    while (meta_queue.size()) {
        MetaIndex top_meta = meta_queue.top();
        meta_queue.pop();
        int key_len = top_meta.GetKeyLen();
        char *key = new char[key_len];
        int value_len = top_meta.GetValueLen();
        char *value = new char[value_len];
        top_meta.GetKey(key);
        top_meta.GetValue(value);

        int fd = fds[top_meta.GetPartition() - 1];
        write(fd, key, key_len);
        char c = ' ';
        write(fd, &c, 1);
        write(fd, value, value_len);
        char cc[] = "\r\n";
        write(fd, cc, 2);

        delete[] key;
        delete[] value;
    }
    // spill_file_name.push_back(file_name);//装入临时文件名
    spill_id_++;

    for (int i = 0; i < partition_num_; i++) {
        close(fds[i]);
    }
}

bool KVBuffer::WriteJudgementWithSpilling(const std::pair<char *, char *> &content) const
{
    if (static_cast<size_t>(kv_idx_ <= kv_border_ ? kv_border_ - kv_idx_ : buffer_size_ - kv_idx_ + kv_border_) < strlen((char *)(content.first)) + strlen((char *)(content.second)) || 
        (meta_border_ <= meta_idx_ ? meta_idx_ - meta_border_ : meta_idx_ + buffer_size_ - meta_border_) < DEFAULT_META_SIZE) {
        return false;
    }

    return true;
}

bool KVBuffer::WriteJudgementWithoutSpilling(const std::pair<char *, char *> &content) const
{
    if (static_cast<size_t>(meta_idx_ < kv_idx_ ? buffer_size_ - (kv_idx_ - meta_idx_ - 1) : meta_idx_ - kv_idx_ + 1) > strlen((char *)(content.first)) + strlen((char *)(content.second)) + DEFAULT_META_SIZE) {
        return true;
    }

    return false;
}

bool KVBuffer::SpillBuffer()
{
    spill_buffer_ = true;
    while (!(*quit_)) {
        pthread_cond_broadcast(spill_cond_);
    }
    delete quit_;
    delete_this_ = true;

    return true;
}

bool KVBuffer::IsDeleteConditionSatisfy() const
{
    return delete_this_;
}

} // namespace Imagine_MapReduce