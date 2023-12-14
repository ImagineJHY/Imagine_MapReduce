#ifndef IMAGINE_MAPREDUCE_COMMON_DEFINITION_H
#define IMAGINE_MAPREDUCE_COMMON_DEFINITION_H

#include <memory>
#include <string.h>

namespace Imagine_MapReduce
{

/*
    -用于多路归并排序的kv读取(基于默认方式)
        -" "作为key和value的分隔符
        -"\r\n"作为每一对数据的分隔符
*/
class KVReader
{
 public:
    KVReader(const std::string& key, const std::string& value, int idx) : reader_key_(key), reader_value_(value), reader_idx_(idx) {}

    std::string GetReaderKey() const { return reader_key_; }

    std::string GetReaderValue() const {return reader_value_; }

    int GetReaderIdx() const { return reader_idx_; }

 private:
    std::string reader_key_;
    std::string reader_value_;
    int reader_idx_;
};

class KVReaderCmp
{
 public:
    bool operator()(KVReader *a, KVReader *b)
    {
        return strcmp(a->GetReaderKey().c_str(), b->GetReaderKey().c_str()) < 0 ? true : false;
    }
};

class HashPair
{
 public:
    template <typename first, typename second>
    std::size_t operator()(const std::pair<first, second> &p) const
    {
        auto hash1 = std::hash<first>()(p.first);
        auto hash2 = std::hash<second>()(p.second);
        return hash1 ^ hash2;
    }
};

class EqualPair
{
 public:
    template <typename first, typename second>
    bool operator()(const std::pair<first, second> &a, const std::pair<first, second> &b) const
    {
        return a.first == b.first && a.second == b.second;
    }
};

} // namespace Imagine_MapReduce

#endif