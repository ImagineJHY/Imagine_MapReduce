#ifndef IMAGINE_MAPREDUCE_STRINGPARTITIONER_H
#define IMAGINE_MAPREDUCE_STRINGPARTITIONER_H

#include <string>
#include <unordered_set>

#include "Partitioner.h"

namespace Imagine_MapReduce
{

class StringPartitioner : public Partitioner<std::string>
{
 public:
    StringPartitioner(int partition_num = DEFAULT_PARTITION_NUM) : Partitioner(partition_num) {}

    ~StringPartitioner() {}

    int Partition(std::string key)
    {
        std::unordered_set<std::string> set;
        return ((set.hash_function()(key)) % partition_num_) + 1;
    }
};

} // namespace Imagine_MapReduce

#endif