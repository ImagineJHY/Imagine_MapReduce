#ifndef IMAGINE_MAPREDUCE_STRINGPARTITIONER_H
#define IMAGINE_MAPREDUCE_STRINGPARTITIONER_H

#include<string>
#include<unordered_set>

#include"Partitioner.h"

namespace Imagine_MapReduce{

class StringPartitioner : public Partitioner<std::string>
{


public:

    StringPartitioner(int partition_num_=DEFAULT_PARTITION_NUM):Partitioner(partition_num_){}

    ~StringPartitioner(){}

    int Partition(std::string key)
    {
        std::unordered_set<std::string> set_;
        return ((set_.hash_function()(key))%partition_num)+1;
    }
};



}


#endif