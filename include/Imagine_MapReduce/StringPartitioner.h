#ifndef IMAGINE_MAPREDUCE_STRINGPARTITIONER_H
#define IMAGINE_MAPREDUCE_STRINGPARTITIONER_H

#include "Partitioner.h"

#include <string>
#include <unordered_set>

namespace Imagine_MapReduce
{

class StringPartitioner : public Partitioner<std::string>
{
 public:
    StringPartitioner(int partition_num = DEFAULT_PARTITION_NUM);

    ~StringPartitioner();

    int Partition(const std::string& key) const;
};

} // namespace Imagine_MapReduce

#endif