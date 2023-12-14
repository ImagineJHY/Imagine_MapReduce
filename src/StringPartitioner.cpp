#include "Imagine_MapReduce/StringPartitioner.h"

namespace Imagine_MapReduce
{

StringPartitioner::StringPartitioner(int partition_num) : Partitioner(partition_num)
{
}

StringPartitioner::~StringPartitioner()
{
}

int StringPartitioner::Partition(const std::string& key) const
{
    std::unordered_set<std::string> set;

    return ((set.hash_function()(key)) % partition_num_) + 1;
}

} // namespace Imagine_MapReduce