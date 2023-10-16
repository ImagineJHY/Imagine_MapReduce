#ifndef IMAGINE_MAPREDUCE_PARTITIONER_H
#define IMAGINE_MAPREDUCE_PARTITIONER_H

#include "Callbacks.h"

namespace Imagine_MapReduce
{

template <typename key>
class Partitioner
{
 public:
    Partitioner(int partition_num = DEFAULT_PARTITION_NUM) : partition_num_(partition_num) {}

    virtual ~Partitioner() {}

    virtual int Partition(key partition_key) = 0;

 protected:
    const int partition_num_;
};

} // namespace Imagine_MapReduce

#endif