#ifndef IMAGINE_MAPREDUCE_PARTITIONER_H
#define IMAGINE_MAPREDUCE_PARTITIONER_H

#include "common_macro.h"

namespace Imagine_MapReduce
{

template <typename key>
class Partitioner
{
 public:
    Partitioner(int partition_num = DEFAULT_PARTITION_NUM);

    virtual ~Partitioner();

    virtual int Partition(const key& partition_key) const = 0;

 protected:
    const int partition_num_;
};

template <typename key>
Partitioner<key>::Partitioner(int partition_num) : partition_num_(partition_num)
{
}

template <typename key>
Partitioner<key>::~Partitioner()
{
}

} // namespace Imagine_MapReduce

#endif