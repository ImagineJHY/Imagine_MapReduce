#ifndef IMAGINE_MAPREDUCE_TASKCOMPLETESERVICE_H
#define IMAGINE_MAPREDUCE_TASKCOMPLETESERVICE_H

#include "Imagine_Rpc/Imagine_Rpc.h"

namespace Imagine_MapReduce
{

class MapReduceMaster;

namespace Internal
{

class TaskCompleteRequestMessage;
class TaskCompleteResponseMessage;

class TaskCompleteService : public Imagine_Rpc::Service
{
 public:
    TaskCompleteService();

    TaskCompleteService(MapReduceMaster* master);

    ~TaskCompleteService();

    void Init();

    Imagine_Rpc::Status TaskCompleteProcessor(Imagine_Rpc::Context* context, TaskCompleteRequestMessage* request_msg, TaskCompleteResponseMessage* response_msg);

    Imagine_Rpc::Status MapTaskCompleteProcessor(Imagine_Rpc::Context* context, TaskCompleteRequestMessage* request_msg, TaskCompleteResponseMessage* response_msg);

    Imagine_Rpc::Status ReduceTaskCompleteProcessor(Imagine_Rpc::Context* context, TaskCompleteRequestMessage* request_msg, TaskCompleteResponseMessage* response_msg);
 
 private:
    MapReduceMaster* master_;
};


} // namespace Internal

} // namespace Imagine_MapReduce


#endif