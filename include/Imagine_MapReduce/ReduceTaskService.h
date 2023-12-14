#ifndef IMAGINE_MAPREDUCE_REDUCETASKSERVICE_H
#define IMAGINE_MAPREDUCE_REDUCETASKSERVICE_H

#include "Reducer.h"
#include "log_macro.h"
#include "ReduceTaskMessage.pb.h"

#include "Imagine_Rpc/Imagine_Rpc.h"

namespace Imagine_MapReduce
{

template <typename key, typename value>
class Reducer;

namespace Internal
{

template <typename key, typename value>
class ReduceTaskService : public Imagine_Rpc::Service
{
 public:
    ReduceTaskService();

    ReduceTaskService(::Imagine_MapReduce::Reducer<key, value>* reducer);

    ~ReduceTaskService();

    void Init();

    Imagine_Rpc::Status ReduceProcessor(Imagine_Rpc::Context* context, ReduceTaskRequestMessage* request_msg, ReduceTaskResponseMessage* response_msg);

 private:
    ::Imagine_MapReduce::Reducer<key, value>* reducer_;
};

template <typename key, typename value>
ReduceTaskService<key, value>::ReduceTaskService() : Imagine_Rpc::Service(INTERNAL_REDUCE_TASK_SERVICE_NAME), reducer_(nullptr)
{
    Init();
}

template <typename key, typename value>
ReduceTaskService<key, value>::ReduceTaskService(::Imagine_MapReduce::Reducer<key, value>* reducer) : Imagine_Rpc::Service(INTERNAL_REDUCE_TASK_SERVICE_NAME), reducer_(reducer)
{
    Init();
}

template <typename key, typename value>
ReduceTaskService<key, value>::~ReduceTaskService()
{
}

template <typename key, typename value>
void ReduceTaskService<key, value>::Init()
{
    REGISTER_MEMBER_FUNCTION(INTERNAL_REDUCE_TASK_METHOD_NAME, ReduceTaskRequestMessage, ReduceTaskResponseMessage, (&ReduceTaskService<key, value>::ReduceProcessor));
}

template <typename key, typename value>
Imagine_Rpc::Status ReduceTaskService<key, value>::ReduceProcessor(Imagine_Rpc::Context* context, ReduceTaskRequestMessage* request_msg, ReduceTaskResponseMessage* response_msg)
{
    IMAGINE_MAPREDUCE_LOG("this is Reduce Method !");
    std::pair<std::string, std::string> master_pair = std::make_pair(request_msg->master_ip_(), request_msg->master_port_());
    std::pair<std::string, std::string> mapper_pair = std::make_pair(request_msg->mapper_ip_(), request_msg->mapper_port_());
    reducer_->ReceiveSplitFileData(master_pair, mapper_pair, request_msg->file_name_(), request_msg->split_file_name_(), request_msg->split_num_());
    response_msg->set_status_(Status::Ok);

    return Imagine_Rpc::Status::OK;
}


} // namespace Internal
} // namespace Imagine_MapReduce

#endif