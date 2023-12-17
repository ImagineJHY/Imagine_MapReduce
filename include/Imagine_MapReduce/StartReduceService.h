#ifndef IMAGINE_MAPREDUCE_STARTREDUCESERVICE_H
#define IMAGINE_MAPREDUCE_STARTREDUCESERVICE_H

#include "Reducer.h"
#include "log_macro.h"
#include "StartReduceMessage.pb.h"

#include "Imagine_Rpc/Imagine_Rpc.h"

namespace Imagine_MapReduce
{

template <typename key, typename value>
class Reducer;

namespace Internal
{

template <typename key, typename value>
class StartReduceService : public Imagine_Rpc::Service
{
 public:
    StartReduceService();

    StartReduceService(::Imagine_MapReduce::Reducer<key, value>* reducer);

    ~StartReduceService();

    void Init();

    Imagine_Rpc::Status StartReduceProcessor(Imagine_Rpc::Context* context, StartReduceRequestMessage* request_msg, StartReduceResponseMessage* response_msg);

 private:
    ::Imagine_MapReduce::Reducer<key, value>* reducer_;
};

template <typename key, typename value>
StartReduceService<key, value>::StartReduceService() : Imagine_Rpc::Service(INTERNAL_START_REDUCE_SERVICE_NAME), reducer_(nullptr)
{
    Init();
}

template <typename key, typename value>
StartReduceService<key, value>::StartReduceService(::Imagine_MapReduce::Reducer<key, value>* reducer) : Imagine_Rpc::Service(INTERNAL_START_REDUCE_SERVICE_NAME), reducer_(reducer)
{
    Init();
}

template <typename key, typename value>
StartReduceService<key, value>::~StartReduceService()
{
}

template <typename key, typename value>
void StartReduceService<key, value>::Init()
{
    REGISTER_MEMBER_FUNCTION(INTERNAL_START_REDUCE_METHOD_NAME, StartReduceRequestMessage, StartReduceResponseMessage, (&StartReduceService<key, value>::StartReduceProcessor));
}

template <typename key, typename value>
Imagine_Rpc::Status StartReduceService<key, value>::StartReduceProcessor(Imagine_Rpc::Context* context, StartReduceRequestMessage* request_msg, StartReduceResponseMessage* response_msg)
{
    IMAGINE_MAPREDUCE_LOG("This is Start Reduce Method !");
    std::pair<std::string, std::string> new_master_pair = std::make_pair(request_msg->listen_ip_(), request_msg->listen_port_());
    std::vector<std::string> file_list;
    for (size_t i = 0; i < request_msg->file_list_().size(); i++) {
        IMAGINE_MAPREDUCE_LOG("%s", request_msg->file_list_(i).c_str());
        file_list.push_back(request_msg->file_list_(i));
    }
    reducer_->RegisterMaster(new_master_pair, file_list);
    response_msg->set_status_(Internal::Status::Ok);

    return Imagine_Rpc::Status::OK;
}

} // namespace Internal

} // namespace Imagine_MapReduce


#endif