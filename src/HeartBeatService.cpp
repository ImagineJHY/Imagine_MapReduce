#include "Imagine_MapReduce/HeartBeatService.h"

#include "Imagine_MapReduce/common_definition.h"
#include "Imagine_MapReduce/HeartBeatMessage.pb.h"

namespace Imagine_MapReduce
{
namespace Internal
{

HeartBeatService::HeartBeatService() : Imagine_Rpc::Service(INTERNAL_HEARTBEAT_SERVICE_NAME)
{
    Init();
}

HeartBeatService::~HeartBeatService()
{
}

void HeartBeatService::Init()
{
    REGISTER_MEMBER_FUNCTION(INTERNAL_HEARTBEAT_METHOD_NAME, HeartBeatRequestMessage, HeartBeatResponseMessage, &HeartBeatService::HeartBeatPacketProcessor);
}

Imagine_Rpc::Status HeartBeatService::HeartBeatPacketProcessor(Imagine_Rpc::Context* context, HeartBeatRequestMessage* request_msg, HeartBeatResponseMessage* response_msg)
{
    if (request_msg->recv_identity_() != Identity::Master) {
        throw std::exception();
    }

    switch (request_msg->send_identity_())
    {
        case Identity::Mapper :
            return MapTaskHeartBeatPacketProcessor(context, request_msg, response_msg);
        
        case Identity::Reducer :
            return ReduceTaskHeartBeatPacketProcessor(context, request_msg, response_msg);

        default:
            throw std::exception();
            break;
    }
}

Imagine_Rpc::Status HeartBeatService::MapTaskHeartBeatPacketProcessor(Imagine_Rpc::Context* context, HeartBeatRequestMessage* request_msg, HeartBeatResponseMessage* response_msg)
{
    LOG_INFO("Map Task HeartBeat Packet!");
    switch (request_msg->task_status_())
    {
        case TaskStatus::Start :
            LOG_INFO("File %s split %d Task Start!", request_msg->file_name_().c_str(), request_msg->split_id_());
            break;
        
        case TaskStatus::Process :
            LOG_INFO("File %s split %d Task Processing Progress is %lf%!", request_msg->file_name_().c_str(), request_msg->split_id_(), request_msg->task_progress_());
            break;
        
        default:
            throw std::exception();
            break;
    }
    response_msg->set_status_(Status::Ok);
    return Imagine_Rpc::Status::OK;
}

Imagine_Rpc::Status HeartBeatService::ReduceTaskHeartBeatPacketProcessor(Imagine_Rpc::Context* context, HeartBeatRequestMessage* request_msg, HeartBeatResponseMessage* response_msg)
{
    LOG_INFO("Reduce Task HeartBeat Packet!");
}

} // namespace Internal
} // namespace Imagine_MapReducenamespace Internal
