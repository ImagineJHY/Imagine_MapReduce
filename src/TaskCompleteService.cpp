#include "Imagine_MapReduce/TaskCompleteService.h"

#include "Imagine_MapReduce/log_macro.h"
#include "Imagine_MapReduce/MapReduceMaster.h"
#include "Imagine_MapReduce/TaskCompleteMessage.pb.h"

namespace Imagine_MapReduce
{
namespace Internal
{

TaskCompleteService::TaskCompleteService() : Service(INTERNAL_TASK_COMPLETE_SERVICE_NAME), master_(nullptr)
{
    Init();
}

TaskCompleteService::TaskCompleteService(MapReduceMaster* master) : Service(INTERNAL_TASK_COMPLETE_SERVICE_NAME), master_(master)
{
    Init();
}

TaskCompleteService::~TaskCompleteService()
{
}

void TaskCompleteService::Init()
{
    // RegisterMethods({INTERNAL_TASK_COMPLETE_METHOD_NAME}, {new Imagine_Rpc::RpcMethodHandler<TaskCompleteRequestMessage, TaskCompleteResponseMessage>(std::bind(&TaskCompleteService::TaskCompleteProcessor, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3))});
    REGISTER_MEMBER_FUNCTION(INTERNAL_TASK_COMPLETE_METHOD_NAME, TaskCompleteRequestMessage, TaskCompleteResponseMessage, &TaskCompleteService::TaskCompleteProcessor);
}

Imagine_Rpc::Status TaskCompleteService::TaskCompleteProcessor(Imagine_Rpc::Context* context, TaskCompleteRequestMessage* request_msg, TaskCompleteResponseMessage* response_msg)
{
    if (request_msg->recv_identity_() != Identity::Master) {
        throw std::exception();
    }

    switch (request_msg->send_identity_())
    {
        case Identity::Mapper :
            MapTaskCompleteProcessor(context, request_msg, response_msg);
            break;
        
        case Identity::Reducer :
            MapTaskCompleteProcessor(context, request_msg, response_msg);
            break;
        
        default:
            throw std::exception();
            break;
    }
}

Imagine_Rpc::Status TaskCompleteService::MapTaskCompleteProcessor(Imagine_Rpc::Context* context, TaskCompleteRequestMessage* request_msg, TaskCompleteResponseMessage* response_msg)
{
    IMAGINE_MAPREDUCE_LOG("Recv Map Task Finish Message! file %s split %d can Transport to Reducer!", request_msg->file_name_().c_str(), request_msg->split_id_());

    size_t reducer_num = master_->GetReducerNum();

    if (reducer_num != request_msg->file_list_().size()) {
        throw std::exception();
    }
    for (size_t i = 1; i <= reducer_num; i++) {
        const std::string &split_file_name = request_msg->file_list_(i - 1);
        MapReduceMaster::ReducerNode* reducer_node = master_->FindReducerNode(i);
        if (reducer_node == nullptr) {
            throw std::exception();
        }

        if (!(reducer_node->IsReady())) {
            // reducer未就绪
            reducer_node->Lock();
            if (!(reducer_node->IsReady())) {
                // 再次确认
                IMAGINE_MAPREDUCE_LOG("Searching Reducer!");
                Imagine_Rpc::Stub* stub = master_->GenerateNewStub();
                stub->SetServiceName(INTERNAL_START_REDUCE_SERVICE_NAME)->SetMethodName(INTERNAL_START_REDUCE_METHOD_NAME)->SearchNewServer();
                IMAGINE_MAPREDUCE_LOG("GET REDUCER IP:%s, PORT:%s",stub->GetServerIp().c_str(), stub->GetServerPort().c_str());
                master_->StartReducer(stub->GetServerIp(), stub->GetServerPort());                                           // 启动reducer与master的连接
                reducer_node->SetStub(stub);
                reducer_node->UpdateIsReadyStat(true);
                reducer_node->UnLock();
            } else {
                reducer_node->UnLock();
            }
        }
        master_->ConnReducer(request_msg->split_num_(), request_msg->file_name_(), split_file_name, request_msg->listen_ip_(), request_msg->listen_port_(), reducer_node->GetIp(), reducer_node->GetPort());
    }
}

Imagine_Rpc::Status TaskCompleteService::ReduceTaskCompleteProcessor(Imagine_Rpc::Context* context, TaskCompleteRequestMessage* request_msg, TaskCompleteResponseMessage* response_msg)
{
}

} // namespace Internal
} // namespace Imagine_MapReduce
