#ifndef IMAGINE_MAPREDUCE_HEARTBEAT_SERVICE_H
#define IMAGINE_MAPREDUCE_HEARTBEAT_SERVICE_H

#include "Imagine_Rpc/Imagine_Rpc.h"

namespace Imagine_MapReduce
{

namespace Internal
{

class HeartBeatRequestMessage;
class HeartBeatResponseMessage;

class HeartBeatService : public Imagine_Rpc::Service
{
 public:
    HeartBeatService();

    ~HeartBeatService();

    void Init();

    Imagine_Rpc::Status HeartBeatPacketProcessor(Imagine_Rpc::Context* context, HeartBeatRequestMessage* request_msg, HeartBeatResponseMessage* response_msg);

    Imagine_Rpc::Status MapTaskHeartBeatPacketProcessor(Imagine_Rpc::Context* context, HeartBeatRequestMessage* request_msg, HeartBeatResponseMessage* response_msg);

    Imagine_Rpc::Status ReduceTaskHeartBeatPacketProcessor(Imagine_Rpc::Context* context, HeartBeatRequestMessage* request_msg, HeartBeatResponseMessage* response_msg);
};  

} // namespace Internal

} // namespace Imagine_MapReduce

#endif