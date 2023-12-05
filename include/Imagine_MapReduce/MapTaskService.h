#ifndef IMAGINE_MAPREDUCE_MAPTASKSERVICE_H
#define IMAGINE_MAPREDUCE_MAPTASKSERVICE_H

#include "Imagine_Rpc/Stub.h"
#include "Imagine_Rpc/Service.h"
#include "Imagine_Rpc/RpcMethodHandler.h"
#include "Mapper.h"
#include "MapTaskMessage.pb.h"
#include "common_definition.h"
#include "TaskCompleteMessage.pb.h"
#include "HeartBeatMessage.pb.h"

namespace Imagine_MapReduce
{

namespace Internal
{

template <typename reader_key, typename reader_value, typename key, typename value>
class MapTaskService : public Imagine_Rpc::Service
{
 public:
    MapTaskService();

    MapTaskService(::Imagine_MapReduce::Mapper<reader_key, reader_value, key, value>* mapper);

    ~MapTaskService();

    void Init();

    Imagine_Rpc::Status MapTaskProcessor(Imagine_Rpc::Context* context, MapTaskRequestMessage* request_msg, MapTaskResponseMessage* response_msg);

 private:
    ::Imagine_MapReduce::Mapper<reader_key, reader_value, key, value>* mapper_;
};

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskService<reader_key, reader_value, key, value>::MapTaskService() : Imagine_Rpc::Service(INTERNAL_MAP_TASK_SERVICE_NAME)
{
    Init();
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskService<reader_key, reader_value, key, value>::MapTaskService(::Imagine_MapReduce::Mapper<reader_key, reader_value, key, value>* mapper) : Imagine_Rpc::Service(INTERNAL_MAP_TASK_SERVICE_NAME), mapper_(mapper)
{
    Init();
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskService<reader_key, reader_value, key, value>::~MapTaskService()
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
void MapTaskService<reader_key, reader_value, key, value>::Init()
{
    // RegisterMethods({INTERNAL_MAP_TASK_METHOD_NAME}, {new Imagine_Rpc::RpcMethodHandler<MapTaskRequestMessage, MapTaskResponseMessage>(std::bind(&MapTaskService<reader_key, reader_value, key, value>::MapTaskProcessor, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3))});
    REGISTER_MEMBER_FUNCTION(INTERNAL_MAP_TASK_METHOD_NAME, MapTaskRequestMessage, MapTaskResponseMessage, (&MapTaskService<reader_key, reader_value, key, value>::MapTaskProcessor));
}

template <typename reader_key, typename reader_value, typename key, typename value>
Imagine_Rpc::Status MapTaskService<reader_key, reader_value, key, value>::MapTaskProcessor(Imagine_Rpc::Context* context, MapTaskRequestMessage* request_msg, MapTaskResponseMessage* response_msg)
{
    // 获取split数据
    std::vector<InputSplit *> splits = MapReduceUtil::DefaultReadSplitFunction(request_msg->file_name_(), request_msg->split_size_()); // 传入目标文件名和split大小
    // 将数据按要求转换成kv数据
    for (size_t i = 0; i < splits.size(); i++) {
        pthread_t* thread = new pthread_t;
        std::shared_ptr<RecordReader<reader_key, reader_value>> new_record_reader = mapper_->GenerateRecordReader(splits[i], i + 1);
        LOG_INFO("Create RecordReader, use count is %d", new_record_reader.use_count());
        MapRunner<reader_key, reader_value, key, value> *runner = mapper_->GenerateMapRunner(i + 1, splits.size(), request_msg->file_name_(), request_msg->listen_ip_(), request_msg->listen_port_());
        runner->SetRecordReader(new_record_reader);
        LOG_INFO("Runner Get RecordReader, use count is %d", new_record_reader.use_count());
        runner->SetTimerCallback(mapper_->GetTimerCallback());
        runner->SetThread(thread);
        runner->SetHeartBeatStub(mapper_->GenerateNewStub());
        runner->SetCompleteStub(mapper_->GenerateNewStub());
        pthread_create(
            thread, nullptr, [](void *argv) -> void *
            {
                MapRunner<reader_key, reader_value, key, value> *runner = (MapRunner<reader_key, reader_value, key, value> *)argv;
                MAP map = runner->GetMap();
                std::shared_ptr<RecordReader<reader_key, reader_value>> reader = runner->GetRecordReader();
                std::shared_ptr<Imagine_Rpc::Stub> heartbeat_stub = runner->GetHeartBeatStub();

                heartbeat_stub->SetServiceName(INTERNAL_HEARTBEAT_SERVICE_NAME)->SetMethodName(INTERNAL_HEARTBEAT_METHOD_NAME)->SetServerIp(runner->GetMasterIp())->SetServerPort(runner->GetMasterPort());

                // 连接Master
                long long timerid;
                Internal::HeartBeatRequestMessage heartbeat_request_msg;
                Internal::HeartBeatResponseMessage response_msg;
                heartbeat_stub->ConnectServer();
                MapReduceUtil::GenerateHeartBeatStartMessage(&heartbeat_request_msg, Internal::Identity::Mapper, runner->GetFileName(), runner->GetId());
                heartbeat_stub->CallConnectServer(&heartbeat_request_msg, &response_msg);
                LOG_INFO("111Mappper Task Start, split id is %d", reader->GetSplitId());
                if (response_msg.status_() == Internal::Status::Ok) {
                    LOG_INFO("Before SetTimer RecordReader, use count is %d", new_record_reader.use_count());
                    timerid = runner->GetRpcServer()->SetTimer(std::bind(runner->GetTimerCallback(), heartbeat_stub, reader), DEFAULT_HEARTBEAT_INTERVAL_TIME, DEFAULT_HEARTBEAT_DELAY_TIME);
                    LOG_INFO("After SetTimer RecordReader, use count is %d", new_record_reader.use_count());
                } else {
                    throw std::exception();
                }
                reader->SetTimerId(timerid);

                runner->StartSpillingThread();
                sleep(1);
                while (reader->NextKeyValue()) {
                    runner->WriteToBuffer(map(reader->GetCurrentKey(), reader->GetCurrentValue()));
                }

                runner->CompleteMapping(); // buffer在spill线程中销毁

                LOG_INFO("Remove Heartbeat Timer, timerid is %ld", timerid);
                // runner->GetRpcServer()->RemoveTimer(timerid);

                std::shared_ptr<Imagine_Rpc::Stub> complete_stub = runner->GetCompleteStub();
                Internal::TaskCompleteRequestMessage complete_request_msg;
                Internal::TaskCompleteResponseMessage complete_response_msg;
                complete_stub->SetServiceName(INTERNAL_TASK_COMPLETE_SERVICE_NAME)->SetMethodName(INTERNAL_TASK_COMPLETE_METHOD_NAME)->SetServerIp(runner->GetMasterIp())->SetServerPort(runner->GetMasterPort());
                complete_stub->ConnectServer();
                MapReduceUtil::GenerateTaskCompleteMessage(&complete_request_msg, Internal::Identity::Mapper, runner->GetFileName(), runner->GetId(), runner->GetSplitNum(), runner->GetMapperIp(), runner->GetMapperPort(), runner->GetShuffleFile());
                LOG_INFO("Mapper Task Complete, split id is %d, msg size is %d", reader->GetSplitId(), complete_request_msg.ByteSize() + complete_response_msg.ByteSize());
                complete_stub->CallConnectServer(&complete_request_msg, &complete_response_msg);

                if (complete_response_msg.status_() == Internal::Status::Ok) {
                    complete_stub->CloseConnection();
                } else {
                    throw std::exception();
                }

                delete runner;
                LOG_INFO("Task Over RecordReader, use count is %d", new_record_reader.use_count());
                return nullptr;
            },
            runner);
        pthread_detach(*thread);
    }
    response_msg->set_status_(Internal::Status::Ok);
    LOG_INFO("Set Response Message OK, response msg size is %d", response_msg->ByteSize());

    return Imagine_Rpc::Status::OK;
}

} // namespace Internal
} // namespace Imagine_MapReduce


#endif