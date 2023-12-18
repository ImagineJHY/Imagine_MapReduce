#ifndef IMAGINE_MAPREDUCE_MAPTASKHANDLER_H
#define IMAGINE_MAPREDUCE_MAPTASKHANDLER_H

#include "log_macro.h"

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value, typename key, typename value>
class MapRunner;

namespace Internal
{

template <typename reader_key, typename reader_value, typename key, typename value>
class MapTaskHandler
{
 public:
    static void HandleEvent();

 public:
    MapTaskHandler(MapRunner<reader_key, reader_value, key, value>* runner);

    ~MapTaskHandler();

 private:
    MapTaskHandler();

 private:
    MapRunner<reader_key, reader_value, key, value>* runner_;
};

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskHandler<reader_key, reader_value, key, value>::MapTaskHandler()
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskHandler<reader_key, reader_value, key, value>::MapTaskHandler(MapRunner<reader_key, reader_value, key, value>* runner) : runner_(runner)
{
}

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskHandler<reader_key, reader_value, key, value>::~MapTaskHandler()
{
    delete runner_;
}

template <typename reader_key, typename reader_value, typename key, typename value>
void MapTaskHandler<reader_key, reader_value, key, value>::HandleEvent()
{
    MapCallback<reader_key, reader_value, key, value> map = runner->GetMap();
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
    IMAGINE_MAPREDUCE_LOG("111Mappper Task Start, split id is %d", reader->GetSplitId());
    if (response_msg.status_() == Internal::Status::Ok) {
        IMAGINE_MAPREDUCE_LOG("Before SetTimer RecordReader, use count is %d", reader.use_count());
        timerid = runner->GetRpcServer()->SetTimer(std::bind(runner->GetTimerCallback(), heartbeat_stub, reader), DEFAULT_HEARTBEAT_INTERVAL_TIME, DEFAULT_HEARTBEAT_DELAY_TIME);
        IMAGINE_MAPREDUCE_LOG("After SetTimer RecordReader, use count is %d", reader.use_count());
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

    IMAGINE_MAPREDUCE_LOG("Remove Heartbeat Timer, timerid is %ld", timerid);
    // runner->GetRpcServer()->RemoveTimer(timerid);

    std::shared_ptr<Imagine_Rpc::Stub> complete_stub = runner->GetCompleteStub();
    Internal::TaskCompleteRequestMessage complete_request_msg;
    Internal::TaskCompleteResponseMessage complete_response_msg;
    complete_stub->SetServiceName(INTERNAL_TASK_COMPLETE_SERVICE_NAME)->SetMethodName(INTERNAL_TASK_COMPLETE_METHOD_NAME)->SetServerIp(runner->GetMasterIp())->SetServerPort(runner->GetMasterPort());
    complete_stub->ConnectServer();
    MapReduceUtil::GenerateTaskCompleteMessage(&complete_request_msg, Internal::Identity::Mapper, runner->GetFileName(), runner->GetId(), runner->GetSplitNum(), runner->GetMapperIp(), runner->GetMapperPort(), runner->GetShuffleFile());
    IMAGINE_MAPREDUCE_LOG("Mapper Task Complete, split id is %d, msg size is %d", reader->GetSplitId(), complete_request_msg.ByteSize() + complete_response_msg.ByteSize());
    complete_stub->CallConnectServer(&complete_request_msg, &complete_response_msg);

    if (complete_response_msg.status_() == Internal::Status::Ok) {
        complete_stub->CloseConnection();
    } else {
        throw std::exception();
    }

    delete runner;
    IMAGINE_MAPREDUCE_LOG("Task Over RecordReader, use count is %d, split id is %d", reader.use_count(), reader->GetSplitId()); 
}

} // namespace Internal

} // namespace Imagine_MapReduce


#endif