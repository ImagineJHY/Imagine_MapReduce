#ifndef IMAGINE_MAPREDUCE_MAPTASKSERVICE_H
#define IMAGINE_MAPREDUCE_MAPTASKSERVICE_H

#include "Mapper.h"
#include "MapTaskMessage.pb.h"
#include "HeartBeatMessage.pb.h"
#include "TaskCompleteMessage.pb.h"

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value, typename key, typename value>
class Mapper;

namespace Internal
{

template <typename reader_key, typename reader_value, typename key, typename value>
class MapTaskService : public Imagine_Rpc::Service
{
 public:
    MapTaskService(::Imagine_MapReduce::Mapper<reader_key, reader_value, key, value>* mapper);

    ~MapTaskService();

    void Init();

    Imagine_Rpc::Status MapTaskProcessor(Imagine_Rpc::Context* context, MapTaskRequestMessage* request_msg, MapTaskResponseMessage* response_msg);

 private:
    MapTaskService(); // 不允许使用!

 private:
    const ::Imagine_MapReduce::Mapper<reader_key, reader_value, key, value>* mapper_;
};

template <typename reader_key, typename reader_value, typename key, typename value>
MapTaskService<reader_key, reader_value, key, value>::MapTaskService() : Imagine_Rpc::Service(INTERNAL_MAP_TASK_SERVICE_NAME), mapper_(nullptr)
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
    REGISTER_MEMBER_FUNCTION(INTERNAL_MAP_TASK_METHOD_NAME, MapTaskRequestMessage, MapTaskResponseMessage, (&MapTaskService<reader_key, reader_value, key, value>::MapTaskProcessor));
}

template <typename reader_key, typename reader_value, typename key, typename value>
Imagine_Rpc::Status MapTaskService<reader_key, reader_value, key, value>::MapTaskProcessor(Imagine_Rpc::Context* context, MapTaskRequestMessage* request_msg, MapTaskResponseMessage* response_msg)
{
    // 获取split数据
    std::vector<InputSplit *> splits = MapReduceUtil::DefaultReadSplitFunction(request_msg->file_name_(), request_msg->split_size_()); // 传入目标文件名和split大小
    // 将数据按要求转换成kv数据
    for (size_t i = 0; i < splits.size(); i++) {
        std::shared_ptr<RecordReader<reader_key, reader_value>> new_record_reader = mapper_->GenerateRecordReader(splits[i], i + 1);
        IMAGINE_MAPREDUCE_LOG("Create RecordReader, use count is %d", new_record_reader.use_count());
        MapRunner<reader_key, reader_value, key, value> *runner = mapper_->GenerateMapRunner(i + 1, splits.size(), request_msg->file_name_(), request_msg->listen_ip_(), request_msg->listen_port_());
        runner->SetRecordReader(new_record_reader);
        IMAGINE_MAPREDUCE_LOG("Runner Get RecordReader, use count is %d", new_record_reader.use_count());
        runner->SetTimerCallback(mapper_->GetTimerCallback());
        runner->SetHeartBeatStub(mapper_->GenerateNewStub());
        runner->SetCompleteStub(mapper_->GenerateNewStub());
        mapper_->AddNewMapTaskHandler(std::make_shared<MapTaskService<reader_key, reader_value, key, value>>(runner));
    }
    response_msg->set_status_(Internal::Status::Ok);
    IMAGINE_MAPREDUCE_LOG("Set Response Message OK, response msg size is %d", response_msg->ByteSize());

    return Imagine_Rpc::Status::OK;
}

} // namespace Internal
} // namespace Imagine_MapReduce


#endif