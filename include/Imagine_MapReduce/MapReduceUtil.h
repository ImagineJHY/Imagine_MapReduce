#ifndef IMAGINE_MAPREDUCE_UTIL_H
#define IMAGINE_MAPREDUCE_UTIL_H

#include "Imagine_Rpc/RpcUtil.h"
#include "InputSplit.h"
#include "common_definition.h"
#include "InternalType.pb.h"

#include <functional>
#include <unordered_map>
#include <vector>
#include <string.h>

namespace Imagine_MapReduce
{

namespace Internal
{

class MapTaskRequestMessage;
class ReduceTaskRequestMessage;
class HeartBeatRequestMessage;
class TaskCompleteRequestMessage;
class StartReduceRequestMessage;
class RetrieveSplitFileRequestMessage;

} // namespace Internal


class MapReduceUtil
{
 public:
    static void DefaultMapFunction(const std::string &read_split);

    static void DefaultReduceFunction(const std::string &input);

    // static std::unordered_map<std::string,std::string> DefaultRecordReader(const std::string& read_split);

    // 哈希的方式处理Map输入数据,传入的数据保证正确的开始和结尾(不会读到一句话的一半)
    static void DefaultMapFunctionHashHandler(const std::string &input, std::unordered_map<std::string, int> &kv_map);

    static void DefaultReduceFunctionHashHandler(const std::string &input, std::unordered_map<std::string, int> &kv_map);

    // 一次读完split到内存
    static std::vector<InputSplit *> DefaultReadSplitFunction(const std::string &file_name, int split_size = DEFAULT_READ_SPLIT_SIZE);
    // static std::string DefaultReadSplitFunction(const std::string& file_name, int split_size=DEFAULT_READ_SPLIT_SIZE);

    static int StringToInt(const std::string &input);
    static std::string IntToString(int input);
    static std::string DoubleToString(double input);

    static bool ReadKVReaderFromMemory(const std::string &content, int &idx, std::string &key, std::string &value);
    static bool ReadKVReaderFromDisk(const int fd, std::string &key, std::string &value);

    static bool WriteKVReaderToDisk(const int fd, const KVReader *const kv_reader);

    static bool MergeKVReaderFromDisk(const int *const fds, const int fd_num, const std::string &file_name); // 对fds对应的所有的文件做多路归并排序

    static void GenerateMapTaskMessage(Internal::MapTaskRequestMessage* request_msg, const std::string& file_name, size_t split_size, const std::string& master_ip, const std::string& master_port);

    static void GenerateReduceTaskMessage(Internal::ReduceTaskRequestMessage* request_msg, const std::string& file_name, const std::string& split_file_name, size_t split_num, const std::string& mapper_ip, const std::string& mapper_port, const std::string& master_ip, const std::string& master_port);

    static void GenerateHeartBeatStartMessage(Internal::HeartBeatRequestMessage* request_msg, Internal::Identity send_identity, const std::string& file_name, size_t split_id);

    static void GenerateHeartBeatProcessMessage(Internal::HeartBeatRequestMessage* request_msg, Internal::Identity send_identity, const std::string& file_name, size_t split_id, double process);

    static void GenerateTaskCompleteMessage(Internal::TaskCompleteRequestMessage* request_msg, Internal::Identity send_identity, const std::string& file_name, size_t split_id, size_t split_num, const std::string& listen_ip, const std::string& listen_port, std::vector<std::string> file_list);\

    static void GenerateStartReduceMessage(Internal::StartReduceRequestMessage* request_msg, const std::string& master_ip, const std::string& master_port, std::vector<std::string>& file_list);

    static void GenerateRetrieveSplitFileMessage(Internal::RetrieveSplitFileRequestMessage* request_msg, const std::string& split_file_name);
};

} // namespace Imagine_MapReduce

#endif