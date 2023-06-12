#ifndef IMAGINE_MAPREDUCE_UTIL_H
#define IMAGINE_MAPREDUCE_UTIL_H

#include<functional>
#include<unordered_map>
#include<vector>
#include<string.h>

#include<Imagine_Rpc/Imagine_Rpc/Rpc.h>

#include"InputSplit.h"
#include"Callbacks.h"

using namespace Imagine_Rpc;

namespace Imagine_MapReduce{

class MapReduceUtil
{

public:

    static void DefaultMapFunction(const std::string& read_split);

    static void DefaultReduceFunction(const std::string& input);

    // static std::unordered_map<std::string,std::string> DefaultRecordReader(const std::string& read_split);

    //哈希的方式处理Map输入数据,传入的数据保证正确的开始和结尾(不会读到一句话的一半)
    static void DefaultMapFunctionHashHandler(const std::string& input, std::unordered_map<std::string,int>& kv_map);

    static void DefaultReduceFunctionHashHandler(const std::string& input, std::unordered_map<std::string,int>& kv_map);

    //一次读完split到内存
    static std::vector<InputSplit*> DefaultReadSplitFunction(const std::string& file_name, int split_size=DEFAULT_READ_SPLIT_SIZE);
    // static std::string DefaultReadSplitFunction(const std::string& file_name, int split_size=DEFAULT_READ_SPLIT_SIZE);

    static int StringToInt(const std::string& input);
    static std::string IntToString(int input);
    static std::string DoubleToString(double input);

    static std::string GetIovec(const struct iovec* input_iovec){return Rpc::GetIovec(input_iovec);}

    static bool ReadKVReaderFromMemory(const std::string& content, int& idx, std::string& key_, std::string& value_);
    static bool ReadKVReaderFromDisk(const int fd, std::string& key_, std::string& value_);

    static bool WriteKVReaderToDisk(const int fd, const KVReader* const kv_reader);

    static bool MergeKVReaderFromDisk(const int* const fds, const int fd_num, const std::string& file_name);//对fds对应的所有的文件做多路归并排序

};


}



#endif