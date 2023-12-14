#ifndef IMAGINE_MAPREDUCE_RECORDREADER_H
#define IMAGINE_MAPREDUCE_RECORDREADER_H

#include "log_macro.h"
#include "InputSplit.h"

#include "Imagine_Rpc/Imagine_Rpc.h"

#include <atomic>

namespace Imagine_MapReduce
{

class InputSplit;

template <typename reader_key, typename reader_value>
class RecordReader
{
 public:
    RecordReader(InputSplit *split = nullptr, int split_id = 0);

    virtual ~RecordReader();

    virtual bool NextKeyValue() = 0;

    virtual reader_key GetCurrentKey() const = 0;

    virtual reader_value GetCurrentValue() const = 0;

    // 获取当前运行进展
    virtual double GetProgress() const = 0;

    // 关闭RecordReader
    virtual void Close() = 0;

    // 创建自己类型返回
    virtual std::shared_ptr<RecordReader<reader_key, reader_value>> CreateRecordReader(InputSplit *split = nullptr, int split_id = 0) const = 0;

    RecordReader<reader_key, reader_value>* SetOutputFileName(const std::string &name);

    int GetSplitId() const;

    std::string GetFileName() const;

    RecordReader<reader_key, reader_value>* SetTimerId(long long timerid);

    long long GetTimerId() const;

    RecordReader<reader_key, reader_value>* SetServer(Imagine_Rpc::RpcServer* rpc_server);

    Imagine_Rpc::RpcServer* GetServer() const;

 protected:
    const int split_id_;                        // split的Id
    const std::string file_name_;               // 输出文件名
    InputSplit *split_;                         // 文件的某个split对象, 在RecordReader中删除
    Imagine_Rpc::RpcServer *rpc_server_;        // rpc服务器对象, 后期考虑有无可能改成const Imagine_Rpc::RpcServer *类型
    std::atomic<long long> timerid_;            // Mapper定时向Master汇报任务进度的timerId, 用于任务结束时在回调函数中销毁定时器
};

template <typename reader_key, typename reader_value>
RecordReader<reader_key, reader_value>::RecordReader(InputSplit *split, int split_id) : split_id_(split_id), file_name_(split != nullptr ? split->GetFileName() : ""), split_(split)
{
}

template <typename reader_key, typename reader_value>
RecordReader<reader_key, reader_value>::~RecordReader()
{
    if (split_) {
        delete split_;
    }
    IMAGINE_MAPREDUCE_LOG("delete recordreader!");
}

template <typename reader_key, typename reader_value>
RecordReader<reader_key, reader_value>* RecordReader<reader_key, reader_value>::SetOutputFileName(const std::string &name)
{
    file_name_ = name;

    return this;
}

template <typename reader_key, typename reader_value>
int RecordReader<reader_key, reader_value>::GetSplitId() const
{
    return split_id_;
}

template <typename reader_key, typename reader_value>
std::string RecordReader<reader_key, reader_value>::GetFileName() const
{
    return file_name_;
}

template <typename reader_key, typename reader_value>
RecordReader<reader_key, reader_value>* RecordReader<reader_key, reader_value>::SetTimerId(long long timerid)
{
    timerid_.store(timerid);

    return this;
}

template <typename reader_key, typename reader_value>
long long RecordReader<reader_key, reader_value>::GetTimerId() const
{
    return timerid_.load();
}

template <typename reader_key, typename reader_value>
RecordReader<reader_key, reader_value>* RecordReader<reader_key, reader_value>::SetServer(Imagine_Rpc::RpcServer* rpc_server)
{
    rpc_server_ = rpc_server;

    return this;
}

template <typename reader_key, typename reader_value>
Imagine_Rpc::RpcServer* RecordReader<reader_key, reader_value>::GetServer() const
{
    return rpc_server_;
}

} // namespace Imagine_MapReduce

#endif