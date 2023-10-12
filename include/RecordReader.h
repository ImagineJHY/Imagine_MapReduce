#ifndef IMAGINE_MAPREDUCE_RECORDREADER_H
#define IMAGINE_MAPREDUCE_RECORDREADER_H

#include "InputSplit.h"

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value>
class RecordReader
{
 public:
    RecordReader(InputSplit *split = nullptr, int split_id = 0) : split_id_(split_id), file_name_(split != nullptr ? split->GetFileName() : ""), split_(split) {}

    virtual ~RecordReader()
    {
        if (split_) {
            delete split_;
        }
        printf("delete recordreader!\n");
    }

    virtual bool NextKeyValue() = 0;

    virtual reader_key GetCurrentKey() = 0;

    virtual reader_value GetCurrentValue() = 0;

    // 获取当前运行进展
    virtual double GetProgress() = 0;

    // 关闭RecordReader
    virtual void Close() = 0;

    // 创建自己类型返回
    virtual std::shared_ptr<RecordReader<reader_key, reader_value>> CreateRecordReader(InputSplit *split = nullptr, int split_id = 0) = 0;

    void SetOutputFileName(const std::string &name)
    {
        file_name_ = name;
    }

    int GetSplitId() { return split_id_; }

    std::string GetFileName()
    {
        return file_name_;
    }

 protected:
    const int split_id_;
    const std::string file_name_; // 输出文件名

    InputSplit *split_;
};

} // namespace Imagine_MapReduce

#endif