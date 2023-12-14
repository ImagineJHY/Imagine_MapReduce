#ifndef IMAGINE_MAPREDUCE_LINERECORDREADER_H
#define IMAGINE_MAPREDUCE_LINERECORDREADER_H

#include "RecordReader.h"

#include <fcntl.h>
#include <unistd.h>

namespace Imagine_MapReduce
{

class LineRecordReader : public RecordReader<int, std::string>
{
 public:
    LineRecordReader(InputSplit *split = nullptr, int split_id = 0);

    ~LineRecordReader();

    bool NextKeyValue();

    int GetCurrentKey() const;

    std::string GetCurrentValue() const;

    // 获取当前运行进展
    double GetProgress() const;

    // 关闭RecordReader
    void Close();

    std::shared_ptr<RecordReader<int, std::string>> CreateRecordReader(InputSplit *split, int split_id) const;

    bool ReadLine();

 protected:
    int offset_;
    std::string line_text_;
};

} // namespace Imagine_MapReduce

#endif