#ifndef IMAGINE_MAPREDUCE_LINERECORDREADER_H
#define IMAGINE_MAPREDUCE_LINERECORDREADER_H

#include "RecordReader.h"

#include <unistd.h>
#include <fcntl.h>

namespace Imagine_MapReduce
{

// template<typename key,typename value>
class LineRecordReader : public RecordReader<int, std::string>
{
 public:
    LineRecordReader(InputSplit *split = nullptr, int split_id = 0);

    ~LineRecordReader() {};

    bool NextKeyValue();

    int GetCurrentKey();

    std::string GetCurrentValue();

    // 获取当前运行进展
    double GetProgress();

    // 关闭RecordReader
    void Close();

    std::shared_ptr<RecordReader<int, std::string>> CreateRecordReader(InputSplit *split, int split_id);

    bool ReadLine();

 protected:
    int offset_;
    std::string line_text_;
};

// template<typename key,typename value>
LineRecordReader::LineRecordReader(InputSplit *split, int split_id) : RecordReader(split, split_id)
{
    if (split_) {
        offset_ = split->GetOffset();
        if (offset_) {
            ReadLine();
        }
    }
}

// template<typename key,typename value>
bool LineRecordReader::NextKeyValue()
{
    offset_ += line_text_.size(); // 下一句的起始位置
    if (offset_ >= split_->GetLength() + split_->GetOffset()) {
        return false; // lenth+split是下一段的起始位置
    }
    line_text_.clear();

    return ReadLine();
}

// template<typename key,typename value>
int LineRecordReader::GetCurrentKey()
{
    return offset_;
}

// template<typename key,typename value>
std::string LineRecordReader::GetCurrentValue()
{
    return line_text_.substr(0, line_text_.size() - 2);
}

// template<typename key,typename value>
double LineRecordReader::GetProgress()
{
    return (offset_ - split_->GetOffset()) * 1.0 / split_->GetLength();
}

// template<typename key,typename value>
void LineRecordReader::Close() {}

std::shared_ptr<RecordReader<int, std::string>> LineRecordReader::CreateRecordReader(InputSplit *split, int split_id)
{
    return std::make_shared<LineRecordReader>(split, split_id);
}

bool LineRecordReader::ReadLine()
{
    int fd = open(&split_->GetFileName()[0], O_RDWR);
    lseek(fd, offset_, SEEK_SET);
    bool flag = false;
    while (1) {
        char c;
        int ret = read(fd, &c, 1);
        if (ret) {
            line_text_.push_back(c);
            if (flag && c == '\n') {
                break;
            }
            if (c == '\r') {
                flag = true;
            } else {
                flag = false;
            }
        } else { // 读完了,文件不以\r\n结尾
            line_text_ += "\r\n";
            break;
        }
    }
    close(fd);

    return true;
}

} // namespace Imagine_MapReduce

#endif