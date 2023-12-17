#include "Imagine_MapReduce/LineRecordReader.h"

namespace Imagine_MapReduce
{

LineRecordReader::LineRecordReader(InputSplit *split, int split_id) : RecordReader(split, split_id)
{
    if (split_) {
        offset_ = split->GetOffset();
        if (offset_) {
            ReadLine();
        }
    }
}

LineRecordReader::~LineRecordReader()
{
}

bool LineRecordReader::NextKeyValue()
{
    offset_ += line_text_.size(); // 下一句的起始位置
    if (offset_ >= split_->GetLength() + split_->GetOffset()) {
        return false; // lenth+split是下一段的起始位置
    }
    line_text_.clear();

    return ReadLine();
}

int LineRecordReader::GetCurrentKey() const
{
    return offset_;
}

std::string LineRecordReader::GetCurrentValue() const
{
    return line_text_.substr(0, line_text_.size() - 2);
}

double LineRecordReader::GetProgress() const
{
    return (offset_ - split_->GetOffset()) * 1.0 / split_->GetLength();
}

void LineRecordReader::Close()
{
}

std::shared_ptr<RecordReader<int, std::string>> LineRecordReader::CreateRecordReader(InputSplit *split, int split_id) const
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
