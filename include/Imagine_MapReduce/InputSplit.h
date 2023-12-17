#ifndef IMAGINE_MAPREDUCE_INPUTSPLIT_H
#define IMAGINE_MAPREDUCE_INPUTSPLIT_H

#include <string>

namespace Imagine_MapReduce
{

class InputSplit
{
 public:
    InputSplit(const std::string &file_name, int offset, int length);

    ~InputSplit();

    int GetLength() const;

    std::string GetFileName() const;

    int GetOffset() const;

 private:
    std::string file_name_; // 文件名
    int offset_;            // 偏移量(起始位置)
    int length_;            // split大小
};

} // namespace Imagine_MapReduce

#endif