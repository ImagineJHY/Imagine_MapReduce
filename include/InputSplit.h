#ifndef IMAGINE_MAPREDUCE_INPUTSPLIT_H
#define IMAGINE_MAPREDUCE_INPUTSPLIT_H

#include <string>

namespace Imagine_MapReduce
{

class InputSplit
{
 public:
    InputSplit(const std::string &file_name, int offset, int length) : file_name_(file_name), offset_(offset), length_(length) {}

    ~InputSplit(){};

    int GetLength() { return length_; }

    void GetLocations();

    void GetLocationInfo();

    std::string GetFileName() { return file_name_; }

    int GetOffset() { return offset_; }

 private:
    std::string file_name_; // 文件名
    int offset_;            // 偏移量(起始位置)
    int length_;            // split大小
};

} // namespace Imagine_MapReduce

#endif