#include "Imagine_MapReduce/InputSplit.h"

namespace Imagine_MapReduce
{

InputSplit::InputSplit(const std::string &file_name, int offset, int length) : file_name_(file_name), offset_(offset), length_(length)
{
}

InputSplit::~InputSplit()
{
}

int InputSplit::GetLength() const
{
    return length_;
}

std::string InputSplit::GetFileName() const
{
    return file_name_;
}

int InputSplit::GetOffset() const
{
    return offset_;
}

} // namespace Imagine_MapReduce
