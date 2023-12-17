#ifndef IMAGINE_MAPREDUCE_TEXTOUTPUTFORMAT_H
#define IMAGINE_MAPREDUCE_TEXTOUTPUTFORMAT_H

#include "OutputFormat.h"
#include "MapReduceUtil.h"

#include <string>
#include <memory.h>

namespace Imagine_MapReduce
{

class TextOutputFormat : public OutputFormat<std::string, int>
{
 public:
    TextOutputFormat();

    ~TextOutputFormat();

    std::pair<char *, char *> ToString(const std::pair<std::string, int>& content) const;
};

} // namespace Imagine_MapReduce

#endif