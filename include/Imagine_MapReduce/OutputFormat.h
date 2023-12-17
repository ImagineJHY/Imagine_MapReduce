#ifndef IMAGINE_MAPREDUCE_OUTPUTFORMAT_H
#define IMAGINE_MAPREDUCE_OUTPUTFORMAT_H

#include <memory>

namespace Imagine_MapReduce
{

template <typename key, typename value>
class OutputFormat
{
 public:
    OutputFormat();

    virtual ~OutputFormat();

    virtual std::pair<char *, char *> ToString(const std::pair<key, value>& content) const = 0;
};

template <typename key, typename value>
OutputFormat<key, value>::OutputFormat()
{
}

template <typename key, typename value>
OutputFormat<key, value>::~OutputFormat()
{
}

} // namespace Imagine_MapReduce

#endif