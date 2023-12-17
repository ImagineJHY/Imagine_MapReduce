#ifndef IMAGINE_MAPREDUCE_COMMON_TYPENAME_H
#define IMAGINE_MAPREDUCE_COMMON_TYPENAME_H

#include "Imagine_Log/Imagine_Log.h"

#include <functional>

namespace Imagine_Rpc
{

class Stub;

} // namespace Imagine_Rpc

namespace Imagine_MapReduce
{

template <typename reader_key, typename reader_value>
class RecordReader;

// map函数
template <typename reader_key, typename reader_value, typename key, typename value>
using MapCallback = std::function<std::pair<key, value>(reader_key, reader_value)>;

// reduce函数
using ReduceCallback = std::function<void(const std::string &)>;

// mapper定时向master发送进度的回调函数
template <typename reader_key, typename reader_value>
using MapTimerCallback = std::function<void(std::shared_ptr<Imagine_Rpc::Stub>&, std::shared_ptr<RecordReader<reader_key, reader_value>>&)>;

using Logger = ::Imagine_Tool::Imagine_Log::Logger;
using SingletonLogger = ::Imagine_Tool::Imagine_Log::SingletonLogger;
using NonSingletonLogger = ::Imagine_Tool::Imagine_Log::NonSingletonLogger;

} // namespace Imagine_MapReduce


#endif