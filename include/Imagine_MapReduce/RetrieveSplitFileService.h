#ifndef IMAGINE_MAPREDUCE_RETRIEVE_SPLIT_FILE_SERVICE_H
#define IMAGINE_MAPREDUCE_RETRIEVE_SPLIT_FILE_SERVICE_H

#include "Imagine_Rpc/Service.h"

namespace Imagine_MapReduce
{
namespace Internal
{

class RetrieveSplitFileRequestMessage;
class RetrieveSplitFileResponseMessage;

class RetrieveSplitFileService : public Imagine_Rpc::Service
{
 public:
    RetrieveSplitFileService();

    ~RetrieveSplitFileService();

    void Init();

    Imagine_Rpc::Status RetrieveSplitFileProcessor(Imagine_Rpc::Context* context, RetrieveSplitFileRequestMessage* request_msg, RetrieveSplitFileResponseMessage* response_msg);
};

} // namespace Internal
} // namespace Imagine_MapReducenamespace Internal

#endif