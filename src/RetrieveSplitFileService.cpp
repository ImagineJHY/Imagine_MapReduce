#include "Imagine_MapReduce/RetrieveSplitFileService.h"

#include "Imagine_MapReduce/log_macro.h"
#include "Imagine_MapReduce/common_macro.h"
#include "Imagine_MapReduce/RetrieveSplitFileMessage.pb.h"

namespace Imagine_MapReduce
{
namespace Internal
{

RetrieveSplitFileService::RetrieveSplitFileService() : Imagine_Rpc::Service(INTERNAL_RETRIEVE_SPLIT_FILE_SERVICE_NAME)
{
    Init();
}

RetrieveSplitFileService::~RetrieveSplitFileService()
{
    Init();
}

void RetrieveSplitFileService::Init()
{
    REGISTER_MEMBER_FUNCTION(INTERNAL_RETRIEVE_SPLIT_FILE_METHOD_NAME, RetrieveSplitFileRequestMessage, RetrieveSplitFileResponseMessage, &RetrieveSplitFileService::RetrieveSplitFileProcessor);
}

Imagine_Rpc::Status RetrieveSplitFileService::RetrieveSplitFileProcessor(Imagine_Rpc::Context* context, RetrieveSplitFileRequestMessage* request_msg, RetrieveSplitFileResponseMessage* response_msg)
{
    std::string content;
    IMAGINE_MAPREDUCE_LOG("Retrieve file %s", request_msg->split_file_name_().c_str());
    int fd = open(request_msg->split_file_name_().c_str(), O_RDWR);
    while (1) {
        char buffer[1024];
        int ret = read(fd, buffer, 1024);
        for (int i = 0; i < ret; i++)
        {
            content.push_back(buffer[i]);
        }
        if (ret != 1024)
            break;
    }
    close(fd);
    if (content.size()) {
        response_msg->set_status_(Status::Ok);
        response_msg->set_split_file_content_(content);
    }

    return Imagine_Rpc::Status::OK;
}

} // namespace Internal
} // namespace Imagine_MapReducenamespace Internal
