#pragma once

#include "proto/tools.pb.h"

namespace certain {

class ToolsServiceImpl : public ToolsService {
  void DumpEntry(google::protobuf::RpcController* controller,
                 const DumpEntryReq* req, DumpEntryRsp* rsp,
                 google::protobuf::Closure* done) override;
};

}  // namespace certain
