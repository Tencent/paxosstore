#include "tools/tools_service.h"

#include "src/async_queue_mng.h"
#include "src/command.h"
#include "tiny_rpc/tiny_client.h"
#include "utils/memory.h"

namespace certain {

void ToolsServiceImpl::DumpEntry(google::protobuf::RpcController* ctrl_base,
                                 const DumpEntryReq* req, DumpEntryRsp* rsp,
                                 google::protobuf::Closure* done) {
  auto& controller = *static_cast<certain::TinyController*>(ctrl_base);

  auto queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetToolsReqQueueByEntityId(req->entity_id());

  auto cmd = std::make_unique<ClientCmd>(kCmdDumpEntry);
  cmd->set_entity_id(req->entity_id());
  cmd->set_entry(req->entry());
  int ret = queue->PushByMultiThread(&cmd);
  if (ret != 0) {
    return controller.SetRetCode(ret);
  }
}

}  // namespace certain
