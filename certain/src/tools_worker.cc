#include "src/tools_worker.h"

#include "src/entity_worker.h"

namespace certain {
namespace {

auto queue_mng = AsyncQueueMng::GetInstance();

}  // namespace

int ToolsWorker::GoToToolsReqQueue(std::unique_ptr<ClientCmd>& pcmd) {
  auto queue = queue_mng->GetToolsReqQueueByEntityId(pcmd->entity_id());
  int ret = queue->PushByMultiThread(&pcmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportToolsReqQueueFail();
    CERTAIN_LOG_ERROR("push cmd to tools_req_queue_ failed ret %d", ret);
  }
  return ret;
}

std::unique_ptr<ClientCmd> ToolsWorker::GetJob() {
  std::unique_ptr<ClientCmd> cmd;
  queue_.PopByOneThread(&cmd);
  return cmd;
}

void ToolsWorker::DoJob(std::unique_ptr<ClientCmd> cmd) {
  if (cmd->cmd_id() == kCmdDumpEntry) {
    auto queue = queue_mng->GetUserReqQueueByEntityId(cmd->entity_id());
    while (queue->PushByMultiThread(&cmd) != 0) {
      auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
      monitor->ReportUserReqQueueFail();
      CERTAIN_LOG_ERROR("push cmd to user_req_queue_ failed, will retry");
      poll(nullptr, 0, 100);
    }
  }
}

}  // namespace certain
