#include "src/recover_worker.h"

#include "certain/errors.h"
#include "entity_worker.h"
#include "src/wrapper.h"
#include "utils/log.h"

namespace certain {

int RecoverWorker::GoToRecoverReqQueue(std::unique_ptr<PaxosCmd>& pcmd) {
  AsyncQueueMng* queue_mng = AsyncQueueMng::GetInstance();

  auto queue = queue_mng->GetRecoverReqQueueByEntityId(pcmd->entity_id());
  int ret = queue->PushByMultiThread(&pcmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportRecoverReqQueueFail();
    CERTAIN_LOG_ERROR("push cmd to recover_req_queue_ failed ret %d", ret);
  }
  return ret;
}

void RecoverWorker::GoToRecoverRspQueue(std::unique_ptr<PaxosCmd>& pcmd) {
  AsyncQueueMng* queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetRecoverRspQueueByEntityId(pcmd->entity_id());

  while (true) {
    int ret = queue->PushByMultiThread(&pcmd);
    if (ret == 0) {
      break;
    }
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportRecoverRspQueueFail();
    CERTAIN_LOG_ERROR("push cmd to recover_rsp_queue_ failed ret %d", ret);
    // The pcmd must response to entityworker, wait and retry.
    poll(nullptr, 0, 1);
  }
}

std::unique_ptr<PaxosCmd> RecoverWorker::GetJob() {
  std::unique_ptr<PaxosCmd> cmd;
  while (recover_req_queue_->PopByOneThread(&cmd) == 0) {
    if (doing_.count(cmd->entity_id())) {
      continue;
    }
    doing_.insert(cmd->entity_id());
    return cmd;
  }
  return nullptr;
}

void RecoverWorker::DoJob(std::unique_ptr<PaxosCmd> cmd) {
  // traffic limiter
  uint64_t sleep_ms = 0;
  do {
    sleep_ms = limiter_.UseCount();
    poll(nullptr, 0, sleep_ms);
  } while (sleep_ms);

  auto db = Wrapper::GetInstance()->GetDbImpl();
  uint64_t entity_id = cmd->entity_id();

  uint64_t max_committed_entry = 0;
  int ret = db->SnapshotRecover(entity_id, cmd->peer_acceptor_id(),
                                &max_committed_entry);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("EntityId %lu SnapshotRecover iRet %d", entity_id, ret);
  }

  cmd->set_result(ret);
  cmd->set_entry(max_committed_entry);

  CERTAIN_LOG_INFO("EntityId %lu AcceptorId %u MaxCommittedEntry %lu Ret %d",
                   entity_id, cmd->peer_acceptor_id(), max_committed_entry,
                   ret);

  GoToRecoverRspQueue(cmd);
  doing_.erase(entity_id);
}

}  // namespace certain
