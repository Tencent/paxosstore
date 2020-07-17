#include "src/catchup_worker.h"

#include "src/async_queue_mng.h"
#include "src/msg_worker.h"

namespace certain {

int CatchupWorker::GoToCatchupReqQueue(std::unique_ptr<PaxosCmd> pcmd) {
  auto queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetCatchupReqQueueByEntityId(pcmd->entity_id());
  int ret = queue->PushByMultiThread(&pcmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportCatchupReqQueueFail();
    CERTAIN_LOG_ERROR("push cmd to catchup_req_queue_ failed ret %d", ret);
  }
  return ret;
}

std::unique_ptr<PaxosCmd> CatchupWorker::GetJob() {
  auto queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetCatchupReqQueueByIdx(worker_id_);

  std::unique_ptr<PaxosCmd> cmd;
  queue->PopByOneThread(&cmd);
  return cmd;
}

void CatchupWorker::DoJob(std::unique_ptr<PaxosCmd> job) {
  // traffic limiter
  uint64_t sleep_ms = 0;
  do {
    sleep_ms = limiter_.UseBytes(job->SerializedByteSize());
    if (sleep_ms > 0) {
      monitor_->ReportCatchupTotalFlowLimit();
    }
    poll(nullptr, 0, sleep_ms);
  } while (sleep_ms);
  do {
    sleep_ms = limiter_.UseCount();
    if (sleep_ms > 0) {
      monitor_->ReportCatchupTotalCountLimit();
    }
    poll(nullptr, 0, sleep_ms);
  } while (sleep_ms);

  MsgWorker::GoAndDeleteIfFailed(std::move(job));
}

}  // namespace certain
