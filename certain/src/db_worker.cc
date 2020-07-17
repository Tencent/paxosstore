#include "src/db_worker.h"

#include "certain/errors.h"
#include "db_limited_worker.h"
#include "utils/log.h"

namespace certain {

int DbWorker::GoToDbReqQueue(std::unique_ptr<ClientCmd>& cmd) {
  AsyncQueueMng* queue_mng = AsyncQueueMng::GetInstance();

  auto queue = queue_mng->GetDbReqQueueByEntityId(cmd->entity_id());
  int ret = queue->PushByMultiThread(&cmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportDbReqQueueFail();
    CERTAIN_LOG_ERROR("push cmd to db_req_queue_ failed ret %d", ret);
    return ret;
  }
  return 0;
}

std::unique_ptr<ClientCmd> DbWorker::GetJob() {
  std::unique_ptr<ClientCmd> cmd;
  db_req_queue_->PopByOneThread(&cmd);
  return cmd;
}

void DbWorker::DoJob(std::unique_ptr<ClientCmd> cmd) {
  DbEntityLock lock(db_, cmd->entity_id());

  uint64_t max_committed_entry = 0;
  Db::RecoverFlag flag = Db::kNormal;
  int ret = db_->GetStatus(cmd->entity_id(), &max_committed_entry, &flag);
  if (ret != 0 && ret != certain::kImplDbNotFound) {
    CERTAIN_LOG_FATAL("E(%lu) DB GetStatus ret %d", cmd->entity_id(), ret);
    return;
  }
  if (flag == Db::kRecover) {
    CERTAIN_LOG_FATAL("E(%lu) DB in Recover", cmd->entity_id());
    return;
  }

  CERTAIN_LOG_INFO("E(%lu, %lu) max_committed_entry %lu value.size %lu",
                   cmd->entity_id(), cmd->entry(), max_committed_entry,
                   cmd->value().size());

  if (cmd->entry() < max_committed_entry + 1) {
    CERTAIN_LOG_ERROR(
        "E(%lu, %lu) DB Commit Outdated data (MaxApplyEntry: %lu)",
        cmd->entity_id(), cmd->entry(), max_committed_entry);
    return;
  } else if (cmd->entry() > max_committed_entry + 1) {
    DbLimitedWorker::GoToDbLimitedReqQueue(cmd);
    return;
  }

  // cmd->entry() == max_committed_entry + 1
  TimeDelta time_delta;
  ret = db_->Commit(cmd->entity_id(), cmd->entry(), cmd->value());
  monitor_->ReportDbCommitTimeCost(ret, time_delta.DeltaUsec());
  if (ret != 0) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) DB Commit ret %d", cmd->entity_id(),
                      cmd->entry(), ret);
    return;
  }
}

}  // namespace certain
