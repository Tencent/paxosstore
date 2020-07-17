#include "src/db_limited_worker.h"

#include "certain/certain.h"
#include "certain/errors.h"
#include "co_routine.h"
#include "utils/co_lock.h"
#include "utils/log.h"

namespace certain {

int DbLimitedWorker::GoToDbLimitedReqQueue(std::unique_ptr<ClientCmd>& cmd) {
  auto queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetDbLimitedReqQueueByEntityId(cmd->entity_id());
  int ret = queue->PushByMultiThread(&cmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportDbLimitedReqQueueFail();
    CERTAIN_LOG_ERROR("push cmd to db_req_queue_ failed ret %d", ret);
    return ret;
  }
  return 0;
}

void DbLimitedWorker::Tick() {
  Tick::Run();

  std::unique_ptr<ClientCmd> cmd;
  while (db_req_queue_->PopByOneThread(&cmd) == 0) {
    auto entity_id = cmd->entity_id();
    auto it = hash_.find(entity_id);
    if (it == hash_.end()) {
      pending_.push_front(std::move(cmd));
      hash_[entity_id] = pending_.begin();
    } else if ((*it->second)->entry() < cmd->entry()) {
      (*it->second)->set_entry(cmd->entry());
    }
  }
}

std::unique_ptr<ClientCmd> DbLimitedWorker::GetJob() {
  if (pending_.empty()) {
    return nullptr;
  }

  auto cmd = std::move(pending_.back());
  pending_.pop_back();
  hash_.erase(cmd->entity_id());

  return cmd;
}

void DbLimitedWorker::DoJob(std::unique_ptr<ClientCmd> cmd) {
  uint64_t entity_id = cmd->entity_id();

  while (true) {
    DbEntityLock lock(db_, entity_id);

    uint64_t max_committed_entry = 0;
    Db::RecoverFlag flag = Db::kNormal;
    int ret = db_->GetStatus(entity_id, &max_committed_entry, &flag);
    if (ret != 0 && ret != certain::kImplDbNotFound) {
      CERTAIN_LOG_FATAL("E(%lu) DB GetStatus ret %d", entity_id, ret);
      return;
    }
    if (flag == Db::kRecover) {
      CERTAIN_LOG_INFO("E(%lu) DB in Recover", cmd->entity_id());
      return;
    }

    if (max_committed_entry >= cmd->entry()) {
      CERTAIN_LOG_ERROR(
          "E(%lu, %lu) DB Commit Outdated data (MaxApplyEntry: %lu)", entity_id,
          cmd->entry(), max_committed_entry);
      return;
    }

    uint64_t entry = max_committed_entry + 1;
    std::string write_value;
    if (entry == cmd->entry() && cmd->value().size()) {
      write_value = cmd->value();
    } else {
      int ret = Certain::GetWriteValue(entity_id, entry, &write_value);
      if (ret == kRetCodeNotChosen) {
        CERTAIN_LOG_FATAL("E(%lu, %lu) ret %d must be chosen but not",
                          entity_id, entry, ret);
      }
      if (ret != 0) {
        break;
      }
    }

    TimeDelta time_delta;
    ret = db_->Commit(entity_id, entry, write_value);
    monitor_->ReportDbLimitedCommitTimeCost(ret, time_delta.DeltaUsec());
    if (ret != 0) {
      CERTAIN_LOG_FATAL("E(%lu, %lu) DB Commit ret %d", entity_id, entry, ret);
      return;
    }

    if (entry >= cmd->entry()) {
      return;
    }

    lock.Unlock();

    // traffic limiter
    uint64_t sleep_ms = 0;
    do {
      sleep_ms = limiter_.UseBytes(write_value.size());
      poll(nullptr, 0, sleep_ms);
    } while (sleep_ms);
    do {
      sleep_ms = limiter_.UseCount();
      poll(nullptr, 0, sleep_ms);
    } while (sleep_ms);
  }
}

}  // namespace certain
