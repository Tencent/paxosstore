#include "src/wrapper.h"

#include "certain/errors.h"
#include "src/async_queue_mng.h"
#include "src/catchup_worker.h"
#include "src/command.h"
#include "src/common.h"
#include "src/conn_mng.h"
#include "src/conn_worker.h"
#include "src/db_limited_worker.h"
#include "src/db_worker.h"
#include "src/entity_info_mng.h"
#include "src/entity_worker.h"
#include "src/libco_notify_helper.h"
#include "src/msg_worker.h"
#include "src/plog_worker.h"
#include "src/recover_worker.h"
#include "src/tools_worker.h"
#include "utils/memory.h"

namespace certain {
namespace {

auto queue_mng = AsyncQueueMng::GetInstance();
auto entity_info_group = EntityInfoGroup::GetInstance();
auto libco_notify_helper = LibcoNotifyHelper::GetInstance();

int PushClientCmd(std::unique_ptr<ClientCmd>& cmd) {
  AsyncQueue* queue = queue_mng->GetUserReqQueueByEntityId(cmd->entity_id());
  int ret = queue->PushByMultiThread(&cmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportUserReqQueueFail();
    CERTAIN_LOG_ERROR("queue %p PushByMultiThread ret %d", queue, ret);
    return ret;
  }
  return 0;
}

void TriggerClient(uint64_t entity_id, uint32_t cmd_id) {
  auto cmd = std::make_unique<ClientCmd>(cmd_id);
  cmd->set_entity_id(entity_id);
  PushClientCmd(cmd);
}

int SyncWait(ClientCmd* cmd) {
  auto ctx = libco_notify_helper->AllocContext();
  cmd->set_context(ctx.get());

  std::unique_ptr<ClientCmd> dummy(cmd);
  int ret = PushClientCmd(dummy);
  if (ret != 0) {
    dummy.release();
    return ret;
  }
  libco_notify_helper->Wait(ctx);
  return cmd->result();
}

}  // namespace

int Wrapper::Init(Options* options, Route* route_impl, Plog* plog_impl,
                  Db* db_impl) {
  // 1. Init log.
  // 2. Init route_impl_/plog_impl_/db_impl_.
  route_impl_ = route_impl;
  plog_impl_ = plog_impl;
  db_impl_ = db_impl;
  monitor_impl_ = options->monitor();
  Log::GetInstance()->Init(options->log(), monitor_impl_);

  // 2. Init options_.
  options_ = options;
  options_->set_local_addr(route_impl_->GetLocalAddr());

  int ret = InitManagers();
  if (ret != 0) {
    CERTAIN_LOG_ERROR("InitManagers ret %d", ret);
    return ret;
  }

  ret = InitWorkers();
  if (ret != 0) {
    CERTAIN_LOG_ERROR("InitWorkers ret %d", ret);
    return ret;
  }

  return 0;
}

int Wrapper::Write(const CmdOptions& cmd_options, uint64_t entity_id,
                   uint64_t entry, const std::string& value,
                   const std::vector<uint64_t>& uuids) {
  ClientCmd write(kCmdWrite);
  write.set_entity_id(entity_id);
  write.set_entry(entry);
  write.set_value(value);
  write.set_uuids(uuids);
  write.set_timeout_msec(cmd_options.client_cmd_timeout_msec());
  return SyncWait(&write);
}

int Wrapper::Read(const CmdOptions& cmd_options, uint64_t entity_id,
                  uint64_t entry) {
  ClientCmd read(kCmdRead);
  read.set_entity_id(entity_id);
  read.set_entry(entry);
  read.set_timeout_msec(cmd_options.client_cmd_timeout_msec());
  return SyncWait(&read);
}

int Wrapper::Replay(const CmdOptions& cmd_options, uint64_t entity_id,
                    uint64_t* entry) {
  *entry = 0;
  auto timeout_msec = cmd_options.client_cmd_timeout_msec() == 0
                          ? options_->client_cmd_timeout_msec()
                          : cmd_options.client_cmd_timeout_msec();
  auto deadline_msec = GetTimeByMsec() + timeout_msec;

  Db::RecoverFlag flags = Db::kNormal;
  uint64_t& max_committed_entry = *entry;

  int ret = db_impl_->GetStatus(entity_id, &max_committed_entry, &flags);
  if (ret != 0 && ret != certain::kImplDbNotFound) {
    CERTAIN_LOG_FATAL("db_impl_->GetStatus entity_id %lu ret %d", entity_id,
                      ret);
    return ret;
  }
  CERTAIN_LOG_INFO("GetStatus E(%lu, %lu) flags %u ret %d", entity_id,
                   max_committed_entry, flags, ret);

  if (flags == Db::kRecover) {
    // trigger recover and not wait
    TriggerClient(entity_id, kCmdRecover);
    return kRetCodeRecoverPending;
  }

  uint64_t max_chosen_entry = 0;
  uint64_t max_cont_chosen_entry = 0;
  ret = entity_info_group->GetMaxChosenEntry(entity_id, &max_chosen_entry,
                                             &max_cont_chosen_entry);
  if (ret != 0) {
    ClientCmd load(kCmdLoad);
    load.set_entity_id(entity_id);
    int ret = SyncWait(&load);
    if (ret != 0) {
      return ret;
    }
    if (entity_info_group->GetMaxChosenEntry(entity_id, &max_chosen_entry,
                                             &max_cont_chosen_entry) != 0) {
      return kRetCodeLoadEntityFailed;
    }
  }
  if (max_cont_chosen_entry < max_chosen_entry) {
    TriggerClient(entity_id, kCmdCatchup);
    return kRetCodeCatchupPending;
  }

  if (max_committed_entry + options_->max_replay_num() < max_chosen_entry) {
    auto cmd = std::make_unique<ClientCmd>(kCmdReplay);
    cmd->set_entity_id(entity_id);
    cmd->set_entry(max_chosen_entry);
    int ret = DbLimitedWorker::GoToDbLimitedReqQueue(cmd);
    if (ret != 0) {
      CERTAIN_LOG_ERROR("GoToDbLimitedReqQueue failed with %d", ret);
      return ret;
    }
    return kRetCodeReplayPending;
  }

  while (max_committed_entry < max_chosen_entry) {
    if (deadline_msec < GetTimeByMsec()) {
      return kRetCodeTimeout;
    }

    DbEntityLock lock(db_impl_, entity_id);
    int ret = db_impl_->GetStatus(entity_id, &max_committed_entry, &flags);
    if (ret != 0 && ret != certain::kImplDbNotFound) {
      CERTAIN_LOG_FATAL("db_impl_->GetStatus entity_id %lu ret %d", entity_id,
                        ret);
      return ret;
    }
    if (flags == Db::kRecover) {
      CERTAIN_LOG_INFO("E(%lu) DB in Recover", entity_id);
      return kRetCodeRecoverPending;
    }
    if (max_committed_entry >= max_chosen_entry) {
      break;
    }

    std::string write_value;
    ret = GetWriteValue(entity_id, max_committed_entry + 1, &write_value);
    if (ret != 0) {
      CERTAIN_LOG_FATAL("E(%lu, %lu) Plog GetWriteValue failed with %d",
                        entity_id, max_committed_entry + 1, ret);
      return ret;
    }

    TimeDelta time_delta;
    ret = db_impl_->Commit(entity_id, max_committed_entry + 1, write_value);
    monitor_impl_->ReportDbLimitedCommitTimeCost(ret, time_delta.DeltaUsec());
    if (ret != 0) {
      CERTAIN_LOG_FATAL("E(%lu, %lu) DB Replay Commit failed with %d",
                        entity_id, max_committed_entry + 1, ret);
      return ret;
    }
    ++max_committed_entry;
  }

  return 0;
}

int Wrapper::EvictEntity(uint64_t entity_id) { return 0; }

int Wrapper::GetWriteValue(uint64_t entity_id, uint64_t entry,
                           std::string* write_value) {
  std::string record_str;
  int ret = plog_impl_->GetRecord(entity_id, entry, &record_str);

  if (ret != 0) {
    if (ret != certain::kImplPlogNotFound) {
      CERTAIN_LOG_FATAL("Plog DB Get E(%lu, %lu) Failed with %d", entity_id,
                        entry, ret);
      return ret;
    }

    CERTAIN_LOG_INFO("E(%lu, %lu) not found", entity_id, entry);
    return certain::kRetCodeNotFound;
  }

  EntryRecord record;
  if (!record.ParseFromString(record_str)) {
    CERTAIN_LOG_FATAL("Parse Record Failed [%s]", record_str.c_str());
    return certain::kImplPlogGetErr;
  }

  write_value->swap(*record.mutable_value());
  if (!record.chosen()) {
    CERTAIN_LOG_INFO("Unchosen: %s", EntryRecordToString(record).c_str());
    return certain::kRetCodeNotChosen;
  }

  return 0;
}

void Wrapper::Destroy() {
  DestroyWorkers();
  DestroyManagers();
}

void Wrapper::StartWorkers() {
  for (auto& worker : workers_) {
    worker->Start();
  }
}

void Wrapper::StopWorkers() {
  for (auto& worker : workers_) {
    worker->set_exit_flag(true);
    worker->WaitExit();
  }
}

int Wrapper::InitWorkers() {
  workers_.clear();

  workers_.push_back(std::make_unique<ConnWorker>(options_));

  for (uint32_t i = 0; i < options_->msg_worker_num(); ++i) {
    workers_.push_back(std::make_unique<MsgWorker>(options_, i));
  }

  for (uint32_t i = 0; i < options_->entity_worker_num(); ++i) {
    workers_.push_back(std::make_unique<EntityWorker>(options_, i));
  }

  for (uint32_t i = 0; i < options_->plog_worker_num(); ++i) {
    workers_.push_back(
        std::make_unique<PlogWorker>(options_, i, plog_impl_, db_impl_));
  }

  for (uint32_t i = 0; i < options_->plog_readonly_worker_num(); ++i) {
    workers_.push_back(std::make_unique<PlogReadonlyWorker>(
        options_, i, plog_impl_, db_impl_));
  }

  for (uint32_t i = 0; i < options_->db_worker_num(); ++i) {
    workers_.push_back(std::make_unique<DbWorker>(options_, i, db_impl_));
  }

  for (uint32_t i = 0; i < options_->db_limited_worker_num(); ++i) {
    workers_.push_back(
        std::make_unique<DbLimitedWorker>(options_, i, db_impl_));
  }

  for (uint32_t i = 0; i < options_->recover_worker_num(); ++i) {
    workers_.push_back(std::make_unique<RecoverWorker>(options_, i));
  }

  for (uint32_t i = 0; i < options_->catchup_worker_num(); ++i) {
    workers_.push_back(std::make_unique<CatchupWorker>(options_, i));
  }

  for (uint32_t i = 0; i < options_->tools_worker_num(); ++i) {
    workers_.push_back(std::make_unique<ToolsWorker>(options_, i));
  }

  return 0;
}

int Wrapper::InitManagers() {
  int ret;

  ret = ConnMng::GetInstance()->Init(options_->local_addr(),
                                     options_->msg_worker_num());
  if (ret != 0) {
    CERTAIN_LOG_ERROR("ConnMng::GetInstance()->Init ret %d", ret);
    return ret;
  }

  ret = AsyncQueueMng::GetInstance()->Init(options_);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("AsyncQueueMng::GetInstance()->Init ret %d", ret);
    return ret;
  }

  ret = NotifyHelper::GetInstance()->Init(options_);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("NotifyHelper::GetInstance()->Init ret %d", ret);
    return ret;
  }

  ret = LibcoNotifyHelper::GetInstance()->Init(options_);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("LibcoNotifyHelper::GetInstance()->Init ret %d", ret);
    return ret;
  }

  return 0;
}

void Wrapper::DestroyManagers() {}

void Wrapper::DestroyWorkers() {}

void Wrapper::Run() {
  StartWorkers();
  started_ = true;

  uint64_t count = 0;
  while (!ThreadBase::exit_flag()) {
    usleep(100000);

    // Every 10s.
    if (count % 100 == 0) {
      AsyncQueueMng::GetInstance()->LogAllQueueStat();
      EntityWorker::LogMemoryUsage();
    }

    // Every 100ms.
    Log::GetInstance()->Flush();
    count++;
  }
}

void Wrapper::WaitExit() {
  ThreadBase::WaitExit();
  StopWorkers();
}

}  // namespace certain
