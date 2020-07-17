#include "src/entity_helper.h"

#include "certain/errors.h"
#include "src/catchup_worker.h"
#include "src/db_worker.h"
#include "src/plog_worker.h"
#include "src/recover_worker.h"
#include "utils/memory.h"

namespace certain {
namespace {

inline void UpdateIfLessThan(uint64_t& src, uint64_t dest) {
  if (src < dest) {
    src = dest;
  }
}

}  // namespace

bool EntityHelper::CheckIfHasWork() {
  int ret;
  bool has_work = false;

  // 1. Move cmd from finished_list_ to user_rsp_queue_.
  while (!finished_list_.empty()) {
    ret = user_rsp_queue_->PushByMultiThread(&finished_list_.front());
    if (ret != 0) {
      monitor_->ReportUserRspQueueFail();
      CERTAIN_LOG_ERROR("push queue failed: %s",
                        finished_list_.front()->ToString().c_str());
      break;
    }
    finished_list_.pop_front();
    has_work = true;
  }

  // 2. Move timeout cmd from timer to user_rsp_queue_.
  while (true) {
    auto entity_info = timer_->TakeTimerElt();
    if (entity_info == nullptr) {
      break;
    }

    auto client_cmd = std::move(entity_info->client_cmd);
    if (client_cmd == nullptr) {
      CERTAIN_LOG_FATAL("entity_id %lu cmd not found", entity_info->entity_id);
      continue;
    }

    client_cmd->set_result(kRetCodeTimeout);
    CERTAIN_LOG_ERROR("Timeout cmd: %s", client_cmd->ToString().c_str());

    ret = user_rsp_queue_->PushByMultiThread(&client_cmd);
    if (ret != 0) {
      monitor_->ReportUserRspQueueFail();
      CERTAIN_LOG_ERROR("push queue failed: %s",
                        client_cmd->ToString().c_str());
      finished_list_.push_back(std::move(client_cmd));
    }
    has_work = true;
  }

  if (entry_info_mng_->CleanupExpiredChosenEntry()) {
    has_work = true;
  }

  IterateAndCatchup();

  return has_work;
}

void EntityHelper::IterateAndCatchup() {
  // Iterate one entity.
  auto entity_info = entity_info_mng_->NextEntityInfo();
  if (entity_info == nullptr) {
    return;
  }
  TryCatchup(entity_info, __LINE__);
}

void EntityHelper::TryCatchup(EntityInfo* entity_info, int line) {
  uint64_t max_cont_chosen_entry = entity_info->max_cont_chosen_entry;
  uint64_t dest_entry = entity_info->max_chosen_entry;

  assert(max_cont_chosen_entry <= dest_entry);
  if (max_cont_chosen_entry == dest_entry) {
    return;
  }
  if (entity_info->recover_pending) {
    return;
  }

  if (dest_entry > max_cont_chosen_entry + options_->max_catchup_num()) {
    dest_entry = max_cont_chosen_entry + options_->max_catchup_num();
  }
  if (max_cont_chosen_entry < entity_info->max_catchup_entry) {
    dest_entry = entity_info->max_catchup_entry;
  }

  CERTAIN_LOG_INFO("entity_info: %s dest_entry %lu line %d",
                   ToString(entity_info).c_str(), dest_entry, line);
  uint64_t ready_to_catchup = dest_entry - entity_info->max_catchup_entry;
  if (ready_to_catchup) {
    monitor_->ReportReadyToCatchup(ready_to_catchup);
  }

  entity_info->max_catchup_entry = dest_entry;
  uint64_t entity_id = entity_info->entity_id;
  uint64_t now_msec = GetTimeByMsec();
  for (uint64_t curr = max_cont_chosen_entry + 1; curr <= dest_entry; ++curr) {
    // Catchup routine:
    // 1. Get entry_info from plog. (May skip this step if in the memory.)
    // 2. SyncToPeer. (May skip this step if the entry is chosen.)
    // 3. Set entry_info to plog. (May skip this step if step2 is skipped.)
    // 4. Commit to db asynchronously.
    // Count limit for Step 1 and 2. And break catchup routine once some entry
    // is limited.
    auto info = entry_info_mng_->FindEntryInfo(entity_id, curr);
    if (info == nullptr) {
      if (!catchup_get_count_limiter_->AcquireOne()) {
        // Limit for Step1.
        CERTAIN_LOG_ERROR("E(%lu, %lu) dest_entry %lu catchup get count limit",
                          entity_id, curr, dest_entry);
        monitor_->ReportCatchupGetLimit();
        break;
      }
      monitor_->ReportGetForCatchup();
      int ret = CreateEntryInfoIfMiss(entity_info, curr, info);
      if (ret != 0) {
        CERTAIN_LOG_ERROR("CreateEntryInfoIfMiss E(%lu, %lu) ret %d", entity_id,
                          curr, ret);
        break;
      }
    }
    if (info->uncertain) {
      continue;
    }
    if (info->machine->entry_state() == EntryState::kChosen) {
      monitor_->ReportAyncCommitForCatchup();
      UpdateByChosenEntry(info);
      continue;
    }
    CERTAIN_LOG_DEBUG("E(%lu, %lu) catchup_timestamp_msec %lu now_msec %lu",
                      entity_id, curr, info->catchup_timestamp_msec, now_msec);
    if (info->catchup_timestamp_msec > now_msec) {
      continue;
    }
    // Limit for Step2.
    if (!catchup_sync_count_limiter_->AcquireOne()) {
      CERTAIN_LOG_ERROR("E(%lu, %lu) dest_entry %lu catchup peer count limit",
                        entity_id, curr, dest_entry);
      monitor_->ReportCatchupSyncLimit();
      break;
    }
    info->catchup_times++;
    monitor_->ReportCatchupTimes(info->catchup_times);
    monitor_->ReportSyncForCatchup();
    CatchupToPeer(info);
    UpdateCatchupTimestamp(info);
  }
}

void EntityHelper::CatchupToPeer(EntryInfo* info) {
  SyncToPeer(info, info->entity_info->active_peer_acceptor_id, false, true);
}

void EntityHelper::UpdateCatchupTimestamp(EntryInfo* info) {
  info->catchup_timestamp_msec =
      GetTimeByMsec() + options_->catchup_timeout_msec();
}

int EntityHelper::TriggerRecover(EntityInfo* entity_info) {
  assert(entity_info != nullptr);
  uint64_t entity_id = entity_info->entity_id;

  if (entity_info->recover_pending) {
    CERTAIN_LOG_INFO("entity %lu is recovering", entity_id);
    return kRetCodeEntityLoading;
  }

  const uint64_t kRecoverIntervalMsec = 60 * 1000;  // ignore in 1min
  if (entity_info->recover_timestamp_msec + kRecoverIntervalMsec >=
      GetTimeByMsec()) {
    return 0;
  }

  auto cmd = std::make_unique<PaxosCmd>();
  cmd->set_entity_id(entity_id);
  cmd->set_timestamp_usec(GetTimeByUsec());
  int ret = RecoverWorker::GoToRecoverReqQueue(cmd);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("GoToRecoverReqQueue entity %lu ret %d", entity_id, ret);
    return ret;
  }

  CERTAIN_LOG_INFO("trigger recover for entity %lu", entity_id);
  entity_info->recover_pending = true;
  return 0;
}

int EntityHelper::HandleClientCmd(std::unique_ptr<ClientCmd>& client_cmd) {
  uint64_t entity_id = client_cmd->entity_id();
  uint64_t entry = client_cmd->entry();
  CERTAIN_LOG_INFO("cmd: %s", client_cmd->ToString().c_str());

  EntityInfo* entity_info = nullptr;
  int ret = CreateEntityInfoIfMiss(entity_id, entity_info);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("cmd: %s ret %d", client_cmd->ToString().c_str(), ret);
    FinishClientCmd(client_cmd, ret);
    return ret;
  }

  if (client_cmd->cmd_id() == kCmdCatchup) {
    if (entity_info->loading) {
      return kRetCodeEntityLoading;
    }
    TryCatchup(entity_info, __LINE__);
    return 0;
  }
  if (client_cmd->cmd_id() == kCmdRecover) {
    return TriggerRecover(entity_info);
  }
  if (client_cmd->cmd_id() == kCmdDumpEntry) {
    DumpEntry(std::move(client_cmd));
    return 0;
  }

  if (entity_info->client_cmd != nullptr) {
    CERTAIN_LOG_ERROR("cmd: %s", entity_info->client_cmd->ToString().c_str());
    FinishClientCmd(client_cmd, kRetCodeClientCmdConflict);
    return kRetCodeClientCmdConflict;
  }

  auto elapsed_msec = GetTimeByMsec() - client_cmd->timestamp_usec() / 1000;
  auto timeout_msec = client_cmd->timeout_msec() == 0
                          ? options_->client_cmd_timeout_msec()
                          : client_cmd->timeout_msec();
  if (elapsed_msec > timeout_msec) {
    FinishClientCmd(client_cmd, kRetCodeTimeout);
    return kRetCodeTimeout;
  }

  entity_info->client_cmd = std::move(client_cmd);
  timer_->Add(entity_info, timeout_msec - elapsed_msec);
  auto& cmd = entity_info->client_cmd;

  // Handle the client_cmd when loading is finish.
  if (entity_info->loading) {
    CERTAIN_LOG_INFO("E(%lu, %lu) entity loading", entity_id, entry);
    return kRetCodeEntityLoading;
  }

  if (cmd->cmd_id() == kCmdLoad) {
    FinishClientCmd(entity_info, kRetCodeOk);
    return 0;
  }

  uint64_t matched_entry = entity_info->max_chosen_entry + 1;
  if (entry != matched_entry) {
    CERTAIN_LOG_ERROR("check if busy matched_entry %lu cmd: %s", matched_entry,
                      cmd->ToString().c_str());
    FinishClientCmd(entity_info, kRetCodeEntryNotMatch);
    TryCatchup(entity_info, __LINE__);
    return kRetCodeEntryNotMatch;
  }

  EntryInfo* info = nullptr;
  ret = CreateEntryInfoIfMiss(entity_info, entry, info);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("cmd: %s ret %d", cmd->ToString().c_str(), ret);
    FinishClientCmd(entity_info, ret);
    return ret;
  }

  if (info->uncertain) {
    CERTAIN_LOG_ERROR("cmd: %s uncertain", cmd->ToString().c_str());
    FinishClientCmd(entity_info, kRetCodeEntryUncertain);
    return kRetCodeEntryUncertain;
  }

  if (cmd->cmd_id() == kCmdWrite) {
    return HandleWriteCmd(info);
  } else if (cmd->cmd_id() == kCmdRead) {
    cmd->set_uuid(entity_info->uuid_base++);
    return HandleReadCmd(info);
  }
  return kRetCodeUnknown;
}

void EntityHelper::RestoreValue(EntryInfo* info,
                                std::unique_ptr<PaxosCmd>& pcmd) {
  auto& machine = info->machine;
  if (pcmd->local_entry_record().has_value_id_only()) {
    EntryRecord local = pcmd->local_entry_record();
    machine->RestoreValueInRecord(local);
    pcmd->set_local_entry_record(local);
  }
  if (pcmd->peer_entry_record().has_value_id_only()) {
    EntryRecord peer = pcmd->peer_entry_record();
    machine->RestoreValueInRecord(peer);
    pcmd->set_peer_entry_record(peer);
  }
}

int EntityHelper::HandlePaxosCmd(std::unique_ptr<PaxosCmd>& pcmd) {
  int ret;
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();

  EntityInfo* entity_info = nullptr;
  ret = CreateEntityInfoIfMiss(entity_id, entity_info);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("cmd: %s ret %d", pcmd->ToString().c_str(), ret);
    return ret;
  }

  // Switch to local view.
  pcmd->SwitchToLocalView(entity_info->local_acceptor_id);
  CERTAIN_LOG_INFO("switched cmd: %s", pcmd->ToString().c_str());

  // Handle the waiting msg when loading is finish.
  if (entity_info->loading) {
    // Store the last paxos cmd only.
    entity_info->waiting_msg = std::move(pcmd);
    return 0;
  }

  uint32_t local_acceptor_id = pcmd->local_acceptor_id();
  uint32_t peer_acceptor_id = pcmd->peer_acceptor_id();
  if (local_acceptor_id != entity_info->local_acceptor_id ||
      local_acceptor_id == peer_acceptor_id ||
      peer_acceptor_id >= options_->acceptor_num()) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) local %u peer %u parameter error", entity_id,
                      entry, local_acceptor_id, peer_acceptor_id);
    return kRetCodeParameterErr;
  }

  // Update max_chosen_entry.
  uint64_t max_chosen_entry = pcmd->max_chosen_entry();
  if (entity_info->max_chosen_entry < max_chosen_entry) {
    entity_info->active_peer_acceptor_id = peer_acceptor_id;
    uint64_t old = entity_info->max_chosen_entry;
    entity_info->max_chosen_entry = max_chosen_entry;
    CERTAIN_LOG_INFO("max_chosen_entry %lu -> %lu cmd: %s", old,
                     max_chosen_entry, pcmd->ToString().c_str());
  }

  if (pcmd->check_empty()) {
    // Fast fail for check empty routine.
    if (entry <= entity_info->max_chosen_entry) {
      pcmd->set_max_chosen_entry(entity_info->max_chosen_entry);
      pcmd->set_result(kRetCodeFastFailed);
      pcmd->set_check_empty(false);
      CERTAIN_LOG_INFO("fast fail cmd: %s", pcmd->ToString().c_str());
      MsgWorker::GoAndDeleteIfFailed(std::move(pcmd));
      return 0;
    }
  }

  if (pcmd->result() == kRetCodeFastFailed) {
    if (entity_info->client_cmd != nullptr) {
      if (entity_info->client_cmd->uuid() == pcmd->uuid() &&
          entity_info->client_cmd->entry() == pcmd->entry()) {
        FinishClientCmd(entity_info, kRetCodeFastFailed);
      }
    }

    TryCatchup(entity_info, __LINE__);
    return kRetCodeFastFailed;
  }

  if (pcmd->result() == kImplPlogNotFound) {
    return TriggerRecover(entity_info);
  }

  // Handle the case of local chosen.
  if (HandleIfLocalChosen(entity_info, pcmd)) {
    CERTAIN_LOG_INFO("E(%lu, %lu) local is chosen", entity_id, entry);
    return 0;
  }

  EntryInfo* info = nullptr;
  ret = CreateEntryInfoIfMiss(entity_info, entry, info);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("cmd: %s ret %d", pcmd->ToString().c_str(), ret);
    return ret;
  }

  RestoreValue(info, pcmd);

  if (info->uncertain) {
    CERTAIN_LOG_INFO("entry uncertain cmd: %s", pcmd->ToString().c_str());
    if (info->waiting_msgs[peer_acceptor_id] != nullptr) {
      CERTAIN_LOG_ERROR(
          "delete cmd: %s",
          info->waiting_msgs[peer_acceptor_id]->ToString().c_str());
    }
    info->waiting_msgs[peer_acceptor_id] = std::move(pcmd);
    entry_info_mng_->UpdateMemorySize(info);
    return 0;
  }

  assert(info->broadcast == false);
  assert(info->peer_to_sync == kInvalidAcceptorId);
  ret = UpdateMachineByPaxosCmd(info, pcmd);

  // Try catchup after update to avoid the entry runs into the catchup routine.
  TryCatchup(entity_info, __LINE__);

  return ret;
}

int EntityHelper::HandlePlogRspCmd(std::unique_ptr<PaxosCmd>& pcmd) {
  int ret = 0;
  if (pcmd->plog_load()) {
    ret = HandleLoadFromPlog(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("HandleLoadFromPlog cmd: %s ret %d",
                        pcmd->ToString().c_str(), ret);
    }
  } else if (pcmd->plog_set_record()) {
    ret = HandleSetFromPlog(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("HandleSetFromPlog cmd: %s ret %d",
                        pcmd->ToString().c_str(), ret);
    }
  } else if (pcmd->plog_get_record()) {
    ret = HandleGetFromPlog(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("HandleGetFromPlog cmd: %s ret %d",
                        pcmd->ToString().c_str(), ret);
    }
  } else {
    CERTAIN_LOG_FATAL("unknown cmd: %s", pcmd->ToString().c_str());
  }
  return ret;
}

int EntityHelper::HandleRecoverRspCmd(std::unique_ptr<PaxosCmd>& pcmd) {
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();
  uint64_t time_cost_us = GetTimeByUsec() - pcmd->timestamp_usec();
  monitor_->ReportRecoverTimeCost(pcmd->result(), time_cost_us);

  EntityInfo* entity_info = nullptr;
  int ret = CreateEntityInfoIfMiss(entity_id, entity_info);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("CreateEntityInfoIfMiss %lu fail with %d", entity_id,
                      ret);
    return ret;
  }

  entity_info->recover_pending = false;
  entity_info->recover_timestamp_msec = GetTimeByMsec();

  if (pcmd->result() != 0) {
    CERTAIN_LOG_ERROR("entity %lu recover failed with %d", entity_id,
                      pcmd->result());
    return pcmd->result();
  }
  CERTAIN_LOG_INFO("entity %lu recover finished!", entity_id);

  UpdateIfLessThan(entity_info->max_chosen_entry, entry);
  UpdateIfLessThan(entity_info->max_cont_chosen_entry, entry);
  UpdateIfLessThan(entity_info->max_catchup_entry,
                   entity_info->max_cont_chosen_entry);
  UpdateIfLessThan(entity_info->max_plog_entry, entry);
  TryCatchup(entity_info, __LINE__);
  return 0;
}

int EntityHelper::HandleWriteCmd(EntryInfo* info) {
  auto entity_info = info->entity_info;
  auto& client_cmd = entity_info->client_cmd;
  uint64_t entity_id = client_cmd->entity_id();
  uint64_t entry = client_cmd->entry();
  auto& machine = info->machine;

  bool pre_auth = false;
  if (options_->enable_pre_auth() &&
      entity_info->pre_auth_entry != kInvalidEntry) {
    pre_auth = (entity_info->pre_auth_entry + 1 == entry);
  }

  int ret = machine->Promise(pre_auth);
  if (ret != 0) {
    FinishClientCmd(entity_info, kRetCodeStatePromiseErr);
    CERTAIN_LOG_ERROR("E(%lu, %lu) Promise ret %d st: %s", entity_id, entry,
                      machine->ToString().c_str());
    return kRetCodeStatePromiseErr;
  }

  // Use local Promised number as value id, which is unique.
  client_cmd->set_value_id(machine->GetLocalPromisedNum());

  if (!machine->IsLocalAcceptable()) {
    if (!StoreEntryInfo(info)) {
      ClearEntryInfo(info);
      FinishClientCmd(entity_info, kRetCodeStoreEntryFailed);
      CERTAIN_LOG_ERROR("E(%lu, %lu) StoreEntryInfo failed", entity_id, entry);
      return kRetCodeStoreEntryFailed;
    }
    info->broadcast = true;
    return kRetCodeWaitBroadcast;
  }

  bool prepared_value_accepted = false;
  ret = machine->Accept(client_cmd->value(), client_cmd->value_id(),
                        client_cmd->uuids(), &prepared_value_accepted);
  if (ret != 0) {
    FinishClientCmd(entity_info, kRetCodeStateAcceptErr);
    CERTAIN_LOG_ERROR("E(%lu, %lu) Accept ret %d machine: %s", entity_id, entry,
                      machine->ToString().c_str());
    return kRetCodeStateAcceptErr;
  }

  assert(prepared_value_accepted);
  if (!StoreEntryInfo(info)) {
    ClearEntryInfo(info);
    FinishClientCmd(entity_info, kRetCodeStoreEntryFailed);
    CERTAIN_LOG_ERROR("E(%lu, %lu) StoreEntryInfo failed", entity_id, entry);
    return kRetCodeStoreEntryFailed;
  }
  info->broadcast = true;

  return kRetCodeWaitBroadcast;
}

int EntityHelper::HandleReadCmd(EntryInfo* info) {
  auto& machine = info->machine;
  if (!machine->IsLocalEmpty()) {
    monitor_->ReportWriteForRead();
    return HandleWriteCmd(info);
  }

  machine->ResetEmptyFlags();
  Broadcast(info, true);
  return kRetCodeWaitBroadcast;
}

void EntityHelper::FinishClientCmd(std::unique_ptr<ClientCmd>& client_cmd,
                                   int result) {
  client_cmd->set_result(result);
  int ret = user_rsp_queue_->PushByMultiThread(&client_cmd);
  if (ret != 0) {
    monitor_->ReportUserRspQueueFail();
    CERTAIN_LOG_ERROR("push queue failed: %s", client_cmd->ToString().c_str());
    finished_list_.push_back(std::move(client_cmd));
  }
}

void EntityHelper::FinishClientCmd(EntityInfo* entity_info, int result) {
  if (entity_info->client_cmd == nullptr) {
    return;
  }

  timer_->Remove(entity_info);
  auto client_cmd = std::move(entity_info->client_cmd);

  client_cmd->set_result(result);
  int ret = user_rsp_queue_->PushByMultiThread(&client_cmd);
  if (ret != 0) {
    monitor_->ReportUserRspQueueFail();
    CERTAIN_LOG_ERROR("push queue failed: %s", client_cmd->ToString().c_str());
    finished_list_.push_back(std::move(client_cmd));
  }
}

void EntityHelper::HandleWaitingMsg(EntryInfo* info) {
  for (uint32_t i = 0; i < options_->acceptor_num(); ++i) {
    auto pcmd = std::move(info->waiting_msgs[i]);
    if (pcmd == nullptr) {
      continue;
    }
    int ret = HandlePaxosCmd(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("cmd: %s ret %d", pcmd->ToString().c_str(), ret);
    }
    // After access from plog, this method will be called again.
    if (info->uncertain) {
      break;
    }
  }
  entry_info_mng_->UpdateMemorySize(info);
}

int EntityHelper::HandleGetFromPlog(std::unique_ptr<PaxosCmd>& pcmd) {
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();
  auto info = entry_info_mng_->FindEntryInfo(entity_id, entry);
  assert(info != nullptr);
  assert(info->uncertain);
  info->uncertain = false;

  uint64_t usetime_us = GetTimeByUsec() - pcmd->timestamp_usec();
  CERTAIN_LOG_INFO("E(%lu, %lu) usetime_us %lu", entity_id, entry, usetime_us);

  if (pcmd->result() == kImplPlogNotFound) {
    pcmd->set_local_entry_record(EntryRecord());
    pcmd->set_result(0);
  }
  if (pcmd->result() != 0) {
    ClearEntryInfo(info);
    CERTAIN_LOG_FATAL("cmd: %s", pcmd->ToString().c_str());
    return pcmd->result();
  }

  auto& machine = info->machine;
  int ret = machine->Update(info->entity_info->local_acceptor_id,
                            pcmd->local_entry_record());
  entry_info_mng_->UpdateMemorySize(info);
  if (ret != 0) {
    CERTAIN_LOG_FATAL("Update ret E(%lu, %lu) st: %s", entity_id, entry,
                      machine->ToString().c_str());
    return ret;
  }

  UpdateByChosenEntry(info);
  HandleWaitingMsg(info);
  TryCatchup(info->entity_info, __LINE__);
  return 0;
}

int EntityHelper::HandleSetFromPlog(std::unique_ptr<PaxosCmd>& pcmd) {
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();
  auto info = entry_info_mng_->FindEntryInfo(entity_id, entry);
  assert(info != nullptr);
  assert(info->uncertain);
  info->uncertain = false;

  uint64_t usetime_us = GetTimeByUsec() - pcmd->timestamp_usec();
  CERTAIN_LOG_INFO("E(%lu, %lu) usetime_us %lu", entity_id, entry, usetime_us);

  if (pcmd->result() != 0) {
    ClearEntryInfo(info);
    CERTAIN_LOG_FATAL("cmd: %s", pcmd->ToString().c_str());
    return pcmd->result();
  }

  UpdateByChosenEntry(info);
  if (info->machine->entry_state() == EntryState::kChosen) {
    uint32_t accepted_num = info->machine->GetLocalAcceptedNum();
    assert(accepted_num > 0);
    monitor_->ReportChosenProposalNum(accepted_num);
    auto entity_info = info->entity_info;
    auto& client_cmd = entity_info->client_cmd;
    const auto& local_record =
        info->machine->GetEntryRecord(entity_info->local_acceptor_id);

    if (client_cmd != nullptr && client_cmd->entry() == entry &&
        client_cmd->value_id() == local_record.value_id()) {
      FinishClientCmd(entity_info, kRetCodeOk);
    }
  }

  if (!info->broadcast && info->peer_to_sync == kInvalidAcceptorId) {
    return 0;
  }

  if (info->broadcast) {
    Broadcast(info);
  } else if (info->compensate_msgs) {
    if (info->machine->entry_state() == EntryState::kAcceptLocal) {
      BroadcastOnAccept(info);
    } else if (info->machine->entry_state() == EntryState::kChosen) {
      BroadcastOnChosen(info);
    } else {
      CERTAIN_LOG_FATAL("info: %s machine: %s", ToString(info).c_str(),
                        info->machine->ToString().c_str());
    }
  } else {
    SyncToPeer(info, info->peer_to_sync);
  }

  info->broadcast = false;
  info->compensate_msgs = false;
  info->peer_to_sync = kInvalidAcceptorId;

  HandleWaitingMsg(info);
  return 0;
}

int EntityHelper::HandleLoadFromPlog(std::unique_ptr<PaxosCmd>& pcmd) {
  int ret;
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();
  auto entity_info = entity_info_mng_->FindEntityInfo(entity_id);
  assert(entity_info != nullptr);
  assert(entity_info->loading);
  entity_info->loading = false;

  uint64_t usetime_us = GetTimeByUsec() - pcmd->timestamp_usec();
  CERTAIN_LOG_INFO("E(%lu, %lu) max_committed_entry %lu usetime_us %lu",
                   entity_id, entry, pcmd->max_committed_entry(), usetime_us);

  if (pcmd->result() != 0) {
    CERTAIN_LOG_ERROR("entity %lu load ret %d", entity_id, pcmd->result());
    FinishClientCmd(entity_info, pcmd->result());
    entity_info_mng_->DestroyEntityInfo(entity_info);
    return 0;
  }

  // Return max_plog_entry in pcmd->entry().
  UpdateByLoadFromPlog(entity_info, pcmd);

  if (entity_info->waiting_msg != nullptr) {
    auto cmd = std::move(entity_info->waiting_msg);
    ret = HandlePaxosCmd(cmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("cmd: %s ret %d", cmd->ToString().c_str(), ret);
    }
  }

  if (entity_info->client_cmd != nullptr) {
    auto cmd = std::move(entity_info->client_cmd);
    HandleClientCmd(cmd);
  }

  return 0;
}

int EntityHelper::UpdateMachineByPaxosCmd(EntryInfo* info,
                                          std::unique_ptr<PaxosCmd>& pcmd) {
  auto& machine = info->machine;
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();
  uint32_t local_acceptor_id = pcmd->local_acceptor_id();
  uint32_t peer_acceptor_id = pcmd->peer_acceptor_id();
  bool compensate_msgs = false;

  EntryState prev_st = machine->entry_state();
  const auto local_old_record = machine->GetEntryRecord(local_acceptor_id);
  int ret = machine->Update(peer_acceptor_id, pcmd->peer_entry_record());
  entry_info_mng_->UpdateMemorySize(info);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("Update ret %d st: %s", ret, machine->ToString().c_str());
    return ret;
  }

  EntryState curr_st = machine->entry_state();
  if (curr_st != prev_st && (curr_st == EntryState::kMajorityPromise ||
                             curr_st == EntryState::kChosen)) {
    compensate_msgs = true;
  }

  bool prepared_value_accepted = false;
  auto entity_info = info->entity_info;
  auto& client_cmd = entity_info->client_cmd;
  if (machine->entry_state() == EntryState::kMajorityPromise) {
    if (client_cmd != nullptr && client_cmd->entry() == entry) {
      ret = machine->Accept(client_cmd->value(), client_cmd->value_id(),
                            client_cmd->uuids(), &prepared_value_accepted);
    } else {
      static const std::string value;
      static const std::vector<uint64_t> uuids;
      uint64_t value_id = machine->GetLocalPromisedNum();
      ret = machine->Accept(value, value_id, uuids, &prepared_value_accepted);
    }

    if (ret != 0) {
      CERTAIN_LOG_ERROR("E(%lu, %lu) Accept ret %d machine: %s", entity_id,
                        entry, machine->ToString().c_str());
      return kRetCodeStateAcceptErr;
    }

    if (!prepared_value_accepted && client_cmd != nullptr &&
        client_cmd->entry() == entry) {
      FinishClientCmd(entity_info, kRetCodeStateAcceptFailed);
    }
  }

  const auto& local_record_at_peer = pcmd->local_entry_record();
  const auto& local_record = machine->GetEntryRecord(local_acceptor_id);
  bool need_sync = machine->IsRecordNewer(local_record_at_peer, local_record);
  bool local_updated = machine->IsRecordNewer(local_old_record, local_record);

  CERTAIN_LOG_INFO("local_updated %u need_sync %u compensate_msgs %u info: %s",
                   local_updated, need_sync, compensate_msgs,
                   ToString(info).c_str());

  if (local_updated) {
    if (!StoreEntryInfo(info)) {
      ClearEntryInfo(info);
      FinishClientCmd(entity_info, kRetCodeStoreEntryFailed);
      CERTAIN_LOG_ERROR("E(%lu, %lu) StoreEntryInfo failed", entity_id, entry);
      return kRetCodeStoreEntryFailed;
    }
    if (need_sync) {
      info->peer_to_sync = peer_acceptor_id;
      info->compensate_msgs = compensate_msgs;
    }
  } else if (need_sync) {
    SyncToPeer(info, peer_acceptor_id);
  } else if (pcmd->check_empty()) {
    assert(machine->IsLocalEmpty());
    pcmd->set_check_empty(false);
    MsgWorker::GoAndDeleteIfFailed(std::move(pcmd));
    return 0;
  }

  if (client_cmd != nullptr && client_cmd->cmd_id() == kCmdRead &&
      pcmd->entry() == entry && pcmd->uuid() == client_cmd->uuid()) {
    if (machine->IsLocalEmpty()) {
      machine->SetEmptyFlag(peer_acceptor_id);
    } else {
      FinishClientCmd(entity_info, kRetCodeReadFailed);
      CERTAIN_LOG_ERROR("pcmd: %s", pcmd->ToString().c_str());
      return kRetCodeReadFailed;
    }
    if (machine->IsMajorityEmpty()) {
      FinishClientCmd(entity_info, kRetCodeOk);
    }
  }

  return 0;
}

bool EntityHelper::HandleIfLocalChosen(EntityInfo* entity_info,
                                       std::unique_ptr<PaxosCmd>& pcmd) {
  uint64_t entity_id = pcmd->entity_id();
  uint64_t entry = pcmd->entry();
  EntryInfo* info = entry_info_mng_->FindEntryInfo(entity_id, entry);

  if (!(info != nullptr &&
        info->machine->entry_state() == EntryState::kChosen) &&
      entry > entity_info->max_cont_chosen_entry) {
    // The local state can't be judge as chosen.
    return false;
  }

  if (pcmd->peer_entry_record().chosen()) {
    CERTAIN_LOG_INFO("entity_info: %lu ignore the cmd: %s",
                     ToString(entity_info).c_str(), pcmd->ToString().c_str());
    return true;
  }

  if (info != nullptr) {
    assert(info->machine->entry_state() == EntryState::kChosen ||
           entry <= entity_info->max_cont_chosen_entry);
    uint32_t local_acceptor_id = entity_info->local_acceptor_id;
    const auto& record = info->machine->GetEntryRecord(local_acceptor_id);
    pcmd->set_local_entry_record(record);
    pcmd->SetChosen(true);
    MsgWorker::GoAndDeleteIfFailed(std::move(pcmd));
    return true;
  }

  // Send message from PlogWorker to MsgWorker directly.
  assert(entry <= entity_info->max_cont_chosen_entry);
  pcmd->set_plog_return_msg(true);
  int ret = PlogWorker::GoToPlogReqQueue(pcmd);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("entity %lu push queue failed", entity_id);
  }
  return true;
}

bool EntityHelper::LoadEntryInfo(EntryInfo* info) {
  uint64_t entity_id = info->entity_info->entity_id;
  uint64_t entry = info->entry;
  CERTAIN_LOG_INFO("E(%lu, %lu) to load the entry", entity_id, entry);

  auto pcmd = std::make_unique<PaxosCmd>();
  pcmd->set_plog_get_record(true);
  pcmd->set_entity_id(entity_id);
  pcmd->set_entry(entry);
  pcmd->set_timestamp_usec(GetTimeByUsec());

  int ret = PlogWorker::GoToPlogReqQueue(pcmd);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("entity %lu push queue failed", entity_id);
    return false;
  }

  assert(!info->uncertain);
  info->uncertain = true;
  return true;
}

bool EntityHelper::LoadEntityInfo(EntityInfo* entity_info) {
  auto pcmd = std::make_unique<PaxosCmd>();
  pcmd->set_entity_id(entity_info->entity_id);
  pcmd->set_plog_load(true);
  pcmd->set_timestamp_usec(GetTimeByUsec());

  int ret = PlogWorker::GoToPlogReqQueue(pcmd);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("entity %lu push queue failed", entity_info->entity_id);
    return false;
  }

  assert(!entity_info->loading);
  entity_info->loading = true;
  return true;
}

bool EntityHelper::StoreEntryInfo(EntryInfo* info) {
  auto& machine = info->machine;
  uint64_t entity_id = info->entity_info->entity_id;
  uint32_t local_acceptor_id = info->entity_info->local_acceptor_id;
  auto local_record = machine->GetEntryRecord(local_acceptor_id);

  auto pcmd = std::make_unique<PaxosCmd>(entity_id, info->entry);
  pcmd->set_plog_set_record(true);
  pcmd->set_local_acceptor_id(local_acceptor_id);
  pcmd->set_local_entry_record(local_record);
  pcmd->set_timestamp_usec(GetTimeByUsec());

  int ret = PlogWorker::GoToPlogReqQueue(pcmd);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("entity %lu push queue failed", entity_id);
    return false;
  }

  UpdateCatchupTimestamp(info);
  assert(!info->uncertain);
  info->uncertain = true;

  return true;
}

void EntityHelper::ClearEntryInfo(EntryInfo* info) {
  auto entity_info = info->entity_info;
  if (entity_info->client_cmd != nullptr &&
      entity_info->client_cmd->entry() == info->entry) {
    FinishClientCmd(entity_info, kRetCodeEntryCleanup);
  }
  entry_info_mng_->DestroyEntryInfo(info);
}

void EntityHelper::Broadcast(EntryInfo* info, bool check_empty) {
  CERTAIN_LOG_INFO("broadcast check_empty %u info: %s", check_empty,
                   ToString(info).c_str());
  auto entity_info = info->entity_info;
  uint32_t acceptor_num = entity_info->acceptor_num;
  uint32_t local_acceptor_id = entity_info->local_acceptor_id;

  for (uint32_t peer_acceptor_id = 0; peer_acceptor_id < acceptor_num;
       ++peer_acceptor_id) {
    if (peer_acceptor_id == local_acceptor_id) {
      continue;
    }
    SyncToPeer(info, peer_acceptor_id, check_empty);
  }
}

void EntityHelper::BroadcastOnAccept(EntryInfo* info) {
  CERTAIN_LOG_INFO("info: %s", ToString(info).c_str());
  auto entity_info = info->entity_info;
  uint32_t acceptor_num = entity_info->acceptor_num;
  uint32_t local_acceptor_id = entity_info->local_acceptor_id;

  for (uint32_t peer_acceptor_id = 0; peer_acceptor_id < acceptor_num;
       ++peer_acceptor_id) {
    if (peer_acceptor_id == local_acceptor_id) {
      continue;
    }
    if (peer_acceptor_id != info->peer_to_sync &&
        !info->machine->HasPromisedMyProposal(peer_acceptor_id)) {
      continue;
    }
    SyncToPeer(info, peer_acceptor_id);
  }
}

void EntityHelper::BroadcastOnChosen(EntryInfo* info) {
  CERTAIN_LOG_INFO("info: %s", ToString(info).c_str());
  auto entity_info = info->entity_info;
  uint32_t acceptor_num = entity_info->acceptor_num;
  uint32_t local_acceptor_id = entity_info->local_acceptor_id;

  for (uint32_t peer_acceptor_id = 0; peer_acceptor_id < acceptor_num;
       ++peer_acceptor_id) {
    if (peer_acceptor_id == local_acceptor_id) {
      continue;
    }
    if (info->machine->GetEntryRecord(peer_acceptor_id).chosen()) {
      continue;
    }
    if (peer_acceptor_id != info->peer_to_sync &&
        !info->machine->HasAcceptedMyProposal(peer_acceptor_id)) {
      continue;
    }
    SyncToPeer(info, peer_acceptor_id);
  }
}

void EntityHelper::SyncToPeer(EntryInfo* info, uint32_t peer_acceptor_id,
                              bool check_empty, bool catchup) {
  auto entity_info = info->entity_info;
  uint64_t entity_id = entity_info->entity_id;
  uint64_t entry = info->entry;
  uint32_t local_acceptor_id = entity_info->local_acceptor_id;

  auto pcmd = std::make_unique<PaxosCmd>(entity_id, entry);
  pcmd->set_local_acceptor_id(local_acceptor_id);
  pcmd->set_peer_acceptor_id(peer_acceptor_id);
  pcmd->set_max_chosen_entry(entity_info->max_chosen_entry);
  pcmd->set_check_empty(check_empty);
  pcmd->set_catchup(catchup);

  if (!check_empty) {
    auto& machine = info->machine;
    auto local_record = machine->GetEntryRecord(local_acceptor_id);
    auto peer_record = machine->GetEntryRecord(peer_acceptor_id);
    pcmd->set_local_entry_record(local_record);
    pcmd->set_peer_entry_record(peer_record);
  } else {
    pcmd->set_uuid(entity_info->client_cmd->uuid());
  }

  CERTAIN_LOG_INFO("cmd: %s", pcmd->ToString().c_str());
  if (catchup) {
    CatchupWorker::GoToCatchupReqQueue(std::move(pcmd));
  } else {
    MsgWorker::GoAndDeleteIfFailed(std::move(pcmd));
  }
}

void EntityHelper::UpdateByChosenEntry(EntryInfo* info) {
  auto entity_info = info->entity_info;
  auto& machine = info->machine;
  if (machine->entry_state() != EntryState::kChosen) {
    return;
  }

  if (entity_info->max_chosen_entry < info->entry) {
    entity_info->max_chosen_entry = info->entry;
  }
  if (machine->HasAcceptedMyProposal(entity_info->local_acceptor_id) &&
      (entity_info->pre_auth_entry == kInvalidEntry ||
       entity_info->pre_auth_entry < info->entry)) {
    entity_info->pre_auth_entry = info->entry;
  }

  if (entity_info->max_cont_chosen_entry + 1 == info->entry) {
    // EntryInfos with entry <= max_cont_chosen_entry move to the old table.
    entity_info->max_cont_chosen_entry++;
    entry_info_mng_->MoveToOldChosenList(info);
    auto client_cmd = std::make_unique<ClientCmd>(kCmdWrite);
    client_cmd->set_entity_id(entity_info->entity_id);
    client_cmd->set_entry(info->entry);
    client_cmd->set_value(machine->GetTheChosenValue());
    int ret = DbWorker::GoToDbReqQueue(client_cmd);
    if (ret != 0) {
      CERTAIN_LOG_ERROR("E(%lu, %lu) push queue failed", entity_info->entity_id,
                        info->entry);
    }
  }
  machine->AddTheChosenUuids();
}

void EntityHelper::UpdateByLoadFromPlog(EntityInfo* entity_info,
                                        std::unique_ptr<PaxosCmd>& pcmd) {
  CERTAIN_LOG_INFO("entity: %s", ToString(entity_info).c_str());
  entity_info->max_plog_entry = pcmd->entry();
  uint64_t max_committed_entry = pcmd->max_committed_entry();
  if (entity_info->max_plog_entry > 0 &&
      entity_info->max_chosen_entry < entity_info->max_plog_entry - 1) {
    // The entry may open only when the prev-entry is chosen.
    entity_info->max_chosen_entry = entity_info->max_plog_entry - 1;
  }
  UpdateIfLessThan(entity_info->max_chosen_entry, max_committed_entry);
  UpdateIfLessThan(entity_info->max_cont_chosen_entry, max_committed_entry);
  UpdateIfLessThan(entity_info->max_catchup_entry, max_committed_entry);
  UpdateIfLessThan(entity_info->max_plog_entry, max_committed_entry);
  CERTAIN_LOG_INFO("entity: %s", ToString(entity_info).c_str());
}

int EntityHelper::CreateEntryInfoIfMiss(EntityInfo* entity_info, uint64_t entry,
                                        EntryInfo*& info) {
  uint64_t entity_id = entity_info->entity_id;
  info = entry_info_mng_->FindEntryInfo(entity_id, entry);
  if (info != nullptr) {
    return 0;
  }

  if (!entry_info_mng_->MakeEnoughRoom()) {
    CERTAIN_LOG_ERROR("no enough room for E(%lu, %lu)", entity_id, entry);
    return kRetCodeEntryLimited;
  }

  info = entry_info_mng_->CreateEntryInfo(entity_info, entry);

  if (entity_info->max_plog_entry < entry) {
    return 0;
  }

  if (!LoadEntryInfo(info)) {
    entry_info_mng_->DestroyEntryInfo(info);
    CERTAIN_LOG_ERROR("E(%lu, %lu) LoadEntryInfo failed", entity_id, entry);
    return kRetCodeLoadEntryFailed;
  }

  return 0;
}

int EntityHelper::CreateEntityInfoIfMiss(uint64_t entity_id,
                                         EntityInfo*& entity_info) {
  int ret;
  entity_info = entity_info_mng_->FindEntityInfo(entity_id);
  if (entity_info != nullptr) {
    return 0;
  }

  if (!entity_info_mng_->MakeEnoughRoom()) {
    CERTAIN_LOG_ERROR("no enough room for entity_id %lu", entity_id);
    return kRetCodeEntityLimited;
  }

  uint32_t local_acceptor_id;
  ret = route_->GetLocalAcceptorId(entity_id, &local_acceptor_id);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("entity_id %lu GetLocalAcceptorId %d", entity_id, ret);
    return kRetCodeLocalAcceptorIdErr;
  }

  entity_info = entity_info_mng_->CreateEntityInfo(
      entity_id, options_->acceptor_num(), local_acceptor_id);

  if (!LoadEntityInfo(entity_info)) {
    entity_info_mng_->DestroyEntityInfo(entity_info);
    CERTAIN_LOG_ERROR("entity_id %lu LoadEntityInfo failed");
    return kRetCodeLoadEntityFailed;
  }

  return 0;
}

uint64_t EntityHelper::MemoryUsage() const {
  return entry_info_mng_->MemoryUsage();
}

void EntityHelper::DumpEntry(std::unique_ptr<ClientCmd> cmd) {
  std::fstream f("dump_entry.txt", std::ios::out);

  EntityInfo* entity_info = nullptr;
  int ret = CreateEntityInfoIfMiss(cmd->entity_id(), entity_info);
  if (ret != 0) {
    f << "load entity failed with ret " << ret << std::endl;
    return;
  }
  f << "Entity Info: " << ToString(entity_info) << std::endl;

  EntryInfo* entry_info = nullptr;
  ret = CreateEntryInfoIfMiss(entity_info, cmd->entry(), entry_info);
  if (ret != 0) {
    f << "load entry failed with ret " << ret << std::endl;
    return;
  }
  f << "Entry Info: " << ToString(entry_info) << std::endl;
  f << "Machine State: " << entry_info->machine->ToString() << std::endl;
}

}  // namespace certain
