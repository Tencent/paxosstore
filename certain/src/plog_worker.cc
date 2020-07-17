#include "src/plog_worker.h"

#include "certain/errors.h"
#include "src/msg_worker.h"

namespace certain {

int PlogWorker::GoToPlogReqQueue(std::unique_ptr<PaxosCmd>& pcmd) {
  AsyncQueueMng* queue_mng = AsyncQueueMng::GetInstance();

  AsyncQueue* queue = nullptr;
  if (pcmd->plog_set_record()) {
    queue = queue_mng->GetPlogReqQueueByEntityId(pcmd->entity_id());
  } else {
    queue = queue_mng->GetPlogReadonlyReqQueueByEntityId(pcmd->entity_id());
  }
  int ret = queue->PushByMultiThread(&pcmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    if (pcmd->plog_set_record()) {
      monitor->ReportPlogReqQueueFail();
    } else {
      monitor->ReportReadonlyReqQueueFail();
    }
    CERTAIN_LOG_ERROR("push cmd to plog_req_queue_ failed ret %d", ret);
  }
  return ret;
}

void PlogWorker::GoToPlogRspQueue(std::unique_ptr<PaxosCmd>& pcmd) {
  AsyncQueueMng* queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetPlogRspQueueByEntityId(pcmd->entity_id());

  while (true) {
    int ret = queue->PushByMultiThread(&pcmd);
    if (ret == 0) {
      break;
    }
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportPlogRspQueueFail();
    CERTAIN_LOG_ERROR("push cmd to plog_rsp_queue_ failed ret %d", ret);
    // The pcmd must response to entityworker, wait and retry.
    poll(nullptr, 0, 1);
  }
}

void PlogWorker::Run() {
  std::vector<std::unique_ptr<PaxosCmd>> cmds;
  while (!exit_flag()) {
    while (cmds.size() < options_->max_plog_batch_size()) {
      std::unique_ptr<PaxosCmd> cmd;
      int ret = plog_req_queue_->PopByOneThread(&cmd);
      if (ret != 0 || cmd == nullptr) {
        break;
      }
      // write only
      assert(cmd->cmd_id() == kCmdPaxos);

      cmds.push_back(std::move(cmd));
      assert(cmds.back()->plog_set_record());
    }

    if (cmds.empty()) {
      poll(NULL, 0, 1);
      continue;
    }

    SetRecord(cmds);
    for (auto& cmd : cmds) {
      GoToPlogRspQueue(cmd);
    }
    cmds.clear();
  }
}

void PlogWorker::SetRecord(std::vector<std::unique_ptr<PaxosCmd>>& cmds) {
  // group by hash of entity
  std::unordered_map<
      uint32_t, std::pair<std::vector<PaxosCmd*>, std::vector<Plog::Record>>>
      records_groups;

  for (auto& cmd : cmds) {
    uint32_t hash_id = plog_->HashId(cmd->entity_id());
    auto& pair = records_groups[hash_id];

    const EntryRecord& record = cmd->local_entry_record();
    std::string buffer;
    if (!record.SerializeToString(&buffer)) {
      cmd->set_result(kRetCodeInvalidRecord);
      continue;
    }

    pair.first.push_back(cmd.get());
    pair.second.emplace_back();
    auto& back = pair.second.back();
    back.entity_id = cmd->entity_id();
    back.entry = cmd->entry();
    back.record = std::move(buffer);
  }

  for (auto& it : records_groups) {
    TimeDelta time_delta;
    int ret = plog_->MultiSetRecords(it.first, it.second.second);
    monitor_->ReportMultiSetRecordsTimeCost(ret, time_delta.DeltaUsec());
    monitor_->ReportTotalRecordByMultiSet(it.second.second.size());
    if (ret != 0) {
      for (auto& cmd : it.second.first) {
        cmd->set_result(ret);
      }
    }
  }
}

std::unique_ptr<PaxosCmd> PlogReadonlyWorker::GetJob() {
  std::unique_ptr<PaxosCmd> cmd;
  plog_req_queue_->PopByOneThread(&cmd);
  return cmd;
}

void PlogReadonlyWorker::DoJob(std::unique_ptr<PaxosCmd> cmd) {
  if (cmd->plog_return_msg()) {
    GetRecord(cmd);
    if (cmd->result() == 0 && cmd->local_entry_record().accepted_num() > 0) {
      assert(cmd->local_entry_record().value_id() > 0);
      cmd->SetChosen(true);
    } else if ((cmd->result() == 0 && !cmd->local_entry_record().chosen()) ||
               cmd->result() == kImplPlogNotFound) {
      cmd->set_result(kImplPlogNotFound);
    } else {
      CERTAIN_LOG_ERROR("Fatal: GetRecord ret %d", cmd->result());
    }
    MsgWorker::GoAndDeleteIfFailed(std::move(cmd));
    return;
  }

  if (cmd->plog_load()) {
    TimeDelta time_delta;
    LoadMaxEntry(cmd);
    monitor_->ReportLoadMaxEntryTimeCost(cmd->result(), time_delta.DeltaUsec());
  } else if (cmd->plog_get_record()) {
    GetRecord(cmd);
  } else {
    assert(false);
  }

  PlogWorker::GoToPlogRspQueue(cmd);
}

void PlogReadonlyWorker::LoadMaxEntry(const std::unique_ptr<PaxosCmd>& cmd) {
  uint64_t entry = -1;
  int ret = plog_->LoadMaxEntry(cmd->entity_id(), &entry);
  if (ret == 0) {
    cmd->set_entry(entry);
  } else if (ret == kImplPlogNotFound) {
    cmd->set_entry(0);
  } else {
    cmd->set_result(ret);
    return;
  }
  uint64_t max_committed_entry = 0;
  Db::RecoverFlag flags = Db::kNormal;
  ret = db_->GetStatus(cmd->entity_id(), &max_committed_entry, &flags);
  if (ret == 0) {
    cmd->set_max_committed_entry(max_committed_entry);
  } else if (ret == kImplDbNotFound) {
    cmd->set_max_committed_entry(0);
  } else {
    cmd->set_result(ret);
  }
  // TODO: do with flags != 0
}

void PlogReadonlyWorker::GetRecord(const std::unique_ptr<PaxosCmd>& cmd) {
  std::string buffer;
  TimeDelta time_delta;
  int ret = plog_->GetRecord(cmd->entity_id(), cmd->entry(), &buffer);
  monitor_->ReportGetRecordTimeCost(ret, time_delta.DeltaUsec());
  if (ret != 0) {
    cmd->set_result(ret);
    return;
  }

  EntryRecord record;
  if (!record.ParseFromString(buffer)) {
    cmd->set_result(kRetCodeInvalidRecord);
    return;
  }
  cmd->set_local_entry_record(record);
}

}  // namespace certain
