#include "src/entity_worker.h"

namespace certain {
namespace {

std::atomic<int64_t> total_memory_usage{0};

}  // namespace

EntityWorker::EntityWorker(Options* options, uint32_t worker_id)
    : ThreadBase("entity_" + std::to_string(worker_id)) {
  options_ = options;
  worker_id_ = worker_id;

  AsyncQueueMng* queue_mng = AsyncQueueMng::GetInstance();
  entity_req_queue_ = queue_mng->GetEntityReqQueueByIdx(worker_id_);
  user_req_queue_ = queue_mng->GetUserReqQueueByIdx(worker_id);
  user_rsp_queue_ = queue_mng->GetUserRspQueueByIdx(worker_id);
  plog_rsp_queue_ = queue_mng->GetPlogRspQueueByIdx(worker_id);
  recover_rsp_queue_ = queue_mng->GetRecoverRspQueueByIdx(worker_id);

  auto route = Wrapper::GetInstance()->GetRouteImpl();
  entity_helper_ = std::make_unique<EntityHelper>(options_, worker_id_,
                                                  user_rsp_queue_, route);
}

void EntityWorker::ReplyClientCmd(ClientCmd* client_cmd) {
  auto ctx = client_cmd->context();
  LibcoNotifyHelper::GetInstance()->Notify(ctx);
}

bool EntityWorker::HandleEvents() {
  bool has_work = false;
  std::unique_ptr<CmdBase> cmd;

  // 1. Handle user_req_queue_.
  while (user_req_queue_->PopByOneThread(&cmd) == 0) {
    assert(cmd != nullptr);
    auto& client_cmd = unique_cast<ClientCmd>(cmd);
    entity_helper_->HandleClientCmd(client_cmd);
    has_work = true;
  }

  // 2. Handle user_rsp_queue_.
  while (user_rsp_queue_->PopByOneThread(&cmd) == 0) {
    assert(cmd != nullptr);
    ReplyClientCmd((ClientCmd*)cmd.get());
    cmd.release();  // do not delete read / write cmd on stack
    has_work = true;
  }

  // 3. Handle entity_req_queue_.
  while (entity_req_queue_->PopByOneThread(&cmd) == 0) {
    assert(cmd != nullptr);
    auto& pcmd = unique_cast<PaxosCmd>(cmd);
    int ret = entity_helper_->HandlePaxosCmd(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("cmd: %s ret %d", pcmd->ToString().c_str(), ret);
    }
    has_work = true;
  }

  // 4. Handle plog_rsp_queue_.
  while (plog_rsp_queue_->PopByOneThread(&cmd) == 0) {
    assert(cmd != nullptr);
    auto& pcmd = unique_cast<PaxosCmd>(cmd);
    int ret = entity_helper_->HandlePlogRspCmd(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("cmd: %s ret %d", pcmd->ToString().c_str(), ret);
    }
    has_work = true;
  }

  // 5. Handle recover_rsp_queue_.
  while (recover_rsp_queue_->PopByOneThread(&cmd) == 0) {
    assert(cmd != nullptr);
    auto& pcmd = unique_cast<PaxosCmd>(cmd);
    int ret = entity_helper_->HandleRecoverRspCmd(pcmd);
    if (ret < 0) {
      CERTAIN_LOG_ERROR("cmd: %s ret %d", pcmd->ToString().c_str(), ret);
    }
    has_work = true;
  }

  if (entity_helper_->CheckIfHasWork()) {
    has_work = true;
  }

  return has_work;
}

void EntityWorker::Run() {
  int count = 0;
  int64_t last_memory_usage = 0;

  while (!ThreadBase::exit_flag()) {
    if (!HandleEvents()) {
      usleep(1000);
    }

    if (++count % 100 == 0) {
      int64_t current_memory_usage = entity_helper_->MemoryUsage();
      total_memory_usage += current_memory_usage - last_memory_usage;
      last_memory_usage = current_memory_usage;
    }
  }
}

void EntityWorker::LogMemoryUsage() {
  CERTAIN_LOG_ZERO("entity worker total memory usage: %.1fMB",
                   total_memory_usage.load() / double(1 << 20));
}

}  // namespace certain
