#pragma once

#include "certain/db.h"
#include "certain/options.h"
#include "certain/plog.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "utils/routine_worker.h"
#include "utils/usetime_stat.h"

namespace certain {

class PlogWorker : public ThreadBase {
 public:
  virtual ~PlogWorker() {}
  PlogWorker(Options* options, uint32_t worker_id, Plog* plog, Db* db)
      : ThreadBase("plog_worker_" + std::to_string(worker_id)),
        options_(options),
        monitor_(options->monitor()),
        worker_id_(worker_id),
        plog_(plog),
        db_(db) {
    auto queue_mng = AsyncQueueMng::GetInstance();
    plog_req_queue_ = queue_mng->GetPlogReqQueueByIdx(worker_id);
  }

  static int GoToPlogReqQueue(std::unique_ptr<PaxosCmd>& pcmd);
  static void GoToPlogRspQueue(std::unique_ptr<PaxosCmd>& pcmd);

 private:
  virtual void Run() override;
  void SetRecord(std::vector<std::unique_ptr<PaxosCmd>>& cmds);

 private:
  Options* options_;
  Monitor* monitor_;
  uint32_t worker_id_;
  Plog* plog_;
  Db* db_;

  // entityworker -> plogworker
  AsyncQueue* plog_req_queue_;
};

class PlogReadonlyWorker : public RoutineWorker<PaxosCmd> {
 public:
  PlogReadonlyWorker(Options* options, uint32_t worker_id, Plog* plog, Db* db)
      : RoutineWorker<PaxosCmd>(
            "plog_readonly_worker_" + std::to_string(worker_id),
            options->plog_routine_num()),
        options_(options),
        monitor_(options->monitor()),
        plog_(plog),
        db_(db) {
    auto queue_mng = AsyncQueueMng::GetInstance();
    plog_req_queue_ = queue_mng->GetPlogReadonlyReqQueueByIdx(worker_id);
  }

 private:
  std::unique_ptr<PaxosCmd> GetJob() final;
  void DoJob(std::unique_ptr<PaxosCmd> cmd) final;

  void LoadMaxEntry(const std::unique_ptr<PaxosCmd>& cmd);
  void GetRecord(const std::unique_ptr<PaxosCmd>& cmd);

 private:
  uint32_t worker_id_;
  Options* options_;
  Monitor* monitor_;
  Plog* plog_;
  Db* db_;
  AsyncQueue* plog_req_queue_;
};

}  // namespace certain
