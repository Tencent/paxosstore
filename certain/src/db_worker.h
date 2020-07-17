#pragma once

#include "certain/db.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "utils/co_lock.h"
#include "utils/routine_worker.h"
#include "utils/usetime_stat.h"

namespace certain {

class DbWorker : public RoutineWorker<ClientCmd> {
 public:
  virtual ~DbWorker() {}
  DbWorker(Options* options, uint32_t worker_id, Db* db)
      : RoutineWorker<ClientCmd>("db_worker_" + std::to_string(worker_id),
                                 options->db_routine_num()),
        options_(options),
        monitor_(options->monitor()),
        worker_id_(worker_id),
        db_(db) {
    auto queue_mng = AsyncQueueMng::GetInstance();
    db_req_queue_ = queue_mng->GetDbReqQueueByIdx(worker_id);
  }

  static int GoToDbReqQueue(std::unique_ptr<ClientCmd>& cmd);

 private:
  std::unique_ptr<ClientCmd> GetJob() final;
  void DoJob(std::unique_ptr<ClientCmd> cmd) final;
  void Tick() final { Tick::Run(); }

 private:
  Options* options_;
  Monitor* monitor_;
  uint32_t worker_id_;
  Db* db_;
  AsyncQueue* db_req_queue_;
};

}  // namespace certain
