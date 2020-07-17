#pragma once

#include <stack>
#include <unordered_map>

#include "certain/db.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "src/wrapper.h"
#include "utils/routine_worker.h"
#include "utils/thread.h"
#include "utils/traffic_limiter.h"
#include "utils/usetime_stat.h"

namespace certain {

class DbLimitedWorker : public RoutineWorker<ClientCmd> {
 public:
  virtual ~DbLimitedWorker() {}
  DbLimitedWorker(Options* options, uint32_t worker_id, Db* db)
      : RoutineWorker<ClientCmd>(
            "db_limiteed_worker_" + std::to_string(worker_id),
            options->db_routine_num()),
        options_(options),
        monitor_(options->monitor()),
        db_(db) {
    auto queue_mng = AsyncQueueMng::GetInstance();
    db_req_queue_ = queue_mng->GetDbLimitedReqQueueByIdx(worker_id);
    limiter_.UpdateSpeed(options_->db_max_kb_per_second() * 1024ul);
    limiter_.UpdateCount(options_->db_max_count_per_second());
  }

  static int GoToDbLimitedReqQueue(std::unique_ptr<ClientCmd>& cmd);

 private:
  void Tick() override;
  std::unique_ptr<ClientCmd> GetJob() final;
  void DoJob(std::unique_ptr<ClientCmd> cmd) final;

 private:
  Options* options_;
  Monitor* monitor_;
  Db* db_;
  AsyncQueue* db_req_queue_;
  TrafficLimiter limiter_;

  std::list<std::unique_ptr<ClientCmd>> pending_;
  std::unordered_map<uint64_t, decltype(pending_)::iterator> hash_;
};

}  // namespace certain
