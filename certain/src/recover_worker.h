#pragma once

#include <unordered_map>

#include "certain/options.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "utils/routine_worker.h"
#include "utils/traffic_limiter.h"

namespace certain {

class RecoverWorker : public RoutineWorker<PaxosCmd> {
 public:
  RecoverWorker(Options* options, uint32_t worker_id)
      : RoutineWorker<PaxosCmd>("recover_" + std::to_string(worker_id),
                                options->recover_routine_num()),
        options_(options),
        worker_id_(worker_id) {
    auto queue_mng = AsyncQueueMng::GetInstance();
    recover_req_queue_ = queue_mng->GetRecoverReqQueueByIdx(worker_id);

    limiter_.UpdateCount(options_->recover_max_count_per_second());
  }

  static int GoToRecoverReqQueue(std::unique_ptr<PaxosCmd>& pcmd);
  static void GoToRecoverRspQueue(std::unique_ptr<PaxosCmd>& pcmd);

 private:
  std::unique_ptr<PaxosCmd> GetJob() final;
  void DoJob(std::unique_ptr<PaxosCmd> job) final;

 private:
  Options* options_;
  uint32_t worker_id_;
  AsyncQueue* recover_req_queue_;
  TrafficLimiter limiter_;

  std::unordered_set<uint64_t> doing_;
};

}  // namespace certain
