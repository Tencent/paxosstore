#pragma once

#include "certain/options.h"
#include "src/command.h"
#include "utils/routine_worker.h"
#include "utils/traffic_limiter.h"

namespace certain {

class CatchupWorker : public RoutineWorker<PaxosCmd> {
 public:
  CatchupWorker(Options* options, uint32_t worker_id)
      : RoutineWorker<PaxosCmd>("catchup_" + std::to_string(worker_id),
                                options->catchup_routine_num()),
        options_(*options),
        monitor_(options->monitor()),
        worker_id_(worker_id) {
    limiter_.UpdateSpeed(options_.catchup_max_kb_per_second() * 1024ul);
    limiter_.UpdateCount(options_.catchup_max_count_per_second());
  }

  static int GoToCatchupReqQueue(std::unique_ptr<PaxosCmd> pcmd);

 private:
  std::unique_ptr<PaxosCmd> GetJob() final;
  void DoJob(std::unique_ptr<PaxosCmd> job) final;

 private:
  const Options& options_;
  Monitor* monitor_;
  uint32_t worker_id_;
  TrafficLimiter limiter_;
};

}  // namespace certain
