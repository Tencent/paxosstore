#pragma once

#include "certain/options.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "utils/routine_worker.h"

namespace certain {

class ToolsWorker : public RoutineWorker<ClientCmd> {
 public:
  ToolsWorker(Options* options, uint32_t worker_id)
      : RoutineWorker<ClientCmd>("tools_" + std::to_string(worker_id), 64),
        options_(*options),
        queue_(
            *AsyncQueueMng::GetInstance()->GetToolsReqQueueByIdx(worker_id)) {}

  static int GoToToolsReqQueue(std::unique_ptr<ClientCmd>& pcmd);

 private:
  std::unique_ptr<ClientCmd> GetJob() final;
  void DoJob(std::unique_ptr<ClientCmd> job) final;

 private:
  const Options& options_;
  AsyncQueue& queue_;
};

}  // namespace certain
