#pragma once

#include "src/entity_helper.h"
#include "src/libco_notify_helper.h"
#include "utils/thread.h"

namespace certain {

class EntityWorker : public ThreadBase {
 public:
  virtual ~EntityWorker() {}
  EntityWorker(Options* options, uint32_t worker_id);

  // Return true if some event handled, otherwise return false.
  bool HandleEvents();

  virtual void Run() override;

  static void ReplyClientCmd(ClientCmd* client_cmd);

  static void LogMemoryUsage();

 private:
  Options* options_;
  uint32_t worker_id_;

  // The queues that go into entity workers.
  // msgworker -> entityworker
  AsyncQueue* entity_req_queue_;

  // user's worker -> entityworker
  AsyncQueue* user_req_queue_;

  // entityworker(EntityHelper) -> entityworker(EntityWorker)
  AsyncQueue* user_rsp_queue_;

  // plogworker -> entityworker
  AsyncQueue* plog_rsp_queue_;

  // recoverworker -> entityworker
  AsyncQueue* recover_rsp_queue_;

  std::unique_ptr<EntityHelper> entity_helper_;
};

}  // namespace certain
