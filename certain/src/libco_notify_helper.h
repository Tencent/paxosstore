#pragma once

#include "co_routine.h"
#include "src/notify_helper.h"
#include "src/wrapper.h"

namespace certain {

// The caller thread:
//
// auto ctx = AllocContext();
// cmd->set_context(ctx.get());
// push cmd to the callee thread..
// Wait(ctx);
//
//
// The callee thread:
//
// take the cmd and process..
// Notify(cmd->context());

struct LibcoNotifyContext {
  uint32_t nid = 0;
  stCoCond_t* cond = nullptr;

  ~LibcoNotifyContext();
};

class LibcoNotifyHelper : public Singleton<LibcoNotifyHelper> {
 private:
  uint32_t context_queue_size_ = 0;

  typedef LockFreeQueue<LibcoNotifyContext> ContextQueue;
  std::unique_ptr<std::unique_ptr<ContextQueue>[]> context_queues_;

 public:
  int Init(Options* options);
  void Destroy();

  void Notify(LibcoNotifyContext* context);

  std::unique_ptr<LibcoNotifyContext> AllocContext();
  void Wait(std::unique_ptr<LibcoNotifyContext>& context);

  void HandleAllNotice();
  void LoopWait();
};

}  // namespace certain
