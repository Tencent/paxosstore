#include "src/libco_notify_helper.h"

#include "co_routine.h"
#include "src/async_queue_mng.h"
#include "utils/co_lock.h"

namespace certain {
namespace {

const uint32_t kMaxContextQueueSize = 1024;
thread_local uint32_t thread_local_nid = UINT32_MAX;

class ThreadGuard {
 public:
  ~ThreadGuard() {
    if (coroutine_) {
      co_free(coroutine_);
    }
  }

  bool Initialized() const { return initialized_; }

  void Init() {
    initialized_ = true;
    co_create(&coroutine_, nullptr, &LoopWaitForwarder, nullptr);
    co_resume(coroutine_);
  }

  static void* LoopWaitForwarder(void*) {
    co_enable_hook_sys();
    LibcoNotifyHelper::GetInstance()->LoopWait();
    return nullptr;
  }

 private:
  stCoRoutine_t* coroutine_ = nullptr;
  bool initialized_ = false;
};
thread_local ThreadGuard guard;

}  // namespace

LibcoNotifyContext::~LibcoNotifyContext() {
  if (thread_local_nid == UINT32_MAX) {
    return;
  }
  assert(cond != nullptr);
  co_cond_free(cond);
}

int LibcoNotifyHelper::Init(Options* options) {
  context_queue_size_ = options->context_queue_size();
  if (context_queues_ != nullptr) {
    return kRetCodeInited;
  }
  context_queues_ =
      std::make_unique<std::unique_ptr<ContextQueue>[]>(context_queue_size_);
  for (uint32_t i = 0; i < context_queue_size_; ++i) {
    context_queues_[i] = nullptr;
  }
  return 0;
}

void LibcoNotifyHelper::Destroy() { context_queues_.reset(); }

void LibcoNotifyHelper::Notify(LibcoNotifyContext* context) {
  if (context->cond == nullptr) {
    NotifyHelper::GetInstance()->NotifyNid(context->nid);
    return;
  }

  auto& ctq = context_queues_[context->nid];
  if (ctq->Size() == 0) {
    NotifyHelper::GetInstance()->NotifyNid(context->nid);
  }

  std::unique_ptr<LibcoNotifyContext> msg(context);
  while (ctq->PushByMultiThread(&msg) != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportContextQueueFail();
    poll(nullptr, 0, 1);
  }
}

std::unique_ptr<LibcoNotifyContext> LibcoNotifyHelper::AllocContext() {
  auto context = std::make_unique<LibcoNotifyContext>();

  if (!co_is_enable_sys_hook()) {
    int ret = NotifyHelper::GetInstance()->TakeNid(context->nid);
    assert(ret == 0);
    return context;
  }

  if (!guard.Initialized()) {
    guard.Init();
    Tick::Add([&] { this->HandleAllNotice(); });
  }

  context->nid = thread_local_nid;
  assert(context->nid != UINT32_MAX);

  context->cond = co_cond_alloc();
  assert(context->cond != nullptr);

  return context;
}

void LibcoNotifyHelper::Wait(std::unique_ptr<LibcoNotifyContext>& context) {
  if (!co_is_enable_sys_hook()) {
    NotifyHelper::GetInstance()->WaitForNid(context->nid);
    return;
  }
  assert(context->cond != nullptr);
  co_cond_timedwait(context->cond, -1);
  return;
}

void LibcoNotifyHelper::HandleAllNotice() {
  assert(thread_local_nid != UINT32_MAX);
  auto& ctq = context_queues_[thread_local_nid];

  std::unique_ptr<LibcoNotifyContext> context;
  while (ctq->PopByOneThread(&context) == 0) {
    int ret = co_cond_signal(context->cond);
    assert(ret == 0);
    context.release();  // do not free this context
  }
}

void LibcoNotifyHelper::LoopWait() {
  assert(thread_local_nid == UINT32_MAX);
  NotifyHelper::GetInstance()->TakeNid(thread_local_nid);
  assert(thread_local_nid != UINT32_MAX);

  context_queues_[thread_local_nid] =
      std::make_unique<ContextQueue>(kMaxContextQueueSize);
  CERTAIN_LOG_ZERO("thread_local_nid %u", thread_local_nid);

  while (true) {
    NotifyHelper::GetInstance()->WaitForNid(thread_local_nid);
  }
}

}  // namespace certain
