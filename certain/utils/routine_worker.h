#pragma once
#include <atomic>
#include <memory>
#include <stack>
#include <thread>
#include <vector>

#include "co_routine_inner.h"
#include "utils/log.h"
#include "utils/thread.h"

namespace certain {

template <class T>
class RoutineWorker : public ThreadBase {
 public:
  RoutineWorker(const std::string& name, uint32_t num_routines)
      : ThreadBase(name), num_routines_(num_routines) {}

 protected:
  virtual std::unique_ptr<T> GetJob() = 0;
  virtual void DoJob(std::unique_ptr<T> job) = 0;
  virtual void Tick() {}

 protected:
  void Run() final {
    std::vector<RoutineArg> args(num_routines_);
    for (uint32_t i = 0; i < args.size(); ++i) {
      auto& arg = args[i];
      co_create(&arg.coroutine, nullptr, &RoutineFuncForwarder, &arg);
      arg.self = this;
      co_resume(arg.coroutine);
    }

    CERTAIN_LOG_INFO("%s %u routines started", name().c_str(), num_routines_);
    co_eventloop(co_get_epoll_ct(), RoutineTickForwarder, this);

    for (auto& arg : args) {
      co_free(arg.coroutine);
    }
  }

  static int RoutineTickForwarder(void* arg_ptr) {
    co_enable_hook_sys();
    auto self = reinterpret_cast<RoutineWorker*>(arg_ptr);
    if (self->exit_flag()) {
      return -1;
    }

    self->Tick();

    while (!self->idles_.empty()) {
      std::unique_ptr<T> job = self->GetJob();
      if (job == nullptr) {
        break;
      }
      auto idle = self->idles_.top();
      self->idles_.pop();
      idle->job = std::move(job);
      co_resume(idle->coroutine);
    }
    return 0;
  }

  static void* RoutineFuncForwarder(void* arg_ptr) {
    co_enable_hook_sys();
    auto arg = reinterpret_cast<RoutineArg*>(arg_ptr);
    while (true) {
      if (arg->job == nullptr) {
        arg->self->idles_.push(arg);
        co_yield_ct();
      } else {
        arg->self->DoJob(std::move(arg->job));
        arg->job = nullptr;
      }
    }
    return nullptr;
  }

 protected:
  struct RoutineArg {
    stCoRoutine_t* coroutine = nullptr;
    RoutineWorker* self = nullptr;
    std::unique_ptr<T> job{nullptr};
  };

  uint32_t num_routines_;
  std::stack<RoutineArg*> idles_;
};

}  // namespace certain
