#pragma once
#include <memory>
#include <mutex>
#include <vector>

#include "co_routine_inner.h"

namespace certain {

class AutoDisableHook {
 public:
  AutoDisableHook() {
    if (co_is_enable_sys_hook()) {
      disabled_ = true;
      co_disable_hook_sys();
    }
  }

  ~AutoDisableHook() {
    if (disabled_) {
      co_enable_hook_sys();
    }
  }

 private:
  bool disabled_ = false;
};

class CoHashLock {
 private:
  uint32_t bucket_num_;
  std::unique_ptr<std::mutex[]> locks_;

 public:
  CoHashLock(uint32_t bucket_num);

  void Lock(uint64_t id);

  void Unlock(uint64_t id);

  // Call in the CoEpollTick func to resume locked coroutine
  void CheckAllLock();
};

class Tick {
 public:
  static void Add(std::function<void()>&& func);
  static void Run();
};

}  // namespace certain
