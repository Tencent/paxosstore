#include "co_lock.h"

#include <cassert>
#include <list>
#include <unordered_set>

#include "utils/hash.h"
#include "utils/memory.h"

namespace certain {
namespace {

thread_local bool added_to_tick_list = false;
thread_local std::vector<std::function<void()>> ticks;
thread_local std::unordered_set<stCoRoutine_t*> gLockedCoroutines;
thread_local std::list<std::pair<stCoRoutine_t*, uint32_t>> gLockedItems;

}  // namespace

void Tick::Add(std::function<void()>&& func) {
  ticks.push_back(std::move(func));
}

void Tick::Run() {
  for (auto&& func : ticks) {
    func();
  }
}

CoHashLock::CoHashLock(uint32_t bucket_num)
    : bucket_num_(bucket_num),
      locks_(std::make_unique<std::mutex[]>(bucket_num_)) {}

void CoHashLock::Lock(uint64_t id) {
  uint32_t bucket = Hash(id) % bucket_num_;
  if (!co_is_enable_sys_hook()) {
    // not in coroutine env
    locks_[bucket].lock();
    return;
  }

  if (!added_to_tick_list) {
    added_to_tick_list = true;
    Tick::Add([&] { this->CheckAllLock(); });
  }

  stCoRoutine_t* co = co_self();
  assert(gLockedCoroutines.count(co) == 0);

  if (locks_[bucket].try_lock()) {
    // acquire lock
    return;
  }

  gLockedCoroutines.insert(co);
  gLockedItems.push_back(std::make_pair(co, bucket));
  co_yield_ct();
}

void CoHashLock::Unlock(uint64_t id) {
  uint32_t bucket = Hash(id) % bucket_num_;
  // release lock
  locks_[bucket].unlock();
}

void CoHashLock::CheckAllLock() {
  for (auto it = gLockedItems.begin(); it != gLockedItems.end();) {
    if (locks_[it->second].try_lock()) {
      // acquire lock and resume
      auto co = it->first;
      gLockedCoroutines.erase(co);
      it = gLockedItems.erase(it);
      co_resume(co);
    } else {
      ++it;
    }
  }
}

}  // namespace certain
