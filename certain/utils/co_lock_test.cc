#include "co_lock.h"

#include <atomic>

#include "gtest/gtest.h"

namespace {

int cnt = 0;

void* Coroutine(void* arg_ptr) {
  co_enable_hook_sys();
  poll(nullptr, 0, 1);

  auto lock = reinterpret_cast<certain::CoHashLock*>(arg_ptr);

  for (int i = 0; i < 50; ++i) {
    {
      lock->Lock(0);
      ++cnt;
      volatile int now = cnt;
      poll(nullptr, 0, 1);
      assert(now == cnt);
      lock->Unlock(0);
    }

    volatile int now = cnt;
    poll(nullptr, 0, 1);
    assert(now != cnt);
  }

  return nullptr;
}

int CheckQuit(void* arg_ptr) {
  auto lock = reinterpret_cast<certain::CoHashLock*>(arg_ptr);
  lock->CheckAllLock();

  if (cnt >= 100) {
    return -1;
  }

  return 0;
}

}  // namespace

TEST(CoLock, HashLock) {
  ASSERT_EQ(0, 0);
  certain::CoHashLock lock(1);

  for (int i = 0; i < 2; ++i) {
    stCoRoutine_t* co = nullptr;
    co_create(&co, nullptr, Coroutine, (void*)&lock);
    co_resume(co);
  }

  co_eventloop(co_get_epoll_ct(), CheckQuit, (void*)&lock);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
