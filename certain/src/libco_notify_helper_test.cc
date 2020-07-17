#include "src/libco_notify_helper.h"

#include <atomic>

#include "co_routine.h"
#include "co_routine_inner.h"
#include "gtest/gtest.h"
#include "utils/co_lock.h"
#include "utils/lock_free_queue.h"

const int N = 10;
static int g_finished = 0;
static certain::LockFreeQueue<certain::LibcoNotifyContext*> g_queue(N);

static int EventLoopCallBackEntry(void* args) {
  certain::Tick::Run();
  return g_finished == N ? -1 : 0;
}

static void* AllocAndWait(void* args) {
  auto ctx = certain::LibcoNotifyHelper::GetInstance()->AllocContext();
  ctx = certain::LibcoNotifyHelper::GetInstance()->AllocContext();
  auto item = std::make_unique<certain::LibcoNotifyContext*>(ctx.get());
  g_queue.PushByMultiThread(&item);
  certain::LibcoNotifyHelper::GetInstance()->Wait(ctx);

  ++g_finished;
  return nullptr;
}

TEST(LibcoNotifyHelper, Test) {
  certain::Options options;
  std::thread([] {
    std::unique_ptr<certain::LibcoNotifyContext*> ctx;
    for (int i = 0; i < N; ++i) {
      while (g_queue.PopByOneThread(&ctx) != 0) {
      }
      certain::LibcoNotifyHelper::GetInstance()->Notify(*ctx);
    }
  }).detach();

  ASSERT_EQ(0, certain::NotifyHelper::GetInstance()->Init(&options));
  ASSERT_EQ(0, certain::LibcoNotifyHelper::GetInstance()->Init(&options));

  {
    co_init_curr_thread_env();
    for (int i = 0; i < N; ++i) {
      stCoRoutine_t* co = nullptr;
      co_create(&co, nullptr, AllocAndWait, nullptr);
      co_resume(co);
    }

    co_eventloop(co_get_epoll_ct(), EventLoopCallBackEntry, nullptr);
  }

  certain::LibcoNotifyHelper::GetInstance()->Destroy();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
