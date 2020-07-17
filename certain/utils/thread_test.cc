#include "utils/thread.h"

#include "gtest/gtest.h"
#include "utils/time.h"

class FooWorker : public certain::ThreadBase {
 public:
  virtual void Run() { usleep(1000 * 90); }
};

TEST(ThreadTest, Basic) {
  uint64_t t0 = certain::GetTimeByMsec();
  FooWorker worker;
  worker.Start();
  worker.WaitExit();
  uint64_t t1 = certain::GetTimeByMsec();
  uint64_t cost = t1 - t0;
  // For precision issue.
  if (cost > 90 && cost < 95) {
    cost = 90;
  }
  ASSERT_EQ(cost, 90);
}

TEST(ThreadTest, Mutex) {
  certain::Mutex mutex;
  { certain::ThreadLock o(&mutex); }
  { certain::ThreadLock o(&mutex); }
}

TEST(ThreadTest, ReadWriteLock) {
  certain::ReadWriteLock rw;
  { certain::ThreadReadLock o(&rw); }
  { certain::ThreadWriteLock o(&rw); }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
