#include "utils/traffic_limiter.h"

#include "gtest/gtest.h"

TEST(CountLimiter, Basic) {
  certain::CountLimiter limiter;
  limiter.UpdateCount(10);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(limiter.AcquireOne());
  }
  ASSERT_FALSE(limiter.AcquireOne());
  sleep(1);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(limiter.AcquireOne());
  }
  ASSERT_FALSE(limiter.AcquireOne());
  sleep(1);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(limiter.AcquireOne());
  }
  ASSERT_FALSE(limiter.AcquireOne());
  sleep(1);
}

TEST(TrafficLimiter, Basic) {
  certain::TrafficLimiter limiter;

  ASSERT_EQ(0, limiter.UseBytes(1 << 30));
  ASSERT_EQ(0, limiter.UseCount(1 << 30));

  limiter.UpdateSpeed(100 << 10);  // 100KB, 1KB/10MS
  for (int i = 0; i < 32; ++i) {
    ASSERT_EQ(0, limiter.UseBytes(32));  // 32B * 32 = 1KB
  }
  ASSERT_GT(limiter.UseBytes(32), 0);
  ASSERT_LE(limiter.UseBytes(32), 10);

  limiter.UpdateCount(32);
  for (int i = 0; i < 32; ++i) {
    ASSERT_EQ(0, limiter.UseCount());
  }
  ASSERT_GT(limiter.UseCount(), 0);
  ASSERT_LE(limiter.UseCount(), 1000);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
