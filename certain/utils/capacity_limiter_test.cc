#include "utils/capacity_limiter.h"

#include "gtest/gtest.h"

TEST(SharedLimiterTest, Basic) {
  certain::SharedLimiter shared(10);
  ASSERT_TRUE(shared.AllocBytes(9));
  ASSERT_FALSE(shared.AllocBytes(2));
  ASSERT_TRUE(shared.AllocBytes(1));

  std::cout << shared.ToString() << std::endl;

  shared.FreeBytes(1);
  shared.FreeBytes(9);
}

TEST(CapacityLimiterTest, Basic) {
  certain::SharedLimiter shared(10);
  certain::CapacityLimiter limiter(10, &shared);

  ASSERT_TRUE(limiter.AllocBytes(9));
  ASSERT_FALSE(limiter.AllocBytes(2));
  ASSERT_TRUE(limiter.AllocBytes(1));

  std::cout << limiter.ToString() << std::endl;

  limiter.FreeBytes(1);
  limiter.FreeBytes(9);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
