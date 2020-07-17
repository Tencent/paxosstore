#include "utils/time.h"

#include "gtest/gtest.h"

TEST(TimeTest, Basic) {
  uint64_t t0 = certain::GetTimeByUsec();
  uint64_t t1 = certain::GetTimeByMsec();
  uint64_t t2 = certain::GetTimeBySecond();
  ASSERT_EQ(t0 / 1000, t1);
  ASSERT_EQ(t1 / 1000, t2);

  usleep(1000);
  uint64_t t3 = certain::GetTimeByMsec();
  ASSERT_TRUE(t1 + 1 <= t3 && t1 + 2 >= t3);
}

TEST(TimeTest, Performance_1000k_times) {
  for (int i = 0; i < 1000000; ++i) {
    certain::GetTimeByUsec();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
