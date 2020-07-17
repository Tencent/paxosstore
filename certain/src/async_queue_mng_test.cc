#include "src/async_queue_mng.h"

#include "gtest/gtest.h"

TEST(AsyncQueueMngTest, Normal) {
  certain::Options options;
  ASSERT_EQ(certain::AsyncQueueMng::GetInstance()->Init(&options), 0);
  certain::AsyncQueueMng::GetInstance()->LogAllQueueStat();
  certain::AsyncQueueMng::GetInstance()->Destroy();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
