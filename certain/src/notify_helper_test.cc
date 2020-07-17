#include "src/notify_helper.h"

#include "gtest/gtest.h"

TEST(NotifyHelperTest, Normal) {
  certain::Options options;
  ASSERT_EQ(certain::NotifyHelper::GetInstance()->Init(&options), 0);

  uint32_t nid = 0;
  for (uint32_t i = 0; i < options.max_notify_num(); ++i) {
    ASSERT_EQ(certain::NotifyHelper::GetInstance()->TakeNid(nid), 0);
    ASSERT_EQ(nid, i);
  }
  ASSERT_EQ(certain::NotifyHelper::GetInstance()->TakeNid(nid),
            certain::kRetCodeNoIdleNotifier);

  for (uint32_t i = 0; i < options.max_notify_num(); ++i) {
    certain::NotifyHelper::GetInstance()->PutBackNid(i);
  }

  certain::NotifyHelper::GetInstance()->Destroy();
}

TEST(NotifyHelperTest, NotifyAndWaitNoTime) {
  certain::Options options;
  ASSERT_EQ(certain::NotifyHelper::GetInstance()->Init(&options), 0);
  for (uint32_t i = 0; i < options.max_notify_num(); ++i) {
    uint32_t nid = 0;
    ASSERT_EQ(certain::NotifyHelper::GetInstance()->TakeNid(nid), 0);
    ASSERT_EQ(nid, i);
  }

  for (uint32_t i = 0; i < options.max_notify_num(); ++i) {
    certain::NotifyHelper::GetInstance()->NotifyNid(i);
  }

  for (uint32_t i = 0; i < options.max_notify_num(); ++i) {
    certain::NotifyHelper::GetInstance()->WaitForNid(i);
  }

  for (uint32_t i = 0; i < options.max_notify_num(); ++i) {
    certain::NotifyHelper::GetInstance()->PutBackNid(i);
  }
  certain::NotifyHelper::GetInstance()->Destroy();
}

TEST(NotifyHelperTest, ReInit) {
  certain::Options options;
  ASSERT_EQ(certain::NotifyHelper::GetInstance()->Init(&options), 0);
  ASSERT_EQ(certain::NotifyHelper::GetInstance()->Init(&options),
            certain::kRetCodeInited);
  certain::NotifyHelper::GetInstance()->Destroy();
  ASSERT_EQ(certain::NotifyHelper::GetInstance()->Init(&options), 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
