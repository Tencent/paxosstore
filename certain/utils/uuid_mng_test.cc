#include "utils/uuid_mng.h"

#include "gtest/gtest.h"

TEST(UuidMng, Normal) {
  auto uuid_mng = certain::UuidMng::GetInstance();
  uuid_mng->Add(0x123456789, 0x123456789);
  ASSERT_TRUE(uuid_mng->Exist(0x123456789, 0x123456789));
  ASSERT_FALSE(uuid_mng->Exist(0x123456789, 0x123456788));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
