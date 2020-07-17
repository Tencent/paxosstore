#include "src/common.h"

#include "gtest/gtest.h"

namespace certain {

TEST(CommonTest, Test) {
  EntryRecord record;
  std::string str = EntryRecordToString(record);
  ASSERT_TRUE(str.size() > 0);
}

}  // namespace certain

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
