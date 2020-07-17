#include "network/msg_header.h"

#include "gtest/gtest.h"

TEST(MsgHeaderTest, Basic) {
  certain::MsgHeader header(1);
  header.version = 2;
  header.len = 123456789;
  header.checksum = 1234567890;
  char buffer[certain::kMsgHeaderSize];
  header.SerializeTo(buffer);
  certain::MsgHeader header2(11);
  header2.ParseFrom(buffer);
  ASSERT_EQ(header2.magic_num, certain::kMagicNum);
  ASSERT_EQ(header2.msg_id, 1);
  ASSERT_EQ(header2.version, 2);
  ASSERT_EQ(header2.len, 123456789);
  ASSERT_EQ(header2.checksum, 1234567890);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
