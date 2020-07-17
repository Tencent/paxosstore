#include "src/msg_serialize.h"

#include "gtest/gtest.h"

namespace certain {

TEST(MsgSerialize, Normal) {
  PaxosCmd cmd;
  cmd.set_entity_id(233);
  cmd.set_entry(666);

  MsgSerialize serializer(&cmd);
  ASSERT_EQ(serializer.GetMsgId(), kCmdPaxos);
  ASSERT_GT(serializer.ByteSize(), 0);

  std::string buffer(serializer.ByteSize(), '\0');
  ASSERT_TRUE(serializer.SerializeTo(&buffer[0], serializer.ByteSize()));

  PaxosCmd out;
  ASSERT_TRUE(out.ParseFromBuffer(buffer.data(), buffer.size()));
  ASSERT_EQ(out.entity_id(), cmd.entity_id());
  ASSERT_EQ(out.entry(), cmd.entry());
}

}  // namespace certain

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
