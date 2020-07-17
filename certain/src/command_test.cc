#include "src/command.h"

#include "gtest/gtest.h"
#include "utils/memory.h"

TEST(CommandTest, PaxosCmd) {
  certain::PaxosCmd cmd;

  cmd.set_local_acceptor_id(1);
  cmd.set_peer_acceptor_id(2);

  certain::EntryRecord local;
  local.set_prepared_num(3);
  cmd.set_local_entry_record(local);
  certain::EntryRecord peer;
  peer.set_prepared_num(4);
  cmd.set_peer_entry_record(peer);

  cmd.set_max_chosen_entry(5);
  cmd.set_check_empty(true);

  CERTAIN_LOG_INFO("cmd %s", cmd.ToString().c_str());

  uint32_t real_len = 0;
  char buffer[1024];
  ASSERT_TRUE(cmd.SerializeToBuffer(buffer, 1024, real_len));

  auto base = certain::CmdFactory::GetInstance()->CreateCmd(certain::kCmdPaxos,
                                                            buffer, real_len);
  ASSERT_TRUE(base != nullptr);
  auto& new_cmd = certain::unique_cast<certain::PaxosCmd>(base);

  ASSERT_EQ(new_cmd->local_acceptor_id(), 1);
  ASSERT_EQ(new_cmd->peer_acceptor_id(), 2);

  ASSERT_EQ(new_cmd->local_entry_record().prepared_num(), 3);
  ASSERT_EQ(new_cmd->peer_entry_record().prepared_num(), 4);

  ASSERT_EQ(new_cmd->max_chosen_entry(), 5);
  ASSERT_TRUE(new_cmd->check_empty());
}

TEST(CommandTest, RangeCatchupCmd) {
  certain::RangeCatchupCmd cmd;

  cmd.set_local_acceptor_id(1);
  cmd.set_peer_acceptor_id(2);

  cmd.set_begin_entry(3);
  cmd.set_end_entry(4);

  CERTAIN_LOG_INFO("cmd %s", cmd.ToString().c_str());

  uint32_t size = cmd.SerializedByteSize();
  std::string buffer(size, '\0');
  ASSERT_TRUE(cmd.SerializeToBuffer(&buffer[0], 1024, size));

  auto base = certain::CmdFactory::GetInstance()->CreateCmd(
      certain::kCmdRangeCatchup, buffer.data(), buffer.size());
  ASSERT_TRUE(base != nullptr);
  auto& new_cmd = certain::unique_cast<certain::RangeCatchupCmd>(base);

  ASSERT_EQ(new_cmd->local_acceptor_id(), 1);
  ASSERT_EQ(new_cmd->peer_acceptor_id(), 2);

  ASSERT_EQ(new_cmd->begin_entry(), 3);
  ASSERT_EQ(new_cmd->end_entry(), 4);

  ASSERT_TRUE(new_cmd->SwitchToLocalView(2));
}

TEST(CommandTest, CreateCmdErr) {
  char buffer[1024];
  auto base = certain::CmdFactory::GetInstance()->CreateCmd(certain::kCmdPaxos,
                                                            buffer, 1024);
  ASSERT_TRUE(base == nullptr);
}

TEST(CommandTest, ClientCmd) {
  certain::ClientCmd cmd(certain::kCmdReplay);
  CERTAIN_LOG_INFO("cmd %s", cmd.ToString().c_str());
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
