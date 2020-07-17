#include "gtest/gtest.h"
#include "src/entity_helper.h"
#include "utils/memory.h"

const uint32_t kAcceptorNum = 5;

class MockRoute : public certain::Route {
 public:
  virtual std::string GetLocalAddr() override { return ""; }

  virtual int GetLocalAcceptorId(uint64_t entity_id,
                                 uint32_t* acceptor_id) override {
    *acceptor_id = entity_id % kAcceptorNum;
    return 0;
  }

  virtual int GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                              uint64_t* addr_id) override {
    return -1;
  }
};

class FiveReplicaTest : public testing::Test {
 public:
  virtual void SetUp() override {
    options_.set_acceptor_num(5);
    options_.set_msg_worker_num(1);
    options_.set_entity_worker_num(1);
    options_.set_plog_worker_num(1);
    options_.set_plog_readonly_worker_num(1);
    certain::AsyncQueueMng::GetInstance()->Init(&options_);
  }

  virtual void TearDown() override {
    certain::AsyncQueueMng::GetInstance()->Destroy();
  }

 protected:
  certain::Options options_;
  MockRoute route_;
};

using certain::unique_cast;
#define CERTAIN_TEST_POP_FROM_QUEUE(var, que)                             \
  std::unique_ptr<certain::PaxosCmd> var;                                 \
  ASSERT_EQ(que->PopByOneThread(&unique_cast<certain::CmdBase>(var)), 0); \
  ASSERT_TRUE(var != nullptr);

TEST_F(FiveReplicaTest, Write) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_req_queue = queue_mng->GetPlogReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto write = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write->set_entity_id(10001);
  write->set_entry(200);
  write->set_value("hello");

  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(write), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(10001, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 10001);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, plog_req_queue);
  // not enable pre_auth
  ASSERT_EQ(pcmd2->local_entry_record().accepted_num(), 0);
  ASSERT_EQ(pcmd2->local_entry_record().promised_num(),
            local_acceptor_id + kAcceptorNum + 1);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd2), 0);
  ASSERT_EQ(msg_req_queue->Size(), kAcceptorNum - 1);

  int ret = 0;
  int count = 0;
  for (uint32_t acceptor_id = 0; acceptor_id < kAcceptorNum; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd3, msg_req_queue);
    ASSERT_EQ(pcmd3->local_acceptor_id(), local_acceptor_id);
    ASSERT_EQ(pcmd3->peer_acceptor_id(), acceptor_id);
    ASSERT_TRUE(pcmd3->SwitchToLocalView(acceptor_id));
    certain::EntryRecord record = pcmd3->local_entry_record();
    ASSERT_EQ(record.promised_num(), 0);
    record.set_promised_num(local_acceptor_id + kAcceptorNum + 1);
    pcmd3->set_local_entry_record(record);
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd3), ret);
    ++count;
  }

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd4, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd4), 0);
  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(msg_req_queue->Size(), kAcceptorNum - 1);

  // The peer just accept the value, but not set chosen flag.
  int ret1 = 0;
  int count1 = 0;
  for (uint32_t acceptor_id = 0; acceptor_id < kAcceptorNum; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd5, msg_req_queue);
    certain::EntryRecord record = pcmd5->local_entry_record();
    ASSERT_EQ(record.accepted_num(), local_acceptor_id + kAcceptorNum + 1);
    ASSERT_STREQ(record.value().c_str(), "hello");
    pcmd5->set_peer_entry_record(record);
    ASSERT_TRUE(pcmd5->SwitchToLocalView(pcmd5->peer_acceptor_id()));
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd5), ret1);
    ++count1;
  }

  ASSERT_EQ(msg_req_queue->Size(), 2);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd6, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd6), 0);
  ASSERT_EQ(msg_req_queue->Size(), kAcceptorNum - 1);
  ASSERT_EQ(plog_req_queue->Size(), 0);

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  std::unique_ptr<certain::CmdBase> cmd;
  user_rsp_queue->PopByOneThread(&cmd);
  ASSERT_EQ(cmd->result(), 0);
}

TEST_F(FiveReplicaTest, WriteWithTwoDown) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_req_queue = queue_mng->GetPlogReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto write = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write->set_entity_id(10001);
  write->set_entry(200);
  write->set_value("hello");

  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(write), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(10001, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 10001);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, plog_req_queue);
  // not enable pre_auth
  ASSERT_EQ(pcmd2->local_entry_record().accepted_num(), 0);
  ASSERT_EQ(pcmd2->local_entry_record().promised_num(),
            local_acceptor_id + kAcceptorNum + 1);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd2), 0);
  ASSERT_EQ(msg_req_queue->Size(), kAcceptorNum - 1);

  int dropcnt = 2;  // Drop two messages.
  int ret = 0;
  int count = 0;
  for (uint32_t acceptor_id = 0; acceptor_id < kAcceptorNum; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd3, msg_req_queue);
    if (count + dropcnt == kAcceptorNum - 1) {
      continue;
    }
    ASSERT_EQ(pcmd3->local_acceptor_id(), local_acceptor_id);
    ASSERT_EQ(pcmd3->peer_acceptor_id(), acceptor_id);
    ASSERT_TRUE(pcmd3->SwitchToLocalView(acceptor_id));
    certain::EntryRecord record = pcmd3->local_entry_record();
    ASSERT_EQ(record.promised_num(), 0);
    record.set_promised_num(local_acceptor_id + kAcceptorNum + 1);
    pcmd3->set_local_entry_record(record);
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd3), ret);
    ++count;
  }

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd4, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd4), 0);
  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(msg_req_queue->Size(), kAcceptorNum - 1 - dropcnt);

  // The peer just accept the value, but not set chosen flag.
  int ret1 = 0;
  int count1 = 0;
  for (uint32_t acceptor_id = 0; acceptor_id < kAcceptorNum; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    if (count1 + dropcnt == kAcceptorNum - 1) {
      break;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd5, msg_req_queue);
    certain::EntryRecord record = pcmd5->local_entry_record();
    ASSERT_EQ(record.accepted_num(), local_acceptor_id + kAcceptorNum + 1);
    ASSERT_STREQ(record.value().c_str(), "hello");
    pcmd5->set_peer_entry_record(record);
    ASSERT_TRUE(pcmd5->SwitchToLocalView(pcmd5->peer_acceptor_id()));
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd5), ret1);
    ++count1;
  }

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd6, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd6), 0);
  ASSERT_EQ(msg_req_queue->Size(), kAcceptorNum - 1 - dropcnt);
  ASSERT_EQ(plog_req_queue->Size(), 0);

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  std::unique_ptr<certain::CmdBase> cmd;
  user_rsp_queue->PopByOneThread(&cmd);
  ASSERT_EQ(cmd->result(), 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
