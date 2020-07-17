#include "src/entity_helper.h"

#include "gtest/gtest.h"

class MockRoute : public certain::Route {
 public:
  virtual std::string GetLocalAddr() override { return ""; }

  virtual int GetLocalAcceptorId(uint64_t entity_id,
                                 uint32_t* acceptor_id) override {
    *acceptor_id = entity_id % 3;
    return 0;
  }

  virtual int GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                              uint64_t* addr_id) override {
    return -1;
  }
};

class EntityHelperTest : public testing::Test {
 public:
  virtual void SetUp() override {
    options_.set_msg_worker_num(1);
    options_.set_entity_worker_num(1);
    options_.set_plog_worker_num(1);
    options_.set_plog_readonly_worker_num(1);
    options_.set_catchup_worker_num(1);
    certain::AsyncQueueMng::GetInstance()->Init(&options_);
  }

  virtual void TearDown() override {
    certain::AsyncQueueMng::GetInstance()->Destroy();
  }

 protected:
  certain::Options options_;
  MockRoute route_;
};

TEST_F(EntityHelperTest, HandleClientCmd_Timeout) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0u, user_rsp_queue, &route_);

  auto write = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write->set_entity_id(123);
  write->set_entry(200);
  write->set_value("hello");

  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(write), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  auto write1 = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write1->set_entity_id(123);
  write1->set_entry(200);
  write1->set_value("hello");

  ASSERT_EQ(helper.HandleClientCmd(write1), certain::kRetCodeClientCmdConflict);

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  std::unique_ptr<certain::CmdBase> cmd;
  ASSERT_EQ(user_rsp_queue->PopByOneThread(&cmd), 0);
}

using certain::unique_cast;
#define CERTAIN_TEST_POP_FROM_QUEUE(var, que)                             \
  std::unique_ptr<certain::PaxosCmd> var;                                 \
  ASSERT_EQ(que->PopByOneThread(&unique_cast<certain::CmdBase>(var)), 0); \
  ASSERT_TRUE(var != nullptr);

TEST_F(EntityHelperTest, HandleClientCmd_WriteOk) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_req_queue = queue_mng->GetPlogReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto write = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write->set_entity_id(10000);
  write->set_entry(200);
  write->set_value("hello");

  std::unique_ptr<certain::CmdBase> cmd;
  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(write), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(10000, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 10000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, plog_req_queue);
  // not enable pre_auth
  ASSERT_EQ(pcmd2->local_entry_record().accepted_num(), 0);
  ASSERT_EQ(pcmd2->local_entry_record().promised_num(), local_acceptor_id + 4);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd2), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  int ret = 0;
  for (uint32_t acceptor_id = 0; acceptor_id <= 2; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd3, msg_req_queue);
    ASSERT_EQ(pcmd3->local_acceptor_id(), local_acceptor_id);
    ASSERT_EQ(pcmd3->peer_acceptor_id(), acceptor_id);
    ASSERT_TRUE(pcmd3->SwitchToLocalView(acceptor_id));
    certain::EntryRecord record = pcmd3->local_entry_record();
    ASSERT_EQ(record.promised_num(), 0);
    record.set_promised_num(local_acceptor_id + 4);
    pcmd3->set_local_entry_record(record);
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd3), ret);
  }

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd4, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd4), 0);
  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  // The peer just accept the value, but not set chosen flag.
  int ret1 = 0;
  for (uint32_t acceptor_id = 0; acceptor_id <= 2; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd5, msg_req_queue);
    certain::EntryRecord record = pcmd5->local_entry_record();
    ASSERT_EQ(record.accepted_num(), local_acceptor_id + 4);
    ASSERT_STREQ(record.value().c_str(), "hello");
    pcmd5->set_peer_entry_record(record);
    ASSERT_TRUE(pcmd5->SwitchToLocalView(pcmd5->peer_acceptor_id()));
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd5), ret1);
  }

  ASSERT_EQ(msg_req_queue->Size(), 1);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd6, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd6), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  cmd = nullptr;
  user_rsp_queue->PopByOneThread(&cmd);
  ASSERT_EQ(cmd->result(), 0);
}

TEST_F(EntityHelperTest, HandleClientCmd_WriteOkWithPeerChosen) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_req_queue = queue_mng->GetPlogReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto write = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write->set_entity_id(10000);
  write->set_entry(200);
  write->set_value("hello");

  std::unique_ptr<certain::CmdBase> cmd;
  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(write), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(10000, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 10000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, plog_req_queue);
  // not enable pre_auth
  ASSERT_EQ(pcmd2->local_entry_record().accepted_num(), 0);
  ASSERT_EQ(pcmd2->local_entry_record().promised_num(), local_acceptor_id + 4);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd2), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  int ret = 0;
  for (uint32_t acceptor_id = 0; acceptor_id <= 2; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd3, msg_req_queue);
    ASSERT_EQ(pcmd3->local_acceptor_id(), local_acceptor_id);
    ASSERT_EQ(pcmd3->peer_acceptor_id(), acceptor_id);
    ASSERT_TRUE(pcmd3->SwitchToLocalView(acceptor_id));
    certain::EntryRecord record = pcmd3->local_entry_record();
    ASSERT_EQ(record.promised_num(), 0);
    record.set_promised_num(local_acceptor_id + 4);
    pcmd3->set_local_entry_record(record);
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd3), ret);
  }

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd4, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd4), 0);
  ASSERT_EQ(plog_req_queue->Size(), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  // The peer just accept the value, but not set chosen flag.
  int ret1 = 0;
  for (uint32_t acceptor_id = 0; acceptor_id <= 2; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd5, msg_req_queue);
    certain::EntryRecord record = pcmd5->local_entry_record();
    record.set_chosen(true);  // with peer chosen
    ASSERT_EQ(record.accepted_num(), local_acceptor_id + 4);
    ASSERT_STREQ(record.value().c_str(), "hello");
    pcmd5->set_peer_entry_record(record);
    ASSERT_TRUE(pcmd5->SwitchToLocalView(pcmd5->peer_acceptor_id()));
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd5), ret1);
  }

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(plog_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd6, plog_req_queue);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd6), 0);
  ASSERT_EQ(msg_req_queue->Size(), 0);  // Not need to sync the chosen entry.

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  cmd = nullptr;
  user_rsp_queue->PopByOneThread(&cmd);
  ASSERT_EQ(cmd->result(), 0);
}

TEST_F(EntityHelperTest, HandleClientCmd_ReadOk) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto read = std::make_unique<certain::ClientCmd>(certain::kCmdRead);
  read->set_entity_id(20000);
  read->set_entry(200);

  std::unique_ptr<certain::CmdBase> cmd;
  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(read), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(20000, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 20000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  ASSERT_EQ(user_rsp_queue->Size(), 0);
  for (uint32_t acceptor_id = 0; acceptor_id <= 2; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, msg_req_queue);
    ASSERT_EQ(pcmd2->local_acceptor_id(), local_acceptor_id);
    ASSERT_EQ(pcmd2->peer_acceptor_id(), acceptor_id);
    ASSERT_TRUE(pcmd2->SwitchToLocalView(acceptor_id));
    ASSERT_TRUE(pcmd2->check_empty());
    pcmd2->set_check_empty(false);
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd2), 0);
  }

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  cmd = nullptr;
  user_rsp_queue->PopByOneThread(&cmd);
  ASSERT_EQ(cmd->result(), 0);
}

TEST_F(EntityHelperTest, HandlePaxosCmd_ReadRemoteNewer) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto read = std::make_unique<certain::ClientCmd>(certain::kCmdRead);
  read->set_entity_id(20000);
  read->set_entry(200);

  std::unique_ptr<certain::CmdBase> cmd;
  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(read), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(20000, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 20000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(msg_req_queue->Size(), 2);

  ASSERT_EQ(user_rsp_queue->Size(), 0);
  for (uint32_t acceptor_id = 0; acceptor_id <= 1; ++acceptor_id) {
    if (acceptor_id == local_acceptor_id) {
      continue;
    }
    CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, msg_req_queue);
    ASSERT_EQ(pcmd2->local_acceptor_id(), local_acceptor_id);
    ASSERT_EQ(pcmd2->peer_acceptor_id(), acceptor_id);
    ASSERT_TRUE(pcmd2->SwitchToLocalView(acceptor_id));
    ASSERT_TRUE(pcmd2->check_empty());
    pcmd2->set_check_empty(false);
    pcmd2->set_result(certain::kRetCodeFastFailed);
    ASSERT_EQ(helper.HandlePaxosCmd(pcmd2), certain::kRetCodeFastFailed);
  }

  ASSERT_EQ(user_rsp_queue->Size(), 1);
  std::unique_ptr<certain::CmdBase> rsp;
  ASSERT_EQ(user_rsp_queue->PopByOneThread(&rsp), 0);
  ASSERT_EQ(rsp->result(), certain::kRetCodeFastFailed);
}

TEST_F(EntityHelperTest, HandlePaxosCmd_CheckEmpty) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);
  auto pcmd = std::make_unique<certain::PaxosCmd>(30000, 200);
  pcmd->set_check_empty(true);
  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(pcmd->entity_id(), &local_acceptor_id);
  pcmd->set_local_acceptor_id(local_acceptor_id);
  pcmd->set_peer_acceptor_id((local_acceptor_id + 1) % 3);

  ASSERT_EQ(helper.HandlePaxosCmd(pcmd), 0);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 30000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(199);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(msg_req_queue->Size(), 1);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, msg_req_queue);
  ASSERT_FALSE(pcmd2->check_empty());
  ASSERT_EQ(pcmd2->result(), 0);
}

TEST_F(EntityHelperTest, HandlePaxosCmd_CheckEmpty1) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto msg_req_queue = queue_mng->GetMsgReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);
  auto pcmd = std::make_unique<certain::PaxosCmd>(30000, 200);
  pcmd->set_check_empty(true);
  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(pcmd->entity_id(), &local_acceptor_id);
  pcmd->set_local_acceptor_id(local_acceptor_id);
  pcmd->set_peer_acceptor_id((local_acceptor_id + 1) % 3);

  ASSERT_EQ(helper.HandlePaxosCmd(pcmd), 0);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 30000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(200);
  pcmd1->set_max_committed_entry(200);

  ASSERT_EQ(msg_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(msg_req_queue->Size(), 1);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, msg_req_queue);
  ASSERT_FALSE(pcmd2->check_empty());
  ASSERT_EQ(pcmd2->result(), certain::kRetCodeFastFailed);
}

TEST_F(EntityHelperTest, HandlePaxosCmd_Catchup) {
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_rsp_queue = queue_mng->GetUserRspQueueByIdx(0);
  auto catchup_req_queue = queue_mng->GetCatchupReqQueueByIdx(0);
  auto plog_readonly_req_queue = queue_mng->GetPlogReadonlyReqQueueByIdx(0);

  certain::EntityHelper helper(&options_, 0, user_rsp_queue, &route_);

  auto write = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
  write->set_entity_id(10000);
  write->set_entry(200);
  write->set_value("hello");

  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandleClientCmd(write), certain::kRetCodeEntityLoading);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 1);

  uint32_t local_acceptor_id = -1;
  route_.GetLocalAcceptorId(10000, &local_acceptor_id);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd1, plog_readonly_req_queue);
  ASSERT_EQ(pcmd1->entity_id(), 10000);
  ASSERT_TRUE(pcmd1->plog_load());
  pcmd1->set_entry(202);
  pcmd1->set_max_committed_entry(199);

  ASSERT_EQ(user_rsp_queue->Size(), 0);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 0);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd1), 0);
  ASSERT_EQ(user_rsp_queue->Size(), 1);
  std::unique_ptr<certain::CmdBase> cmd;
  ASSERT_EQ(user_rsp_queue->PopByOneThread(&cmd), 0);
  ASSERT_EQ(cmd->result(), certain::kRetCodeEntryNotMatch);
  ASSERT_EQ(plog_readonly_req_queue->Size(), 2);

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd2, plog_readonly_req_queue);
  ASSERT_EQ(pcmd2->entry(), 200);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd2), 0);
  ASSERT_EQ(catchup_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd20, catchup_req_queue);
  ASSERT_EQ(pcmd20->entry(), 200);
  ASSERT_TRUE(pcmd20->catchup());

  CERTAIN_TEST_POP_FROM_QUEUE(pcmd3, plog_readonly_req_queue);
  ASSERT_EQ(pcmd3->entry(), 201);
  ASSERT_EQ(helper.HandlePlogRspCmd(pcmd3), 0);
  ASSERT_EQ(catchup_req_queue->Size(), 1);
  CERTAIN_TEST_POP_FROM_QUEUE(pcmd30, catchup_req_queue);
  ASSERT_EQ(pcmd30->entry(), 201);
  ASSERT_TRUE(pcmd30->catchup());
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
