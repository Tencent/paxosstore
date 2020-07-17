#include "src/msg_worker.h"

#include "default/log_impl.h"
#include "gtest/gtest.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "src/conn_mng.h"
#include "src/conn_worker.h"

namespace {
certain::InetAddr addr("127.0.0.1", 8080);
};

class MsgWorkerTest : public testing::Test {
 public:
  MsgWorkerTest() : conn_worker_(nullptr) {}

  virtual void SetUp() override {
    conn_mng_ = certain::ConnMng::GetInstance();
    conn_mng_->Init(addr.ToString(), 1);
    options_.set_local_addr(addr.ToString());
    conn_worker_ = new certain::ConnWorker(&options_);
    conn_worker_->Start();
  }

  virtual void TearDown() override {
    conn_worker_->set_exit_flag(true);
    conn_worker_->WaitExit();

    delete conn_worker_;
    conn_mng_->Destroy();
  }

 protected:
  certain::ConnWorker* conn_worker_;
  certain::Options options_;
  certain::ConnMng* conn_mng_;
};

class MockRoute : public certain::Route {
 public:
  virtual std::string GetLocalAddr() override { return ""; }

  virtual int GetLocalAcceptorId(uint64_t entity_id,
                                 uint32_t* acceptor_id) override {
    *acceptor_id = entity_id % 2;
    return 0;
  }

  virtual int GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                              uint64_t* addr_id) override {
    *addr_id = ::addr.GetAddrId();  // server addr id
    return 0;
  }
};

TEST_F(MsgWorkerTest, Normal) {
  certain::Options options;
  options.set_local_addr(addr.ToString());
  options.set_msg_worker_num(1);
  options.set_entity_worker_num(1);

  static MockRoute route;
  certain::Wrapper::GetInstance()->Init(&options, &route, nullptr, nullptr);

  certain::MsgWorker worker(&options, 0);
  worker.Start();

  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetEntityReqQueueByIdx(0);
  ASSERT_EQ(queue->Size(), 0);

  auto cmd = std::make_unique<certain::PaxosCmd>();
  cmd->set_entity_id(1);
  cmd->set_entry(233);
  worker.GoAndDeleteIfFailed(std::move(cmd));

  poll(nullptr, 0, 30);
  ASSERT_EQ(queue->Size(), 1);

  std::unique_ptr<certain::CmdBase> out;
  ASSERT_EQ(queue->PopByOneThread(&out), 0);
  ASSERT_EQ(out->entry(), 233);

  ASSERT_EQ(queue->Size(), 0);
  for (int i = 0; i < 1000; ++i) {
    worker.GoAndDeleteIfFailed(std::make_unique<certain::PaxosCmd>());
  }
  poll(nullptr, 0, 100);
  ASSERT_EQ(queue->Size(), 1000);

  worker.set_exit_flag(true);
  worker.WaitExit();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
