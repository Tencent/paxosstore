#include "src/conn_worker.h"

#include "gtest/gtest.h"
#include "src/conn_mng.h"

class ConnWorkerTest : public testing::Test {
 public:
  ConnWorkerTest() : addr0_("127.0.0.1", 28080), conn_worker_(nullptr) {}

  virtual void SetUp() override {
    conn_mng_ = certain::ConnMng::GetInstance();
    conn_mng_->Init(addr0_.ToString(), 1);
    options_.set_local_addr("127.0.0.1:28080");
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
  certain::InetAddr addr0_;
  certain::ConnWorker* conn_worker_;
  certain::Options options_;
  certain::ConnMng* conn_mng_;
};

TEST_F(ConnWorkerTest, Normal) {
  int ret;
  usleep(10000);  // Wait to start listening on the addr.

  certain::TcpSocket tcp;
  ASSERT_EQ(tcp.InitSocket(), 0);
  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());
  ASSERT_EQ(tcp.Connect(addr0_), certain::kNetWorkInProgress);
  CERTAIN_LOG_INFO("tcp %s", tcp.ToString().c_str());

  usleep(10000);  // Wait to accept by the peer.
  ASSERT_EQ(tcp.CheckIfValid(), 0);
  ASSERT_FALSE(conn_mng_->IsBalanceTableEmpty());
  auto tcp2 = conn_mng_->TakeByMultiThread(0);
  ASSERT_TRUE(tcp2 != nullptr);
  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());

  char buffer[10] = {0};
  ret = write(tcp2->fd(), "hello", 5);
  CERTAIN_LOG_INFO("tcp2 write %s ret %d errno %d", tcp2->ToString().c_str(),
                   ret, errno);
  ret = read(tcp.fd(), buffer, 10);
  CERTAIN_LOG_INFO("tcp read ret %d errno %d", ret, errno);
  ASSERT_EQ(ret, 5);
  ASSERT_STREQ(buffer, "hello");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
