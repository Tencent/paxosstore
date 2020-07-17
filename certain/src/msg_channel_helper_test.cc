#include "src/msg_channel_helper.h"

#include "gtest/gtest.h"
#include "src/conn_mng.h"
#include "src/conn_worker.h"

class MsgChannelHelperTest : public testing::Test,
                             public certain::MsgCallBackBase {
 public:
  MsgChannelHelperTest() : addr0_("127.0.0.1", 8181), conn_worker_(nullptr) {}

  virtual void SetUp() override {
    conn_mng_ = certain::ConnMng::GetInstance();
    conn_mng_->Init(addr0_.ToString(), 1);
    options_.set_local_addr("127.0.0.1:8181");
    conn_worker_ = new certain::ConnWorker(&options_);
    conn_worker_->Start();
  }

  virtual void TearDown() override {
    conn_worker_->set_exit_flag(true);
    conn_worker_->WaitExit();

    delete conn_worker_;
    conn_mng_->Destroy();
  }

  virtual void OnMessage(uint8_t msg_id, const char* buffer,
                         uint32_t len) override {}

 protected:
  certain::InetAddr addr0_;
  certain::ConnWorker* conn_worker_;
  certain::Options options_;
  certain::ConnMng* conn_mng_;
};

class MockHander : public certain::HandlerBase {
 public:
  virtual int HandleRead(certain::FdObj* fd_obj) override { return 0; }
  virtual int HandleWrite(certain::FdObj* fd_obj) override { return 0; }
};

TEST_F(MsgChannelHelperTest, Normal) {
  certain::Options options;
  options.set_active_timeout_sec(0);
  certain::Poller poller;
  certain::MsgChannelHelper helper(&options, &poller);

  static MockHander hander;
  static certain::SharedLimiter rbuf_shared_limiter(1 << 20);
  static certain::SharedLimiter wbuf_shared_limiter(2 << 20);

  {
    auto socket = std::make_unique<certain::TcpSocket>();
    ASSERT_EQ(socket->InitSocket(), 0);
    ASSERT_EQ(socket->Connect(addr0_), certain::kNetWorkInProgress);

    // will be delete in RemoveMsgChannel
    auto channel_own = std::make_unique<certain::MsgChannel>(
        std::move(socket), &hander, &rbuf_shared_limiter, &wbuf_shared_limiter,
        this);
    auto channel = channel_own.get();

    ASSERT_EQ(helper.GetMsgChannel(addr0_.GetAddrId()), nullptr);

    ASSERT_EQ(helper.AddMsgChannel(channel_own), 0);
    ASSERT_EQ(helper.GetMsgChannel(addr0_.GetAddrId()), channel);
    ASSERT_EQ(helper.GetMsgChannel(addr0_.GetAddrId()), channel);

    ASSERT_EQ(helper.RefreshMsgChannel(channel), 0);
    ASSERT_EQ(helper.GetMsgChannel(addr0_.GetAddrId()), channel);

    ASSERT_EQ(helper.RemoveMsgChannel(channel), 0);
    ASSERT_EQ(helper.GetMsgChannel(addr0_.GetAddrId()), nullptr);
  }

  {
    auto socket = std::make_unique<certain::TcpSocket>();
    ASSERT_EQ(socket->InitSocket(), 0);
    ASSERT_EQ(socket->Connect(addr0_), certain::kNetWorkInProgress);

    // will be delete in Clean
    auto channel_own = std::make_unique<certain::MsgChannel>(
        std::move(socket), &hander, &rbuf_shared_limiter, &wbuf_shared_limiter,
        this);
    auto channel = channel_own.get();

    ASSERT_EQ(helper.AddMsgChannel(channel_own), 0);
    ASSERT_EQ(helper.AddWritableChannel(channel), 0);
    channel->set_writable(true);
    ASSERT_EQ(helper.FlushMsgChannel(), 0);
    ASSERT_EQ(helper.CleanUnactiveMsgChannel(), 0);
    ASSERT_EQ(helper.GetMsgChannel(addr0_.GetAddrId()), nullptr);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
