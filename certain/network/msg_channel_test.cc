#include "network/msg_channel.h"

#include "gtest/gtest.h"
#include "utils/memory.h"

class MsgCallBackTest : public certain::MsgCallBackBase {
 public:
  static std::vector<std::string> buffers_;

  virtual ~MsgCallBackTest() {}

  virtual void OnMessage(uint8_t msg_id, const char* buffer,
                         uint32_t len) override {
    std::string str(buffer, len);
    // printf("OnMessage msg_id %u, buffer %s len %u\n", msg_id, str.c_str(),
    // len);
    buffers_.push_back(str);
  }
};

class SerializeCallBackTest : public certain::SerializeCallBackBase {
 public:
  static std::vector<std::string> buffers_;

  SerializeCallBackTest(uint8_t msg_id, int32_t len) {
    assert(msg_id < 26);
    msg_id_ = msg_id;
    len_ = len;
  }

  virtual uint8_t GetMsgId() override { return msg_id_; }

  virtual int32_t ByteSize() override { return len_; }

  virtual bool SerializeTo(char* buffer, uint32_t len) override {
    assert(len == static_cast<uint32_t>(len_));  // TODO int32 vs uint32
    std::string str(len_, 'a' + msg_id_);
    memcpy(buffer, str.data(), str.size());
    // printf("SerializeTo msg_id %u, buffer %s len %u\n", msg_id_,
    // str.c_str(), len);
    buffers_.push_back(str);
    return true;
  }

  std::vector<std::string> buffers() { return buffers_; }

 private:
  uint8_t msg_id_;
  int32_t len_;
};

std::vector<std::string> SerializeCallBackTest::buffers_;
std::vector<std::string> MsgCallBackTest::buffers_;

class HandlerTest : public certain::HandlerBase {
 public:
  virtual ~HandlerTest() {}

  virtual int HandleRead(certain::FdObj* fd_obj) override { return 0; }

  virtual int HandleWrite(certain::FdObj* fd_obj) override { return 0; }
};

class MsgChannelTest : public testing::Test {
 public:
  virtual void SetUp() override {
    static int port = 33333;
    certain::InetAddr addr1("127.0.0.1", port++);
    certain::InetAddr addr2("127.0.0.1", port++);

    certain::TcpSocket tcp1;
    assert(tcp1.InitSocket() == 0);
    assert(tcp1.Bind(addr1) == 0);
    assert(tcp1.Listen() == 0);
    std::cout << tcp1.ToString() << std::endl;

    auto tcp2 = std::make_unique<certain::TcpSocket>();
    assert(tcp2->InitSocket() == 0);
    assert(tcp2->Bind(addr2) == 0);
    assert(tcp2->Connect(addr1) == certain::kNetWorkInProgress);
    std::cout << tcp2->ToString() << std::endl;

    certain::InetAddr addr;
    int fd = tcp1.Accept(addr);
    ASSERT_GT(fd, 0) << strerror(errno);
    assert(addr == addr2);

    auto tcp3 = std::make_unique<certain::TcpSocket>(fd, addr1, addr2);
    ASSERT_EQ(tcp3->InitSocket(), 0);
    std::cout << tcp3->ToString() << std::endl;

    rbuf_shared_limiter_ = new certain::SharedLimiter(1 << 20);
    wbuf_shared_limiter_ = new certain::SharedLimiter(2 << 20);

    handler_ = new HandlerTest;
    msg_cb_ = new MsgCallBackTest;

    channel1_ =
        new certain::MsgChannel(std::move(tcp2), handler_, rbuf_shared_limiter_,
                                wbuf_shared_limiter_, msg_cb_);
    channel2_ =
        new certain::MsgChannel(std::move(tcp3), handler_, rbuf_shared_limiter_,
                                wbuf_shared_limiter_, msg_cb_);
    std::cout << channel1_->ToString() << std::endl;
    std::cout << channel2_->ToString() << std::endl;
  }

  virtual void TearDown() override {
    delete channel2_;
    delete channel1_;
    delete msg_cb_;
    delete handler_;
    delete wbuf_shared_limiter_;
    delete rbuf_shared_limiter_;
    SerializeCallBackTest::buffers_.clear();
    MsgCallBackTest::buffers_.clear();
  }

 protected:
  certain::SharedLimiter* rbuf_shared_limiter_;
  certain::SharedLimiter* wbuf_shared_limiter_;
  certain::HandlerBase* handler_;
  certain::MsgCallBackBase* msg_cb_;
  certain::MsgChannel* channel1_;
  certain::MsgChannel* channel2_;
};

namespace certain {

TEST_F(MsgChannelTest, ParseAndSerialize) {
  uint32_t size = certain::kMsgHeaderSize * 3 + 6;
  char buffer[size];
  SerializeCallBackTest s1(1, 1);
  SerializeCallBackTest s2(2, 2);
  SerializeCallBackTest s3(3, 3);
  char* offset = buffer;
  ASSERT_TRUE(
      channel1_->SerializeMsg(&s1, offset, certain::kMsgHeaderSize + 1));
  offset += certain::kMsgHeaderSize + 1;
  ASSERT_TRUE(
      channel1_->SerializeMsg(&s2, offset, certain::kMsgHeaderSize + 2));
  offset += certain::kMsgHeaderSize + 2;
  ASSERT_TRUE(
      channel1_->SerializeMsg(&s3, offset, certain::kMsgHeaderSize + 3));

  uint32_t parsed_len = 0;
  ASSERT_TRUE(channel1_->ParseMsg(buffer, size, &parsed_len));
  ASSERT_EQ(parsed_len, size);
}

}  // namespace certain

TEST_F(MsgChannelTest, ReadMore) {
  uint32_t size = certain::kMsgHeaderSize * 3 + 6;
  char buffer[size];
  SerializeCallBackTest s1(1, 1);
  SerializeCallBackTest s2(2, 2);
  SerializeCallBackTest s3(3, 3);

  certain::SerializeCallBackBase* cbs[3];
  cbs[0] = &s1;
  cbs[1] = &s2;
  cbs[2] = &s3;

  ASSERT_TRUE(SerializeCallBackTest::buffers_.empty());
  ASSERT_TRUE(channel1_->Write(&cbs[0], 1));
  ASSERT_TRUE(channel1_->Write(&cbs[1], 2));
  ASSERT_FALSE(channel2_->readable());

  certain::Poller epoll_io;
  epoll_io.Add(channel1_);
  epoll_io.Add(channel2_);

  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel1_->writable());
  ASSERT_EQ(wbuf_shared_limiter_->curr_total_size(),
            6 + 3 * certain::kMsgHeaderSize);
  ASSERT_FALSE(channel1_->broken());
  channel1_->FlushBuffer();
  ASSERT_EQ(wbuf_shared_limiter_->curr_total_size(), 0);

  ASSERT_FALSE(channel2_->readable());

  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel2_->readable());
  channel2_->ReadMore(buffer, size);

  ASSERT_EQ(SerializeCallBackTest::buffers_.size(), 3);
  ASSERT_EQ(MsgCallBackTest::buffers_.size(), 3);

  ASSERT_STREQ(SerializeCallBackTest::buffers_[0].c_str(), "b");
  ASSERT_STREQ(SerializeCallBackTest::buffers_[1].c_str(), "cc");
  ASSERT_STREQ(SerializeCallBackTest::buffers_[2].c_str(), "ddd");

  ASSERT_EQ(SerializeCallBackTest::buffers_[0], MsgCallBackTest::buffers_[0]);
  ASSERT_EQ(SerializeCallBackTest::buffers_[1], MsgCallBackTest::buffers_[1]);
  ASSERT_EQ(SerializeCallBackTest::buffers_[2], MsgCallBackTest::buffers_[2]);
}

TEST_F(MsgChannelTest, WriteMoreThanBatchSize) {
  const uint32_t data_size = certain::kMaxBatchSize + certain::kMaxBatchSize -
                             certain::kMsgHeaderSize - 1 + 1;
  const uint32_t pkg_size = data_size + 3 * certain::kMsgHeaderSize;
  SerializeCallBackTest s1(1, certain::kMaxBatchSize);
  SerializeCallBackTest s2(
      2, certain::kMaxBatchSize - certain::kMsgHeaderSize - 1);
  SerializeCallBackTest s3(3, 1);
  certain::SerializeCallBackBase* cbs[3] = {&s1, &s2, &s3};
  ASSERT_TRUE(SerializeCallBackTest::buffers_.empty());
  ASSERT_TRUE(channel1_->Write(&cbs[0], 3));
  ASSERT_EQ(SerializeCallBackTest::buffers_.size(), 3);
  ASSERT_EQ(SerializeCallBackTest::buffers_[0].size(), certain::kMaxBatchSize);
  ASSERT_EQ(SerializeCallBackTest::buffers_[1].size(),
            certain::kMaxBatchSize - certain::kMsgHeaderSize - 1);
  ASSERT_EQ(SerializeCallBackTest::buffers_[2].size(), 1);
  ASSERT_FALSE(channel1_->writable());
  ASSERT_FALSE(channel2_->readable());

  certain::Poller epoll_io;
  epoll_io.Add(channel1_);
  epoll_io.Add(channel2_);

  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel1_->writable());
  ASSERT_EQ(wbuf_shared_limiter_->curr_total_size(), pkg_size);
  channel1_->FlushBuffer();
  ASSERT_EQ(0, wbuf_shared_limiter_->curr_total_size());

  ASSERT_FALSE(channel2_->readable());
  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel2_->readable());

  std::unique_ptr<char> rbuf(new char[pkg_size]);
  channel2_->ReadMore(rbuf.get(), pkg_size);

  ASSERT_EQ(MsgCallBackTest::buffers_.size(),
            SerializeCallBackTest::buffers_.size());
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(SerializeCallBackTest::buffers_[i], MsgCallBackTest::buffers_[i])
        << i;
  }
}

TEST_F(MsgChannelTest, ReadWhenPeerClosed) {
  const uint32_t data_size = certain::kMaxBatchSize;
  const uint32_t pkg_size = data_size + certain::kMsgHeaderSize;
  SerializeCallBackTest s(1, data_size);
  certain::SerializeCallBackBase* cb = &s;
  ASSERT_TRUE(channel1_->Write(&cb, 1));

  ASSERT_FALSE(channel1_->writable());
  ASSERT_FALSE(channel1_->readable());
  ASSERT_FALSE(channel2_->writable());
  ASSERT_FALSE(channel2_->readable());

  certain::Poller epoll_io;
  epoll_io.Add(channel1_);
  epoll_io.Add(channel2_);

  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel1_->writable());
  ASSERT_EQ(wbuf_shared_limiter_->curr_total_size(), pkg_size);
  channel1_->FlushBuffer();
  ASSERT_EQ(wbuf_shared_limiter_->curr_total_size(), 0);

  ASSERT_FALSE(channel2_->readable());
  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel2_->readable());
  std::unique_ptr<char> rbuf(new char[pkg_size + 1]);
  channel2_->ReadMore(rbuf.get(), pkg_size);

  ASSERT_TRUE(channel2_->readable());
  ASSERT_FALSE(channel2_->broken());
  channel2_->ReadMore(rbuf.get(), 1);
  ASSERT_FALSE(channel2_->readable());
  ASSERT_FALSE(channel2_->broken());

  ASSERT_TRUE(channel1_->Write(&cb, 1));
  channel1_->FlushBuffer();

  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel2_->readable());
  channel2_->ReadMore(rbuf.get(), pkg_size + 1);
  ASSERT_FALSE(channel2_->broken());
  ASSERT_FALSE(channel2_->readable());

  ASSERT_TRUE(channel1_->Write(&cb, 1));
  channel1_->FlushBuffer();
  ASSERT_EQ(epoll_io.Remove(channel1_), 0);
  delete channel1_;
  channel1_ = nullptr;

  epoll_io.RunOnce(1);
  ASSERT_TRUE(channel2_->readable());
  channel2_->ReadMore(rbuf.get(), pkg_size + 1);
  ASSERT_TRUE(channel2_->broken());

  channel2_->FlushBuffer();

  ASSERT_EQ(MsgCallBackTest::buffers_.size(), 3);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
