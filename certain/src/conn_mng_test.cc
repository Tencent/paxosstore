#include "src/conn_mng.h"

#include "gtest/gtest.h"

class ConnMngTest : public testing::Test {
 public:
  ConnMngTest()
      : addr0_("127.0.0.1", 18080),
        addr1_("127.0.0.1", 18081),
        addr2_("127.0.0.1", 18082),
        addr3_("127.0.0.1", 18083) {}

  virtual void SetUp() override { conn_mng_ = certain::ConnMng::GetInstance(); }

  virtual void TearDown() override { conn_mng_->Destroy(); }

 protected:
  certain::ConnMng* conn_mng_;

  certain::InetAddr addr0_;
  certain::InetAddr addr1_;
  certain::InetAddr addr2_;
  certain::InetAddr addr3_;
};

TEST_F(ConnMngTest, Normal) {
  conn_mng_->Init(addr0_.ToString(), 5);
  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());

  for (int i = 0; i < 5; ++i) {
    conn_mng_->PutByOneThread(
        std::make_unique<certain::TcpSocket>(-1, addr0_, addr1_));
    conn_mng_->PutByOneThread(
        std::make_unique<certain::TcpSocket>(-1, addr0_, addr2_));
    conn_mng_->PutByOneThread(
        std::make_unique<certain::TcpSocket>(-1, addr0_, addr3_));
  }
  ASSERT_FALSE(conn_mng_->IsBalanceTableEmpty());

  for (int i = 0; i < 5; ++i) {
    for (int j = 0; j < 3; ++j) {
      auto tcp_socket = conn_mng_->TakeByMultiThread(i);
      ASSERT_NE(tcp_socket, nullptr);
    }
  }

  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());
}

TEST_F(ConnMngTest, ReturnNullptr) {
  conn_mng_->Init(addr0_.ToString(), 5);
  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());

  for (int i = 0; i < 4; ++i) {
    conn_mng_->PutByOneThread(
        std::make_unique<certain::TcpSocket>(-1, addr0_, addr1_));
  }
  ASSERT_FALSE(conn_mng_->IsBalanceTableEmpty());

  for (int i = 0; i < 4; ++i) {
    ASSERT_NE(conn_mng_->TakeByMultiThread(i), nullptr);
  }
  ASSERT_EQ(conn_mng_->TakeByMultiThread(4), nullptr);

  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());
}

TEST_F(ConnMngTest, PutOneAndTakeOne) {
  conn_mng_->Init(addr0_.ToString(), 3);
  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());

  conn_mng_->PutByOneThread(
      std::make_unique<certain::TcpSocket>(-1, addr0_, addr1_));
  ASSERT_NE(conn_mng_->TakeByMultiThread(0), nullptr);
  conn_mng_->PutByOneThread(
      std::make_unique<certain::TcpSocket>(-1, addr0_, addr1_));
  ASSERT_NE(conn_mng_->TakeByMultiThread(1), nullptr);
  conn_mng_->PutByOneThread(
      std::make_unique<certain::TcpSocket>(-1, addr0_, addr1_));
  ASSERT_NE(conn_mng_->TakeByMultiThread(2), nullptr);

  ASSERT_EQ(conn_mng_->TakeByMultiThread(0), nullptr);
  ASSERT_EQ(conn_mng_->TakeByMultiThread(1), nullptr);
  ASSERT_EQ(conn_mng_->TakeByMultiThread(2), nullptr);
  ASSERT_TRUE(conn_mng_->IsBalanceTableEmpty());
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
