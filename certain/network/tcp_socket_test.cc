#include "network/tcp_socket.h"

#include "gtest/gtest.h"

TEST(TcpSocketTest, Basic) {
  certain::InetAddr addr1("127.0.0.1", 14068);

  certain::TcpSocket tcp1;
  ASSERT_EQ(tcp1.InitSocket(), 0);
  ASSERT_EQ(tcp1.Bind(addr1), 0);
  ASSERT_EQ(tcp1.Listen(), 0);
  std::cout << tcp1.ToString() << std::endl;

  bool blocked = false;
  ASSERT_EQ(tcp1.CheckIfNonBlock(blocked), 0);
  ASSERT_TRUE(blocked);

  ASSERT_EQ(tcp1.CheckIfValid(), 0);
  ASSERT_TRUE(tcp1.listened());
  ASSERT_NE(tcp1.fd(), 0);

  {
    certain::InetAddr addr;
    ASSERT_EQ(tcp1.Accept(addr), certain::kNetWorkWouldBlock);
  }

  {
    certain::TcpSocket tcp;
    ASSERT_EQ(tcp.InitSocket(), 0);
    ASSERT_EQ(tcp.Bind(addr1), certain::kNetWorkError);
  }

  certain::TcpSocket tcp2;
  certain::InetAddr addr2("127.0.0.1", 14069);
  ASSERT_EQ(tcp2.InitSocket(), 0);
  ASSERT_EQ(tcp2.Bind(addr2), 0);
  ASSERT_EQ(tcp2.Connect(addr1), certain::kNetWorkInProgress);
}

TEST(TcpSocketTest, BlockedSocket) {
  certain::InetAddr addr1("127.0.0.1", 14068);

  certain::TcpSocket tcp1;
  ASSERT_EQ(tcp1.InitSocket(), 0);
  ASSERT_EQ(tcp1.Bind(addr1), 0);
  ASSERT_EQ(tcp1.Listen(), 0);
  std::cout << tcp1.ToString() << std::endl;

  ASSERT_TRUE(tcp1.listened());

  certain::TcpSocket tcp2;
  certain::InetAddr addr2("127.0.0.1", 14069);
  ASSERT_EQ(tcp2.InitSocket(), 0);
  ASSERT_EQ(tcp2.SetNonBlock(false), 0);
  ASSERT_EQ(tcp2.Bind(addr2), 0);
  ASSERT_EQ(tcp2.Connect(addr1), 0);

  bool blocked = false;
  ASSERT_EQ(tcp2.CheckIfNonBlock(blocked), 0);
  ASSERT_FALSE(blocked);

  {
    certain::InetAddr addr;
    ASSERT_GT(tcp1.Accept(addr), 0);
    ASSERT_TRUE(addr == addr2);
    std::cout << addr.ToString() << std::endl;
  }
}

TEST(TcpSocketTest, InvalidFd) {
  certain::InetAddr addr("127.0.0.1", 8888);

  certain::TcpSocket tcp(666, addr, addr);
  ASSERT_EQ(tcp.SetNonBlock(true), certain::kNetWorkError);
  bool block;
  ASSERT_EQ(tcp.CheckIfNonBlock(block), certain::kNetWorkError);
  ASSERT_EQ(tcp.CheckIfValid(), certain::kNetWorkError);

  std::string buffer("Mock");
  ASSERT_FALSE(tcp.BlockWrite(buffer.data(), buffer.size()));
  ASSERT_FALSE(tcp.BlockRead(&buffer[0], buffer.size()));

  tcp.Shutdown();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
