#include "default/route_impl.h"

#include "gtest/gtest.h"
#include "network/inet_addr.h"
#include "utils/log.h"

TEST(RouteImpl, Normal) {
  std::string local_addr = "127.0.0.1:13068";

  std::vector<std::string> addrs;
  addrs.push_back("127.0.0.1:13066");
  addrs.push_back("127.0.0.1:13067");
  addrs.push_back("127.0.0.1:13068");

  RouteImpl route(local_addr, addrs);

  ASSERT_STREQ(route.GetLocalAddr().c_str(), local_addr.c_str());
  uint32_t acceptor_id = 0;
  ASSERT_EQ(route.GetLocalAcceptorId(2, &acceptor_id), 0);
  ASSERT_EQ(acceptor_id, 1);
  ASSERT_EQ(route.GetLocalAcceptorId(3, &acceptor_id), 0);
  ASSERT_EQ(acceptor_id, 2);
  ASSERT_EQ(route.GetLocalAcceptorId(4, &acceptor_id), 0);
  ASSERT_EQ(acceptor_id, 0);

  uint64_t addr_id = -1;
  {
    ASSERT_EQ(route.GetServerAddrId(4, 0, &addr_id), 0);
    certain::InetAddr addr(addr_id);
    ASSERT_STREQ(addr.ToString().c_str(), "127.0.0.1:13068");
  }
  {
    ASSERT_EQ(route.GetServerAddrId(4, 1, &addr_id), 0);
    certain::InetAddr addr(addr_id);
    ASSERT_STREQ(addr.ToString().c_str(), "127.0.0.1:13066");
  }
  {
    ASSERT_EQ(route.GetServerAddrId(4, 2, &addr_id), 0);
    certain::InetAddr addr(addr_id);
    ASSERT_STREQ(addr.ToString().c_str(), "127.0.0.1:13067");
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
