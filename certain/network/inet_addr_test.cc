#include "network/inet_addr.h"

#include "gtest/gtest.h"
#include "utils/log.h"

TEST(InetAddrTest, Basic) {
  certain::InetAddr addr1("127.0.0.1", 13069);
  certain::InetAddr addr2("127.0.0.1", 13070);
  certain::InetAddr addr3("127.0.0.2", 13069);

  ASSERT_TRUE(addr1 == addr1);
  ASSERT_STREQ(addr1.ToString().c_str(), "127.0.0.1:13069");

  ASSERT_TRUE(addr1 < addr2);
  ASSERT_TRUE(addr2 < addr3);
  ASSERT_TRUE(addr1 < addr3);
}

TEST(InetAddrTest, StrConstruct) {
  certain::InetAddr addr1("127.0.0.1:13069");
  ASSERT_STREQ(addr1.ToString().c_str(), "127.0.0.1:13069");
  CERTAIN_LOG_INFO("addrid: %lu", addr1.GetAddrId());
  certain::InetAddr addr2(addr1.GetAddrId());
  ASSERT_STREQ(addr2.ToString().c_str(), "127.0.0.1:13069");
  CERTAIN_LOG_INFO("addr2: %s", addr2.ToString().c_str());
  ASSERT_EQ(addr1.GetAddrId(), addr2.GetAddrId());
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
