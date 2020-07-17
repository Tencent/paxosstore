#include "utils/crc32.h"

#include "gtest/gtest.h"

TEST(crc32Test, Basic) {
  EXPECT_EQ(certain::crc32(""), 0);
  EXPECT_EQ(certain::crc32("h"), 0x916b06e7);
  EXPECT_EQ(certain::crc32("hello worl"), 0x2811cbc8);
  EXPECT_EQ(certain::crc32("hello world"), 0x0d4a1185);

  std::string str = "hello world";
  uint32_t c0 = certain::crc32(0, str.c_str(), str.size());
  uint32_t c1 = certain::crc32(str.c_str(), str.size());
  uint32_t c2 = certain::crc32(str);

  EXPECT_EQ(c0, c1);
  EXPECT_EQ(c1, c2);

  uint32_t crc = 0;
  for (size_t i = 0; i < str.size(); ++i) {
    crc = certain::crc32(crc, str.c_str() + i, 1);
  }
  EXPECT_EQ(crc, 0x0d4a1185);
}

TEST(crc32Test, Performance_1GB) {
  for (int i = 0; i < 1024; ++i) {
    uint32_t size = (1 << 20);
    char* p = new char[size];
    certain::crc32(p, size);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
