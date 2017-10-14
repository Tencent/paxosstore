#include "gtest/gtest.h"
#include "utils/CRC32.h"

using namespace Certain;

TEST(CRC32Test, Basic)
{
    EXPECT_EQ(CRC32(""), 0);
    EXPECT_EQ(CRC32("h"), 0x916b06e7);
    EXPECT_EQ(CRC32("hello worl"), 0x2811cbc8);
    EXPECT_EQ(CRC32("hello world"), 0x0d4a1185);

    string str = "hello world";
    uint32_t c0 = CRC32(0, str.c_str(), str.size());
    uint32_t c1 = CRC32(str.c_str(), str.size());
    uint32_t c2 = CRC32(str);

    EXPECT_EQ(c0, c1);
    EXPECT_EQ(c1, c2);

    uint32_t iCRC = 0;
    for (size_t i = 0; i < str.size(); ++i)
    {
        iCRC = CRC32(iCRC, str.c_str() + i, 1);
    }
    EXPECT_EQ(iCRC, 0x0d4a1185);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
