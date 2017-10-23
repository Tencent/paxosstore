#include "example/Coding.h"

#include <string>
#include <gtest/gtest.h>

TEST(CodingTest, IntTest) {
    std::string strKey;
    AppendFixed8(strKey, 10);
    AppendFixed16(strKey, 1024);
    AppendFixed32(strKey, 20170310);
    AppendFixed64(strKey, 20180101);
    dbtype::Slice oSliceKey = dbtype::Slice(strKey);
    uint8_t ret8;
    uint16_t ret16;
    uint32_t ret32;
    uint64_t ret64;
    RemoveFixed8(oSliceKey, ret8);
    EXPECT_EQ(10, ret8);
    RemoveFixed16(oSliceKey, ret16);
    EXPECT_EQ(1024, ret16);
    RemoveFixed32(oSliceKey, ret32);
    EXPECT_EQ(20170310, ret32);
    RemoveFixed64(oSliceKey, ret64);
    EXPECT_EQ(20180101, ret64);
    EXPECT_TRUE(oSliceKey.empty());
}

TEST(CodingTest, KeyTest) 
{
    std::string strKey;
    EncodeEntityMetaKey(strKey, kEntityMetaKeyLen);
    EXPECT_EQ(kEntityMetaKeyLen, strKey.size());
    uint64_t iEntityID = 0, iInfoID = 0;
    EXPECT_TRUE(DecodeEntityMetaKey(dbtype::Slice(strKey), iEntityID));
    EXPECT_EQ(kEntityMetaKeyLen, iEntityID);

    strKey.clear();
    EncodeInfoKey(strKey, 10086, 10);
    EXPECT_EQ(kInfoKeyLen, strKey.size());

    EXPECT_TRUE(DecodeInfoKey(dbtype::Slice(strKey), iEntityID, iInfoID));
    EXPECT_EQ(10086, iEntityID);
    EXPECT_EQ(10, iInfoID);
    // Length of string is not equal to kInfoKeyLen.
    EXPECT_FALSE(DecodeInfoKey("aaa", iEntityID, iInfoID));
    strKey.clear();
    AppendFixed8(strKey, 0);
    AppendFixed64(strKey, 0);
    AppendFixed8(strKey, 1);
    AppendFixed64(strKey, 0);
    AppendFixed8(strKey, 2);
    // EXPECTE: (1, x, 0, y, 2), ACTUAL: (0, x, 1, y, 2)
    EXPECT_FALSE(DecodeInfoKey(dbtype::Slice(strKey), iEntityID, iInfoID));
    strKey.clear();
    AppendFixed8(strKey, 1);
    AppendFixed64(strKey, 0);
    AppendFixed8(strKey, 0);
    AppendFixed64(strKey, 0);
    AppendFixed8(strKey, 3);
    // EXPECTE: (1, x, 0, y, 2), ACTUAL: (1, x, 0, y, 3)
    EXPECT_FALSE(DecodeInfoKey(dbtype::Slice(strKey), iEntityID, iInfoID));

    strKey.clear();
    EncodeInfoKey(strKey, 10086, 10);
    const dbtype::Slice& oSliceKey = dbtype::Slice(strKey);
    EXPECT_FALSE(KeyHitEntityID(10087, oSliceKey));
    EXPECT_TRUE(KeyHitEntityID(10086, oSliceKey));
}

TEST(CodingTest, PLogKeyTest)
{
    std::string strKey;
    uint64_t iEntityID;
    uint64_t iEntry;

    EncodePLogKey(strKey, 10086, 10);
    EXPECT_EQ(kPLogKeyLen, strKey.size());

    EXPECT_TRUE(DecodePLogKey(dbtype::Slice(strKey), iEntityID, iEntry));
    EXPECT_EQ(10086, iEntityID);
    EXPECT_EQ(10, iEntry);

    uint64_t iTimestampMS = -1;
    uint64_t iRealMS = Certain::GetCurrTimeMS();
    EXPECT_TRUE(DecodePLogKey(dbtype::Slice(strKey), iEntityID, iEntry, &iTimestampMS));
    EXPECT_EQ(iTimestampMS, iRealMS);

    strKey += "x";
    EXPECT_FALSE(DecodePLogKey(dbtype::Slice(strKey), iEntityID, iEntry));
}

TEST(CodingTest, PLogValueKeyTest)
{
    std::string strKey;
    uint64_t iEntityID;
    uint64_t iEntry;
    uint64_t iValueID;

    EncodePLogValueKey(strKey, 10086, 10, 11);
    EXPECT_EQ(kPLogValueKeyLen, strKey.size());

    EXPECT_TRUE(DecodePLogValueKey(dbtype::Slice(strKey), iEntityID, iEntry, iValueID));
    EXPECT_EQ(10086, iEntityID);
    EXPECT_EQ(10, iEntry);
    EXPECT_EQ(11, iValueID);

    uint64_t iTimestampMS = -1;
    uint64_t iRealMS = Certain::GetCurrTimeMS();
    EXPECT_TRUE(DecodePLogValueKey(dbtype::Slice(strKey), iEntityID, iEntry, iValueID, &iTimestampMS));
    EXPECT_EQ(iTimestampMS, iRealMS);

    strKey += "x";
    EXPECT_FALSE(DecodePLogValueKey(dbtype::Slice(strKey), iEntityID, iEntry, iValueID));
}

TEST(CodingTest, PLogMetaKeyTest)
{
    std::string strKey;
    uint64_t iEntityID;

    EncodePLogMetaKey(strKey, 10086);
    EXPECT_EQ(kPLogMetaKeyLen, strKey.size());

    EXPECT_TRUE(DecodePLogMetaKey(dbtype::Slice(strKey), iEntityID));
    EXPECT_EQ(10086, iEntityID);

    strKey += "x";
    EXPECT_FALSE(DecodePLogMetaKey(dbtype::Slice(strKey), iEntityID));
}

TEST(CodingTest, GetEntityIDTest)
{
    EXPECT_EQ(GetEntityID(123), GetEntityID(123));
    EXPECT_NE(GetEntityID(123), GetEntityID(321));
    EXPECT_EQ(GetEntityID(123, 100), GetEntityID(123, 100));
    EXPECT_EQ(GetEntityID(123, 100), GetEntityID(223, 100));
    EXPECT_NE(GetEntityID(123, 100), GetEntityID(321, 100));
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
