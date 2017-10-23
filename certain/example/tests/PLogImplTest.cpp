#include "PLogImpl.h"

#include <string>
#include <gtest/gtest.h>

static void OpenLevelDB(dbtype::DB** poLevelDB)
{
    std::string strName = "./plog.o";

    dbtype::Options tOpt;
    tOpt.create_if_missing = true;
    tOpt.comparator = new clsPLogComparator;
    assert(dbtype::DB::Open(tOpt, strName, poLevelDB).ok());
}

TEST(CodingTest, GetValueTest)
{
    dbtype::DB *poLevelDB = NULL;
    OpenLevelDB(&poLevelDB);
    std::unique_ptr<dbtype::DB> oAuto(poLevelDB);
    clsPLogImpl oImpl(poLevelDB);

    std::string strValue = "";
    uint64_t iEntityID = 10086;
    uint64_t iEntry = 10;
    uint64_t iValueID = 11;

    EXPECT_EQ(oImpl.GetValue(
                iEntityID, iEntry, iValueID, strValue), Certain::eRetCodeNotFound);
    EXPECT_EQ(oImpl.PutValue(iEntityID, iEntry, iValueID, "hello"), 0);
    EXPECT_EQ(oImpl.GetValue(iEntityID, iEntry, iValueID, strValue), 0);
    EXPECT_STREQ(strValue.c_str(), "hello");
}

TEST(CodingTest, GetRecordTest)
{
    dbtype::DB *poLevelDB = NULL;
    OpenLevelDB(&poLevelDB);
    std::unique_ptr<dbtype::DB> oAuto(poLevelDB);
    clsPLogImpl oImpl(poLevelDB);

    std::string strRecord = "";
    uint64_t iEntityID = 10086;
    uint64_t iEntry = 10;

    EXPECT_EQ(oImpl.Get(iEntityID, iEntry, strRecord), Certain::eRetCodeNotFound);
    EXPECT_EQ(oImpl.Put(iEntityID, iEntry, "world"), 0);
    EXPECT_EQ(oImpl.Get(iEntityID, iEntry, strRecord), 0);
    usleep(10); // change the timestamp
    EXPECT_EQ(oImpl.Get(iEntityID, iEntry, strRecord), 0);
    EXPECT_STREQ(strRecord.c_str(), "world");
}

TEST(CodingTest, GetMetaTest)
{
    dbtype::DB *poLevelDB = NULL;
    OpenLevelDB(&poLevelDB);
    std::unique_ptr<dbtype::DB> oAuto(poLevelDB);
    clsPLogImpl oImpl(poLevelDB);

    std::string strRecord = "";
    Certain::PLogEntityMeta_t tMeta;
    uint64_t iEntityID = 10086;
    uint64_t iEntry = 20;

    EXPECT_EQ(oImpl.Get(iEntityID, iEntry, strRecord), Certain::eRetCodeNotFound);
    EXPECT_EQ(oImpl.GetPLogEntityMeta(iEntityID, tMeta), Certain::eRetCodeNotFound);

    tMeta.iMaxPLogEntry = 30;
    EXPECT_EQ(oImpl.PutWithPLogEntityMeta(
                iEntityID, iEntry, tMeta, "with_meta"), 0);

    EXPECT_EQ(oImpl.Get(iEntityID, iEntry, strRecord), 0);
    EXPECT_STREQ(strRecord.c_str(), "with_meta");

    tMeta.iMaxPLogEntry = 0;
    EXPECT_EQ(oImpl.GetPLogEntityMeta(iEntityID, tMeta), 0);
    EXPECT_EQ(tMeta.iMaxPLogEntry, 30);
}

TEST(CodingTest, LoadTest)
{
    dbtype::DB *poLevelDB = NULL;
    OpenLevelDB(&poLevelDB);
    std::unique_ptr<dbtype::DB> oAuto(poLevelDB);
    clsPLogImpl oImpl(poLevelDB);

    EXPECT_EQ(oImpl.Put(100, 2, "100-2"), 0);
    EXPECT_EQ(oImpl.Put(100, 3, "100-3"), 0);
    EXPECT_EQ(oImpl.Put(100, 5, "100-5"), 0);

    bool bHasMore;
    std::vector< std::pair<uint64_t, std::string> > vecRecord;

    EXPECT_EQ(oImpl.LoadUncommitedEntrys(100, 0, 1, vecRecord, bHasMore), 0);
    EXPECT_EQ(vecRecord.size(), 0);
    EXPECT_TRUE(bHasMore);

    bHasMore = false;
    EXPECT_EQ(oImpl.LoadUncommitedEntrys(100, 0, 2, vecRecord, bHasMore), 0);
    EXPECT_EQ(vecRecord.size(), 1);
    EXPECT_EQ(vecRecord[0].first, 2);
    EXPECT_EQ(vecRecord[0].second, "100-2");
    EXPECT_TRUE(bHasMore);

    bHasMore = false;
    EXPECT_EQ(oImpl.LoadUncommitedEntrys(100, 0, 3, vecRecord, bHasMore), 0);
    EXPECT_EQ(vecRecord.size(), 2);
    EXPECT_EQ(vecRecord[1].first, 3);
    EXPECT_EQ(vecRecord[1].second, "100-3");
    EXPECT_TRUE(bHasMore);

    bHasMore = false;
    EXPECT_EQ(oImpl.LoadUncommitedEntrys(100, 0, 4, vecRecord, bHasMore), 0);
    EXPECT_EQ(vecRecord.size(), 2);
    EXPECT_EQ(vecRecord[1].first, 3);
    EXPECT_EQ(vecRecord[1].second, "100-3");
    EXPECT_TRUE(bHasMore);

    EXPECT_EQ(oImpl.LoadUncommitedEntrys(100, 0, 5, vecRecord, bHasMore), 0);
    EXPECT_EQ(vecRecord.size(), 3);
    EXPECT_EQ(vecRecord[2].first, 5);
    EXPECT_EQ(vecRecord[2].second, "100-5");
    EXPECT_FALSE(bHasMore);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
