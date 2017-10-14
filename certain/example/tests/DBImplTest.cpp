#include "DBImpl.h"

#include <string>
#include <gtest/gtest.h>

#include "Certain.pb.h"
#include "example/Coding.h"
// db
#include "write_batch.h"

using namespace Certain;

namespace Certain
{

class clsDBImplTest : public ::testing::Test
{
protected:
    void SetUp() override 
    {
        m_poLevelDB = NULL;

        string strName = "./db.o";

        dbtype::Options tOpt;
        tOpt.create_if_missing = true;
        assert(dbtype::DB::Open(tOpt, strName, &m_poLevelDB).ok());
    }

    int GetEntityMeta(uint64_t iEntityID, CertainPB::DBEntityMeta& tDBEntityMeta, 
            const dbtype::Snapshot* poSnapshot = NULL)
    {
        clsDBImpl oImpl(m_poLevelDB);

        uint64_t iMaxCommitedEntry = 0;
        uint32_t iFlag = 0;
        int iRet = oImpl.GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag, poSnapshot);
        if (iRet != 0) return iRet;
        tDBEntityMeta.set_max_commited_entry(iMaxCommitedEntry);
        tDBEntityMeta.set_flag(iFlag);
        return 0;
    }

    int SetEntityMeta(uint64_t iEntityID, uint64_t iMaxCommitedEntry, uint32_t iFlag)
    {
        clsDBImpl oImpl(m_poLevelDB);
        return oImpl.SetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
    }

    dbtype::DB *m_poLevelDB = NULL;
};

}  // namespace Certain

// Tests Put, MultiPut, Get, Delete
TEST_F(clsDBImplTest, BasicTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsDBImpl oImpl(m_poLevelDB);

    string strKey = "key";
    string strValue = "value";
    string strGetValue;
    string strWriteBatch;

    EXPECT_EQ(eRetCodeOK, oImpl.Put(strKey, strValue, &strWriteBatch));
    EXPECT_EQ(eRetCodeNotFound, oImpl.Get(strKey, strGetValue));
    EXPECT_EQ(eRetCodeOK, oImpl.Put(strKey, strValue));
    EXPECT_EQ(eRetCodeOK, oImpl.Get(strKey, strGetValue));
    EXPECT_EQ(strValue, strGetValue);

    dbtype::WriteBatch oWB;
    for (int i = 1; i <= 3; ++i)
        oWB.Put(std::to_string(i), std::to_string(i));
    EXPECT_EQ(eRetCodeOK, oImpl.MultiPut(&oWB));
    for (int i = 1; i <= 3; ++i)
    {
        EXPECT_EQ(eRetCodeOK, oImpl.Get(std::to_string(i), strGetValue));
        EXPECT_EQ(std::to_string(i), strGetValue);
    }

    strWriteBatch.clear();
    EXPECT_EQ(eRetCodeOK, oImpl.Delete(strKey, &strWriteBatch));
    EXPECT_EQ(eRetCodeOK, oImpl.Get(strKey, strGetValue));
    EXPECT_EQ(eRetCodeOK, oImpl.Delete(strKey));
    EXPECT_EQ(eRetCodeNotFound, oImpl.Get(strKey, strGetValue));

    oWB.Clear();
    for (int i = 1; i <= 3; ++i)
        oWB.Delete(std::to_string(i));
    EXPECT_EQ(eRetCodeOK, oImpl.MultiPut(&oWB));
    for (int i = 1; i <= 3; ++i)
        EXPECT_EQ(eRetCodeNotFound, oImpl.Get(std::to_string(i), strGetValue));

    string strError = "error";
    dbtype::WriteBatch oErrorWB(strError); 
    EXPECT_EQ(eRetCodeDBPutErr, oImpl.MultiPut(&oErrorWB));
}

TEST_F(clsDBImplTest, CommitTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsDBImpl oImpl(m_poLevelDB);

    uint64_t iEntityID = 1234567890;
    uint64_t iEntry = 10086;
    string strWriteBatch;

    // Only commits (iEntityID->iEntry).
    EXPECT_EQ(eRetCodeOK, oImpl.Commit(iEntityID, iEntry, strWriteBatch));
    
    string strMetaKey, strMetaValue;
    EncodeEntityMetaKey(strMetaKey, iEntityID);
    CertainPB::DBEntityMeta tDBEntityMeta;
    EXPECT_EQ(eRetCodeOK, oImpl.Get(strMetaKey, strMetaValue));
    assert(tDBEntityMeta.ParseFromString(strMetaValue));
    EXPECT_EQ(iEntry, tDBEntityMeta.max_commited_entry());

    // Commits both (iEntityID->iEntry) and kv.
    strMetaKey.clear();
    EncodeEntityMetaKey(strMetaKey, iEntityID + 1);
    EXPECT_EQ(eRetCodeNotFound, oImpl.Get(strMetaKey, strMetaValue));

    string strKey = "key";
    string strValue = "value";
    string strGetValue;

    dbtype::WriteBatch oWB;
    oWB.Put(strKey, strValue);
    strWriteBatch = oWB.Data();
    EXPECT_EQ(eRetCodeOK, oImpl.Commit(iEntityID + 1, iEntry, strWriteBatch));
    EXPECT_EQ(eRetCodeOK, oImpl.Get(strMetaKey, strMetaValue));
    EXPECT_EQ(eRetCodeOK, oImpl.Get(strKey, strGetValue));
    EXPECT_EQ(strValue, strGetValue);
    EXPECT_EQ(eRetCodeOK, oImpl.Delete(strKey));
}

TEST_F(clsDBImplTest, GetEntityMetaTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsDBImpl oImpl(m_poLevelDB);

    dbtype::SnapshotImpl oSnapshot;
    oSnapshot.number_ = 0;

    uint64_t iEntityID = 1234567890;
    uint64_t iEntry = 0;
    uint32_t iFlag = 0;

    EXPECT_EQ(eRetCodeNotFound, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag, &oSnapshot));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag, m_poLevelDB->GetSnapshot()));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag, NULL));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(10086, iEntry);
}

TEST_F(clsDBImplTest, SetEntityMetaTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);

    dbtype::SnapshotImpl oSnapshot;
    oSnapshot.number_ = 0;

    uint64_t iEntityID = 1234567890;
    uint32_t iFlag = 1;
    uint64_t iCommitedEntry = 10087;

    EXPECT_EQ(eRetCodeOK, SetEntityMeta(iEntityID, -1, iFlag));
    CertainPB::DBEntityMeta tDBEntityMeta;
    EXPECT_EQ(eRetCodeOK, GetEntityMeta(iEntityID, tDBEntityMeta));
    EXPECT_EQ(iFlag, tDBEntityMeta.flag());

    EXPECT_EQ(eRetCodeOK, SetEntityMeta(iEntityID, -1, -1));
    tDBEntityMeta.Clear();
    EXPECT_EQ(eRetCodeOK, GetEntityMeta(iEntityID, tDBEntityMeta));
    EXPECT_EQ(1, tDBEntityMeta.flag());

    EXPECT_EQ(eRetCodeOK, SetEntityMeta(iEntityID, iCommitedEntry, iFlag));
    EXPECT_EQ(eRetCodeOK, GetEntityMeta(iEntityID, tDBEntityMeta));
    EXPECT_EQ(iCommitedEntry, tDBEntityMeta.max_commited_entry());
    EXPECT_EQ(1, tDBEntityMeta.flag());

    EXPECT_EQ(eRetCodeOK, SetEntityMeta(iEntityID + 2, iCommitedEntry, iFlag));

    EXPECT_EQ(eRetCodeNotFound, GetEntityMeta(iEntityID, tDBEntityMeta, &oSnapshot));
    EXPECT_EQ(eRetCodeOK, GetEntityMeta(iEntityID, tDBEntityMeta, m_poLevelDB->GetSnapshot()));
    EXPECT_EQ(eRetCodeOK, GetEntityMeta(iEntityID, tDBEntityMeta, NULL));
    EXPECT_EQ(eRetCodeOK, GetEntityMeta(iEntityID, tDBEntityMeta));
    EXPECT_EQ(10087, tDBEntityMeta.max_commited_entry());
}

TEST_F(clsDBImplTest, EntityMetaTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsDBImpl oImpl(m_poLevelDB);

    uint64_t iEntityID = 9123456789LL;
    uint64_t iEntry = 123456789000LL;
    uint32_t iFlag = 1234567890;

    EXPECT_EQ(eRetCodeNotFound, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(eRetCodeOK, oImpl.SetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(123456789000LL, iEntry);
    EXPECT_EQ(1234567890, iFlag);

    EXPECT_EQ(eRetCodeOK, oImpl.SetEntityMeta(iEntityID, iEntry + 1, -1));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(123456789001LL, iEntry);
    EXPECT_EQ(1234567890, iFlag);

    EXPECT_EQ(eRetCodeOK, oImpl.SetEntityMeta(iEntityID, -1, iFlag + 1));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(123456789001LL, iEntry);
    EXPECT_EQ(1234567891, iFlag);

    EXPECT_EQ(eRetCodeOK, oImpl.SetEntityMeta(iEntityID, iEntry + 1, iFlag + 1));
    EXPECT_EQ(eRetCodeOK, oImpl.GetEntityMeta(iEntityID, iEntry, iFlag));
    EXPECT_EQ(123456789002LL, iEntry);
    EXPECT_EQ(1234567892, iFlag);
}

TEST_F(clsDBImplTest, GetBatchTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsDBImpl oImpl(m_poLevelDB);

    uint64_t iEntityID = 9123456789LL;
    string strNextKey;
    string strWriteBatch;

    vector<string> strKeys;
    for (int i = 10; i < 13; ++i)
    {
        string strKey;
        EncodeInfoKey(strKey, iEntityID, (uint64_t)i);
        EXPECT_EQ(eRetCodeOK, oImpl.Put(strKey, std::to_string(i)));
        strKeys.push_back(strKey);
    }

    // strWriteBatch is NULL.
    EXPECT_EQ(eRetCodeGetBatchErr, oImpl.GetBatch(iEntityID, strNextKey, NULL, NULL));

    // strNextKey is empty.
    EXPECT_EQ(eRetCodeOK, oImpl.GetBatch(iEntityID, strNextKey, &strWriteBatch, NULL));
    EXPECT_TRUE(strWriteBatch.empty());

    // iMaxSize is small, and only one kv could be got in batch.
    EncodeInfoKey(strNextKey, iEntityID, 0);
    EXPECT_EQ(eRetCodeOK, oImpl.GetBatch(iEntityID, strNextKey, &strWriteBatch, NULL, 10));
    EXPECT_EQ(strKeys[1], strNextKey);
    EXPECT_FALSE(strWriteBatch.empty());

    // iMaxSize is large, and the left kv were got in batch.
    strWriteBatch.clear();
    EXPECT_EQ(eRetCodeOK, oImpl.GetBatch(iEntityID, strNextKey, &strWriteBatch, NULL, 100));
    EXPECT_TRUE(strNextKey.empty());
    EXPECT_FALSE(strWriteBatch.empty());
}

TEST_F(clsDBImplTest, ClearTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsDBImpl oImpl(m_poLevelDB);

    uint64_t iEntityID = 9123456789LL;
    string strNextKey;

    vector<string> strKeys;
    for (int i = 10; i < 13; ++i)
    {
        string strKey, strValue;
        EncodeInfoKey(strKey, iEntityID, (uint64_t)i);
        strKeys.push_back(strKey);
        EXPECT_EQ(eRetCodeOK, oImpl.Get(strKey, strValue));
        EXPECT_EQ(std::to_string(i), strValue);
    }

    // strNextKey is empty.
    EXPECT_EQ(eRetCodeOK, oImpl.Clear(iEntityID, strNextKey, 0));
    EXPECT_TRUE(strNextKey.empty());

    // iMaxIterateKeyCnt is small, and only one kv could be clear.
    EncodeInfoKey(strNextKey, iEntityID, 0);
    EXPECT_EQ(eRetCodeOK, oImpl.Clear(iEntityID, strNextKey, 1));
    EXPECT_EQ(strKeys[1], strNextKey);
    EXPECT_FALSE(strNextKey.empty());

    // iMaxIterateKeyCnt is large, and the left kv were got in batch.
    EXPECT_EQ(eRetCodeOK, oImpl.Clear(iEntityID, strNextKey, 10));
    EXPECT_TRUE(strNextKey.empty());
}

TEST_F(clsDBImplTest, SnapshotTest)
{
    unique_ptr<dbtype::DB> oAuto(m_poLevelDB);

	pthread_mutex_t* poSnapshotMapMutex = new pthread_mutex_t;
	assert(0 == pthread_mutex_init(poSnapshotMapMutex, NULL));

	std::map<uint32_t, std::pair<uint64_t, std::shared_ptr<clsSnapshotWrapper>>> *poSnapshotMap =
        new std::map<uint32_t, std::pair<uint64_t, std::shared_ptr<clsSnapshotWrapper>>>;
    assert(poSnapshotMap != NULL);

    clsDBImpl* poImpl = new clsDBImpl(m_poLevelDB, poSnapshotMapMutex, poSnapshotMap);

    uint64_t iSequenceNumber1, iSequenceNumber2 = 0;
    const dbtype::Snapshot* poSnapshot = NULL;
    EXPECT_EQ(Certain::eRetCodeOK, poImpl->InsertSnapshot(iSequenceNumber1, poSnapshot));
    EXPECT_EQ(Certain::eRetCodeOK, poImpl->InsertSnapshot(iSequenceNumber2, poSnapshot));
    // Timestamp is the same in the same second.
    EXPECT_EQ(iSequenceNumber1, iSequenceNumber2);
    EXPECT_EQ(1, poImpl->GetSnapshotSize());

    // Two different timestamp are used as keys in map.
    sleep(1);
    EXPECT_EQ(Certain::eRetCodeOK, poImpl->InsertSnapshot(iSequenceNumber2, poSnapshot));
    EXPECT_EQ(iSequenceNumber1, iSequenceNumber2);
    EXPECT_EQ(2, poImpl->GetSnapshotSize());

    // Snapshot with iSequenceNumber1 is in map, but that with (iSequenceNumber1 + 1) is not in map.
    EXPECT_EQ(eRetCodeOK, poImpl->FindSnapshot(iSequenceNumber1, poSnapshot));
    EXPECT_EQ(eRetCodeSnapshotNotFoundErr, poImpl->FindSnapshot(iSequenceNumber1 + 1, poSnapshot));

    EXPECT_EQ(2, poImpl->GetSnapshotSize());

    poImpl->EraseSnapshot();
    // No snapshot were removed.
    EXPECT_EQ(2, poImpl->GetSnapshotSize());

    for (int i = 0; i < 10; ++i) 
    {
        sleep(1);
        EXPECT_EQ(Certain::eRetCodeOK, poImpl->InsertSnapshot(iSequenceNumber2, poSnapshot));
    }
    EXPECT_EQ(12, poImpl->GetSnapshotSize());

    poImpl->EraseSnapshot();
    // No snapshot were removed.
    EXPECT_EQ(5, poImpl->GetSnapshotSize());

    delete poSnapshotMapMutex, poSnapshotMapMutex = NULL;
    delete poSnapshotMap, poSnapshotMap = NULL;
    delete poImpl, poImpl = NULL;
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
