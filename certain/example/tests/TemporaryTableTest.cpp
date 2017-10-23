#include "example/TemporaryTable.h"

#include <string>
#include <gtest/gtest.h>

#include "src/Command.h"

class clsTemporaryTableTest : public ::testing::Test
{
protected:
    void SetUp() override 
    {
        m_poLevelDB = NULL;

        std::string strName = "./db.o";

        dbtype::Options tOpt;
        tOpt.create_if_missing = true;
        assert(dbtype::DB::Open(tOpt, strName, &m_poLevelDB).ok());
    }

    dbtype::DB *m_poLevelDB = NULL;
};

TEST_F(clsTemporaryTableTest, BasicTest)
{
    std::unique_ptr<dbtype::DB> oAuto(m_poLevelDB);
    clsTemporaryTable oTable(m_poLevelDB);

    std::string strKey = "key";
    std::string strValue = "value";

    EXPECT_EQ(Certain::eRetCodeNotFound, oTable.Get(strKey, strValue));

    EXPECT_EQ(Certain::eRetCodeOK, oTable.Put(strKey, strValue));
    // The kv is found in map.
    EXPECT_EQ(Certain::eRetCodeOK, oTable.Get(strKey, strValue));
    EXPECT_EQ("value", strValue);

    // The kv has not been committed into db.
    static dbtype::ReadOptions tReadOpt;
    dbtype::Status s = m_poLevelDB->Get(tReadOpt, strKey, &strValue);
    EXPECT_TRUE(s.IsNotFound());

    static dbtype::WriteOptions tWriteOpt;
    {
        dbtype::WriteBatch oWriteBatch(oTable.GetWriteBatchString());
        s = m_poLevelDB->Write(tWriteOpt, &oWriteBatch);
        EXPECT_TRUE(s.ok());
    }

    // The kv has been committed to db.
    s = m_poLevelDB->Get(tReadOpt, strKey, &strValue);
    EXPECT_TRUE(s.ok());

    // The kv has been deleted in map.
    EXPECT_EQ(Certain::eRetCodeOK, oTable.Delete(strKey));
    EXPECT_EQ(Certain::eRetCodeNotFound, oTable.Get(strKey, strValue));

    // The kv is still in db.
    s = m_poLevelDB->Get(tReadOpt, strKey, &strValue);
    EXPECT_TRUE(s.ok());

    {
        dbtype::WriteBatch oWriteBatch(oTable.GetWriteBatchString());
        s = m_poLevelDB->Write(tWriteOpt, &oWriteBatch);
        EXPECT_TRUE(s.ok());
    }
    // The kv has been deleted in db.
    s = m_poLevelDB->Get(tReadOpt, strKey, &strValue);
    EXPECT_TRUE(s.IsNotFound());
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
