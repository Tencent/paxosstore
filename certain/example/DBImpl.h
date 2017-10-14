#pragma once
#include "Command.h"
#include "Certain.h"
// db
#include "db.h"
#include "DBType.h"

using namespace Certain;

class clsSnapshotWrapper 
{
public:
    clsSnapshotWrapper(dbtype::DB *poLevelDB, const dbtype::Snapshot* poSnapshot) 
        : m_poLevelDB(poLevelDB), m_poSnapshot(poSnapshot) 
    {
        assert(m_poLevelDB != NULL);
        assert(m_poSnapshot != NULL);
    }

    ~clsSnapshotWrapper()
    {
        assert(m_poLevelDB != NULL);
        assert(m_poSnapshot != NULL);
        m_poLevelDB->ReleaseSnapshot(m_poSnapshot);
    }

    const dbtype::Snapshot* GetSnapshot()
    {
        return m_poSnapshot;
    }

    dbtype::DB *m_poLevelDB;
    const dbtype::Snapshot *m_poSnapshot = NULL;
};

class clsDBImpl : public Certain::clsDBBase
{
private:
    static const uint32_t kSnapshotTimeOut;
    static const uint32_t kSnapshotCount;

    dbtype::DB *m_poLevelDB;

    pthread_mutex_t *m_poSnapshotMapMutex;
    // timestmp -> (sequence, snapshot)
	std::map<uint32_t, std::pair<uint64_t, std::shared_ptr<clsSnapshotWrapper>>> *m_poSnapshotMap;

public:
    clsDBImpl(dbtype::DB *poLevelDB, 
            pthread_mutex_t* poSnapshotMapMutex = NULL,
            std::map<uint32_t, std::pair<uint64_t, std::shared_ptr<clsSnapshotWrapper>>> *poSnapshotMap = NULL) 
        : m_poLevelDB(poLevelDB),
          m_poSnapshotMapMutex(poSnapshotMapMutex),
          m_poSnapshotMap(poSnapshotMap) { }

    virtual ~clsDBImpl() { }

    virtual int Commit(uint64_t iEntityID, uint64_t iEntry, const string &strWriteBatch);

    virtual int MultiCommit(vector<Certain::EntryValue_t> vecEntryValue)
    {
        return -1;
    }

    virtual int GetEntityMeta(uint64_t iEntityID, uint64_t &iMaxCommitedEntry, uint32_t &iFlag);

    virtual int GetAllAndSet(uint64_t iEntityID, uint32_t iAcceptorID, uint64_t &iMaxCommitedEntry);

    int GetEntityMeta(uint64_t iEntityID, uint64_t &iMaxCommitedEntry,
                      uint32_t &iFlag, const dbtype::Snapshot* poSnapshot);

    // iFlag or iMaxCommitedEntry ignored if equals -1.
    int SetEntityMeta(uint64_t iEntityID, uint64_t iMaxCommitedEntry,
                      uint32_t iFlag = uint32_t(-1));

    int Put(const string &strKey, const string &strValue, string* strWriteBatch = NULL);

    int MultiPut(dbtype::WriteBatch* oWB);

    int Get(const string &strKey, string &strValue, const dbtype::Snapshot* poSnapshot = NULL);

    int Delete(const string &strKey, string* strWriteBatch = NULL);

    int GetBatch(uint64_t iEntityID, string& strNextKey, string* strWriteBatch,
            const dbtype::Snapshot* poSnapshot,
            uint32_t iMaxSize = 1024 * 1024);

    int Clear(uint64_t iEntityID, string& strNextKey, uint32_t iMaxIterateKeyCnt = 1000);

    int InsertSnapshot(uint64_t& iSequenceNumber, const dbtype::Snapshot* poSnapshot);

    int FindSnapshot(uint64_t iSequenceNumber, const dbtype::Snapshot* poSnapshot);

    void EraseSnapshot();

    // Used by clsDBImplTest.
    uint64_t GetSnapshotSize();
};

