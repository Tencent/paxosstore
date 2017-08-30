#ifndef CERTAIN_EXAMPLE_SIMPLE_KVEngine_H_
#define CERTAIN_EXAMPLE_SIMPLE_KVEngine_H_

#include "Certain.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

class clsKVEngine
{
private:
    leveldb::DB *m_poLevelDB;
    bool m_bNoThing;

    bool m_bUseHash;
    const static uint32_t kGroupNum = 100;
    Certain::clsRWLock m_oRWLock[kGroupNum];
    unordered_map<string, string> m_oMap[kGroupNum];

public:
    clsKVEngine(leveldb::DB *poLevelDB)
    {
        m_poLevelDB = poLevelDB;
        m_bNoThing = false;
        m_bUseHash = false;
    }

    void SetNoThing()
    {
        m_bNoThing = true;
    }

    void SetUseMap()
    {
        m_bUseHash = true;
    }

	int Put(const string &strKey, const string &strValue);

	int MultiPut(const vector< pair<string, string> > &vecKeyValue);

	int Get(const string &strKey, string &strValue);

	int RangeLoad(const string &strStart, const string &strEnd,
			vector< pair<string, string> > &vecKeyValue);
};

#endif
