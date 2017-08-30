#include "KVEngine.h"

int clsKVEngine::Put(const string &strKey, const string &strValue)
{
    if (m_bNoThing)
    {
        return 0;
    }
    leveldb::WriteOptions tOpt;
    //tOpt.sync = true;

    leveldb::Status s = m_poLevelDB->Put(tOpt, strKey, strValue);
    if (!s.ok())
    {
        return Certain::eRetCodePLogPutErr;
    }
    return 0;
}

int clsKVEngine::MultiPut(const vector< pair<string, string> > &vecKeyValue)
{
    if (m_bNoThing)
    {
        return 0;
    }
    if (m_bUseHash)
    {
        for (auto iter = vecKeyValue.begin(); iter != vecKeyValue.end(); ++iter)
        {
            uint32_t iIndex = Certain::Hash(iter->first) % kGroupNum;
            Certain::clsThreadWriteLock oWriteLock(&m_oRWLock[iIndex]);
            m_oMap[iIndex][iter->first] = iter->second;
        }
        return 0;
    }
    leveldb::WriteOptions tOpt;
    //tOpt.sync = true;

    leveldb::WriteBatch wb;
    for (auto iter = vecKeyValue.begin(); iter != vecKeyValue.end(); ++iter)
    {
        wb.Put(iter->first, iter->second);
    }

    leveldb::Status s = m_poLevelDB->Write(tOpt, &wb);
    if (!s.ok())
    {
        return Certain::eRetCodePLogPutErr;
    }
    return 0;
}

int clsKVEngine::Get(const string &strKey, string &strValue)
{
    if (m_bUseHash)
    {
        uint32_t iIndex = Certain::Hash(strKey) % kGroupNum;
        Certain::clsThreadReadLock oReadLock(&m_oRWLock[iIndex]);
        auto iter = m_oMap[iIndex].find(strKey);
        if (iter == m_oMap[iIndex].end())
        {
            return Certain::eRetCodeNotFound;
        }
        strValue = iter->second;
        return 0;
    }
    static leveldb::ReadOptions tOpt;

    leveldb::Status s = m_poLevelDB->Get(tOpt, strKey, &strValue);
    if (!s.ok())
    {
        if (s.IsNotFound())
        {
            return Certain::eRetCodeNotFound;
        }
        return Certain::eRetCodePLogGetErr;
    }
    return 0;
}

int clsKVEngine::RangeLoad(const string &strStart, const string &strEnd,
		vector< pair<string, string> > &vecKeyValue)
{
	vecKeyValue.clear();

    static leveldb::ReadOptions tOpt;

    leveldb::Iterator *iter = m_poLevelDB->NewIterator(tOpt);

    for (iter->Seek(strStart); iter->Valid(); iter->Next())
    {
        string strKey = iter->key().ToString();
        if (strKey >= strEnd)
        {
            break;
        }
        vecKeyValue.push_back(make_pair(strKey, iter->value().ToString()));
    }

	return 0;
}
