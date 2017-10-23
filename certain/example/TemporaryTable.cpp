#include "example/TemporaryTable.h"

#include "Command.h"

#include "CoHashLock.h"

clsTemporaryTable::clsTemporaryTable(dbtype::DB *poLevelDB)
{
    m_poLevelDB = poLevelDB;
    assert(m_poLevelDB);
    m_oWriteBatch.Clear();
}

int clsTemporaryTable::Put(const std::string &strKey, const std::string &strValue)
{
    clsAutoDisableHook oAuto;
    m_oKVMap[strKey] = std::make_pair(KVStatus::VALUE, strValue);
    m_oWriteBatch.Put(strKey, strValue);

    return Certain::eRetCodeOK;
}

int clsTemporaryTable::Get(const std::string &strKey, std::string &strValue, 
        const dbtype::Snapshot* poSnapshot)
{
    clsAutoDisableHook oAuto;
    auto iter = m_oKVMap.find(strKey);
    if (iter != m_oKVMap.end()) 
    {
        if (iter->second.first == KVStatus::DELETE) return Certain::eRetCodeNotFound;
        strValue = iter->second.second;
        return Certain::eRetCodeOK;
    }

    dbtype::ReadOptions tOpt;
    tOpt.snapshot = poSnapshot;

    dbtype::Status s = m_poLevelDB->Get(tOpt, strKey, &strValue);
    if (!s.ok())
    {
        if (s.IsNotFound())
        {
            return Certain::eRetCodeNotFound;
        }
        return Certain::eRetCodeDBGetErr;
    }

    return Certain::eRetCodeOK;
}

int clsTemporaryTable::Delete(const std::string& strKey)
{
    clsAutoDisableHook oAuto;
    m_oKVMap[strKey] = std::make_pair(KVStatus::DELETE, "");
    m_oWriteBatch.Delete(strKey);

    return Certain::eRetCodeOK;
}

const std::string& clsTemporaryTable::GetWriteBatchString()
{
    return m_oWriteBatch.Data();
}
