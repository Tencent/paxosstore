#include "PLogImpl.h"
#include "Coding.h"
#include "Certain.pb.h"

int clsPLogImpl::PutValue(uint64_t iEntityID, uint64_t iEntry,
        uint64_t iValueID, const string &strValue)
{
    string strKey;
    EncodePLogValueKey(strKey, iEntityID, iEntry, iValueID);

    dbtype::WriteOptions tOpt;
    dbtype::WriteBatch oWB;
    oWB.Put(strKey, strValue);

    dbtype::Status s = m_poLevelDB->Write(tOpt, &oWB);
    if (!s.ok())
    {
        return Certain::eRetCodePLogPutErr;
    }
    return 0;
}

int clsPLogImpl::GetValue(uint64_t iEntityID, uint64_t iEntry,
        uint64_t iValueID, string &strValue)
{
    string strKey;
    EncodePLogValueKey(strKey, iEntityID, iEntry, iValueID);

    dbtype::ReadOptions tOpt;
    dbtype::Status s = m_poLevelDB->Get(tOpt, strKey, &strValue);
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

int clsPLogImpl::Put(uint64_t iEntityID, uint64_t iEntry,
        const string &strRecord)
{
    string strKey;
    EncodePLogKey(strKey, iEntityID, iEntry);

    dbtype::WriteOptions tOpt;
    dbtype::WriteBatch oWB;
    oWB.Put(strKey, strRecord);

    dbtype::Status s = m_poLevelDB->Write(tOpt, &oWB);
    if (!s.ok())
    {
        return Certain::eRetCodePLogPutErr;
    }
    return 0;
}

int clsPLogImpl::Get(uint64_t iEntityID, uint64_t iEntry,
        string &strRecord)
{
    string strKey;
    EncodePLogKey(strKey, iEntityID, iEntry);

    dbtype::ReadOptions tOpt;
    dbtype::Status s = m_poLevelDB->Get(tOpt, strKey, &strRecord);
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

int clsPLogImpl::PutWithPLogEntityMeta(uint64_t iEntityID, uint64_t iEntry,
        const Certain::PLogEntityMeta_t &tMeta, const string &strRecord)
{
    string strKey;
    EncodePLogKey(strKey, iEntityID, iEntry);

    string strMetaKey;
    EncodePLogMetaKey(strMetaKey, iEntityID);

    CertainPB::PLogEntityMeta tPLogEntityMeta;
    tPLogEntityMeta.set_max_plog_entry(tMeta.iMaxPLogEntry);
    string strMetaValue;
    assert(tPLogEntityMeta.SerializeToString(&strMetaValue));

    dbtype::WriteOptions tOpt;
    dbtype::WriteBatch oWB;
    oWB.Put(strKey, strRecord);
    oWB.Put(strMetaKey, strMetaValue);

    dbtype::Status s = m_poLevelDB->Write(tOpt, &oWB);
    if (!s.ok())
    {
        return Certain::eRetCodePLogPutErr;
    }
    return 0;
}

int clsPLogImpl::GetPLogEntityMeta(uint64_t iEntityID,
        Certain::PLogEntityMeta_t &tMeta) 
{
    string strKey;
    EncodePLogMetaKey(strKey, iEntityID);

    string strMetaValue;
    dbtype::ReadOptions tOpt;
    dbtype::Status s = m_poLevelDB->Get(tOpt, strKey, &strMetaValue);
    if (!s.ok())
    {
        if (s.IsNotFound())
        {
            return Certain::eRetCodeNotFound;
        }
        return Certain::eRetCodePLogGetErr;
    }

    CertainPB::PLogEntityMeta tPLogEntityMeta;
    if (!tPLogEntityMeta.ParseFromString(strMetaValue))
    {
        return Certain::eRetCodeParseProtoErr;
    }

    tMeta.iMaxPLogEntry = tPLogEntityMeta.max_plog_entry();

    return 0;
}

int clsPLogImpl::LoadUncommitedEntrys(uint64_t iEntityID,
        uint64_t iMaxCommitedEntry, uint64_t iMaxLoadingEntry,
        vector< pair<uint64_t, string> > &vecRecord, bool &bHasMore)
{
    bHasMore = false;
    vecRecord.clear();

    string strStartKey;
    EncodePLogKey(strStartKey, iEntityID, iMaxCommitedEntry + 1);

    dbtype::ReadOptions tOpt;
    std::unique_ptr<dbtype::Iterator> iter(m_poLevelDB->NewIterator(tOpt));

    for (iter->Seek(strStartKey); iter->Valid(); iter->Next())
    {
        const dbtype::Slice& strKey = iter->key();
        uint64_t iCurrEntityID = 0;
        uint64_t iEntry = 0;
        uint64_t iValueID = 0;

        if (!DecodePLogKey(strKey, iCurrEntityID, iEntry) &&
                !DecodePLogValueKey(strKey, iCurrEntityID, iEntry, iValueID))
        {
            break;
        }

        if (iCurrEntityID > iEntityID)
        {
            break;
        }

        if (iValueID != 0)
        {
            continue;
        }

        if (iMaxLoadingEntry < iEntry)
        {
            bHasMore = true;
            break;
        }

        string strValue = iter->value().ToString();
        vecRecord.push_back(std::make_pair(iEntry, strValue));
    }

    return 0;
}
