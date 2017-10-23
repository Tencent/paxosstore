#include "DBImpl.h"

#include <grpc++/grpc++.h>

#include "Certain.pb.h"
#include "Configure.h"

#include "example.grpc.pb.h"
#include "example/CertainUserImpl.h"
#include "example/Client.h"
#include "example/Coding.h"

const uint32_t clsDBImpl::kSnapshotTimeOut = 60;
const uint32_t clsDBImpl::kSnapshotCount = 5;

int clsDBImpl::MultiPut(dbtype::WriteBatch* oWB) 
{
    clsAutoDisableHook oAuto;
    static dbtype::WriteOptions tOpt;

    dbtype::Status s = m_poLevelDB->Write(tOpt, oWB);
    if (!s.ok())
    {
        return Certain::eRetCodeDBPutErr;
    }
    return Certain::eRetCodeOK;
}

int clsDBImpl::Commit(uint64_t iEntityID, uint64_t iEntry,
        const std::string &strWriteBatch)
{
    clsAutoDisableHook oAuto;
    dbtype::WriteBatch oWB(strWriteBatch);
    if (strWriteBatch.empty()) oWB.Clear();

    std::string strMetaKey;
    EncodeEntityMetaKey(strMetaKey, iEntityID);

    std::string strMetaValue;
    CertainPB::DBEntityMeta tDBEntityMeta;
    tDBEntityMeta.set_max_commited_entry(iEntry);
    assert(tDBEntityMeta.SerializeToString(&strMetaValue));

    oWB.Put(strMetaKey, strMetaValue);

//    size_t write_size = strWriteBatch.size();
//    Checks available size of disk.

    return MultiPut(&oWB);
}

int clsDBImpl::GetEntityMeta(uint64_t iEntityID, uint64_t &iMaxCommitedEntry, uint32_t &iFlag)
{
    clsAutoDisableHook oAuto;
    return GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag, NULL);
}

int clsDBImpl::GetEntityMeta(uint64_t iEntityID, uint64_t &iCommitedEntry,
        uint32_t &iFlag, const dbtype::Snapshot* poSnapshot)
{
    clsAutoDisableHook oAuto;
    std::string strMetaKey;
    EncodeEntityMetaKey(strMetaKey, iEntityID);

    std::string strMetaValue;
    
    dbtype::ReadOptions tOpt;
    tOpt.snapshot = poSnapshot;

    dbtype::Status s = m_poLevelDB->Get(tOpt, strMetaKey, &strMetaValue);
    if (!s.ok())
    {
        if (s.IsNotFound())
        {
            return Certain::eRetCodeNotFound;
        }

    }

    CertainPB::DBEntityMeta tDBEntityMeta;
    if (!tDBEntityMeta.ParseFromString(strMetaValue))
    {
        return Certain::eRetCodeParseProtoErr;
    }

    iCommitedEntry = tDBEntityMeta.max_commited_entry();
    iFlag = tDBEntityMeta.flag();

    return Certain::eRetCodeOK;
}

int clsDBImpl::SetEntityMeta(uint64_t iEntityID, uint64_t iMaxCommitedEntry, uint32_t iFlag)
{
    clsAutoDisableHook oAuto;
    uint64_t iOldMCEntry = 0;
    uint32_t iOldFlag = 0;

    int iRet = GetEntityMeta(iEntityID, iOldMCEntry, iOldFlag);

    if (iRet != 0 && iRet != Certain::eRetCodeNotFound)
    {
        return Certain::eRetCodeGetDBEntityMetaErr;
    }

    CertainPB::DBEntityMeta tDBEntityMeta;

    if (iMaxCommitedEntry != uint64_t(-1))
    {
        tDBEntityMeta.set_max_commited_entry(iMaxCommitedEntry);
    }
    else
    {
        if (iMaxCommitedEntry <= iOldMCEntry)
        {
            CertainLogFatal("iEntityID %lu iMaxCommitedEntry %lu <= %lu",
                    iEntityID, iMaxCommitedEntry, iOldMCEntry);
            return Certain::eRetCodeEntryErr;
        }
        tDBEntityMeta.set_max_commited_entry(iOldMCEntry);
    }

    if (iFlag != uint32_t(-1))
    {
        tDBEntityMeta.set_flag(iFlag);
    }
    else
    {
        tDBEntityMeta.set_flag(iOldFlag);
    }

    std::string strMetaKey;
    EncodeEntityMetaKey(strMetaKey, iEntityID);

    std::string strMetaValue;
    assert(tDBEntityMeta.SerializeToString(&strMetaValue));

    dbtype::WriteOptions tOpt;
    dbtype::Status s = m_poLevelDB->Put(tOpt, strMetaKey, strMetaValue);
    if (!s.ok())
    {
        return Certain::eRetCodeDBPutErr;
    }
    return Certain::eRetCodeOK;
}

int clsDBImpl::GetAllAndSet(uint64_t iEntityID, uint32_t iAcceptorID, uint64_t &iMaxCommitedEntry)
{
    clsAutoDisableHook oAuto;
    CertainLogInfo("Start GetAllAndSet()");

    int iRet = 0;

    // Step 1: Sets flags for deleting.
    {
        Certain::clsAutoEntityLock oEntityLock(iEntityID);
        iRet = SetEntityMeta(iEntityID, -1, 1);
        if (iRet != 0) 
        {
            CertainLogError("SetFlag() iEntityID %lu iRet %d", iEntityID, iRet);
            return Certain::eRetCodeSetFlagErr;
        }
    }

    // Step 2: Deletes all kvs in db related to iEntityID.
    std::string strNextKey;
    EncodeInfoKey(strNextKey, iEntityID, 0);
    do 
    {
        iRet = Clear(iEntityID, strNextKey);
        if (iRet != 0)
        {
            CertainLogError("Clear() iEntityID %lu iRet %d", iEntityID, iRet);
            return Certain::eRetCodeClearDBErr;
        }

        if (!strNextKey.empty()) poll(NULL, 0, 1);
    } while (!strNextKey.empty());

    // Step 3: Gets local machine ID.
    clsCertainUserImpl* poCertainUser = dynamic_cast<clsCertainUserImpl *>(
            Certain::clsCertainWrapper::GetInstance()->GetCertainUser());
    uint32_t iLocalAcceptorID = 0;
    iRet = poCertainUser->GetLocalAcceptorID(iEntityID, iLocalAcceptorID);
    if (iRet != 0)
    {
        CertainLogError("GetLocalAcceptorID() iEntityID %lu iRet %d", iEntityID, iRet);
        return Certain::eRetCodeGetLocalMachineIDErr;
    }

    grpc_init();

    grpc::ChannelArguments oArgs;
    oArgs.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 20);

    iRet = -1;
    // iAcceptorID is the ID of peer machine.
    uint32_t iAcceptorNum = Certain::clsCertainWrapper::GetInstance()->GetConf()->GetAcceptorNum();
    for (iAcceptorID = (iLocalAcceptorID + 1) % iAcceptorNum; 
            iRet != 0 && iAcceptorID != iLocalAcceptorID; 
            iAcceptorID = (iAcceptorID + 1) % iAcceptorNum)
    {
        std::string strAddr;
        iRet = poCertainUser->GetServiceAddr(iEntityID, iAcceptorID, strAddr);
        if (iRet != 0)
        {
            CertainLogError("GetSvrAddr() iEntityID %lu iRet %d", iEntityID, iRet);
            iRet = Certain::eRetCodeGetPeerSvrAddrErr;
            continue;
        }

        // Step 4: Gets commited entry and sets local value.
        example::GetRequest oRequest;
        oRequest.set_entity_id(iEntityID);
        example::GetResponse oResponse;

        clsClient oClient(strAddr);

        static const int kRetry = 3;
        for (int i = 0; i < kRetry; ++i) 
        {
            grpc::Status oRet = oClient.Call(oRequest.entity_id(), example::OperCode::eGetDBEntityMeta,
                    &oRequest, &oResponse, strAddr);
            iRet = oRet.error_code();
            if (iRet == 0) break;
        }

        if (iRet != 0)
        {
            CertainLogError("GetEntityMeta() iEntityID %lu iRet %d", iEntityID, iRet);
            iRet = Certain::eRetCodeGetPeerCommitedEntryErr;
            continue;
        }

        iMaxCommitedEntry = oResponse.max_commited_entry();
        uint64_t iSequenceNumber = oResponse.sequence_number();

        {
            Certain::clsAutoEntityLock oEntityLock(iEntityID);
            iRet = SetEntityMeta(iEntityID, iMaxCommitedEntry, -1);
            if (iRet != 0)
            {
                CertainLogError("SetEntityMeta() iEntityID %lu iRet %d", iEntityID, iRet);
                iRet = Certain::eRetCodeSetDBEntityMetaErr;
                continue;
            }
        }

        // Step 5: Gets data from peer endpoint.
        std::string strNextKey;
        EncodeInfoKey(strNextKey, iEntityID, 0);
        do 
        {
            std::string strWriteBatch;
            oRequest.set_max_size(1024 * 1024);
            oRequest.set_next_key(strNextKey);
            oRequest.set_sequence_number(iSequenceNumber);
            oResponse.Clear();
            for (int i = 0; i < kRetry; ++i) 
            {
                grpc::Status oRet = oClient.Call(oRequest.entity_id(), example::OperCode::eGetAllForCertain,
                        &oRequest, &oResponse, strAddr);
                iRet = oRet.error_code();
                if (iRet == 0)
                {
                    strNextKey = oResponse.next_key();
                    strWriteBatch = oResponse.value();
                    break;
                }
            }

            if (iRet != 0)
            {
                CertainLogError("GetAllForCertain() iEntityID %lu iRet %d", iEntityID, iRet);
                iRet = Certain::eRetCodeGetDataFromPeerErr;
                break;
            }

            if (!strWriteBatch.empty())
            {
                dbtype::WriteBatch oWriteBatch(strWriteBatch);
                if (strWriteBatch.empty()) oWriteBatch.Clear();
                iRet = MultiPut(&oWriteBatch);
                if (iRet != 0) 
                {
                    CertainLogError("WriteBatch::Write() iEntityID %lu iRet %d", iEntityID, iRet);
                    iRet = Certain::eRetCodeCommitLocalDBErr;
                    break;
                }
            }

            if (!strNextKey.empty()) poll(NULL, 0, 1);
        } while (!strNextKey.empty());

        if (iRet != 0) 
        {
            // Step 6: Re-deletes all kvs in db related to iEntityID.
            std::string strNextKey;
            EncodeInfoKey(strNextKey, iEntityID, 0);
            do
            {
                int iDelRet = Clear(iEntityID, strNextKey);
                if (iDelRet != 0) 
                {
                    CertainLogError("Re-clear() iEntityID %lu iRet %d", iEntityID, iRet);
                    iRet = Certain::eRetCodeReClearDBErr;
                }

                if (!strNextKey.empty()) poll(NULL, 0, 1);
            } while (!strNextKey.empty());
        }
    }

    if (iRet != 0)
    {
        CertainLogError("Abort GetAllAndSet() iEntityID %lu iRet %d", iEntityID, iRet);
        return iRet;
    }

    // Step 7: Clear flag.
    {
        Certain::clsAutoEntityLock oEntityLock(iEntityID);
        iRet = SetEntityMeta(iEntityID, -1, 0);
        if (iRet != 0)
        {
            CertainLogError("SetFlag() iEntityID %lu iRet %d", iEntityID, iRet);
            return Certain::eRetCodeClearFlagErr;
        }
    }

    CertainLogInfo("Finish GetAllAndSet()");

    return Certain::eRetCodeOK;
}

int clsDBImpl::GetBatch(uint64_t iEntityID, std::string& strNextKey, std::string* strWriteBatch, 
        const dbtype::Snapshot* poSnapshot, uint32_t iMaxSize)
{
    clsAutoDisableHook oAuto;
    if (strWriteBatch == NULL)
    {
        return Certain::eRetCodeGetBatchErr;
    }

    std::string strRep = (strWriteBatch == NULL) ? "" : *strWriteBatch;
    dbtype::WriteBatch oWB(strRep);
    if (strRep.empty()) oWB.Clear();

    dbtype::ReadOptions tOpt;
    if (poSnapshot != NULL) tOpt.snapshot = poSnapshot;
    tOpt.fill_cache = false;

    std::unique_ptr<dbtype::Iterator> iter(m_poLevelDB->NewIterator(tOpt));

    std::string strStartKey = strNextKey;
    strNextKey.clear();
    if (strStartKey.empty()) return Certain::eRetCodeOK;

    uint32_t size = 0;
    uint32_t count = 0;
    for (iter->Seek(strStartKey); iter->Valid(); iter->Next()) 
    {
        const dbtype::Slice& key = iter->key();
        if (!KeyHitEntityID(iEntityID, key))
        {
            break;
        }

        const dbtype::Slice& value = iter->value();
        size += key.size() + value.size();
        if (size >= iMaxSize && count > 0)
        {
            strNextKey = key.ToString();
            break;
        }
        ++count;
        oWB.Put(key, value);
    }

    if (strWriteBatch != NULL && dbtype::WriteBatchInternal::Count(&oWB) > 0)
        *strWriteBatch = oWB.Data();

    return Certain::eRetCodeOK;
}

int clsDBImpl::Clear(uint64_t iEntityID, std::string& strNextKey, uint32_t iMaxIterateKeyCnt)
{
    clsAutoDisableHook oAuto;
    dbtype::WriteBatch oWB;
    oWB.Clear();

    static dbtype::ReadOptions tROpt;

    std::unique_ptr<dbtype::Iterator> iter(m_poLevelDB->NewIterator(tROpt));

    std::string strStartKey = strNextKey;
    strNextKey.clear();
    if (strStartKey.empty()) return Certain::eRetCodeOK;

    uint32_t count = 0;
    for (iter->Seek(strStartKey); iter->Valid(); iter->Next())
    {
        const dbtype::Slice& key = iter->key();
        count++;
        if (count > iMaxIterateKeyCnt)
        {
            strNextKey = key.ToString();
            break;
        }

        if (KeyHitEntityID(iEntityID, key))
        {
            oWB.Delete(key);
        }
        else 
        {
            break;
        }
    }

    static dbtype::WriteOptions tWOpt;
    dbtype::Status s = m_poLevelDB->Write(tWOpt, &oWB);
    if (!s.ok())
    {
        return Certain::eRetCodeClearDBErr;
    }

    return Certain::eRetCodeOK;
}

int clsDBImpl::InsertSnapshot(uint64_t& iSequenceNumber, const dbtype::Snapshot* poSnapshot)
{
    clsAutoDisableHook oAuto;

    Certain::clsThreadLock oLock(&m_poSnapshotMapMutex);
    uint32_t iTimeStamp = time(0);
    auto iter = m_poSnapshotMap.find(iTimeStamp);
    if (iter != m_poSnapshotMap.end())
    {
        iSequenceNumber = iter->second.first;
        poSnapshot = iter->second.second->GetSnapshot();
    }
    else 
    {
        poSnapshot = m_poLevelDB->GetSnapshot();
        iSequenceNumber = reinterpret_cast<const dbtype::SnapshotImpl*>(poSnapshot)->number_;

        for (auto iter = m_poSnapshotMap.begin(); iter != m_poSnapshotMap.end(); ++iter)
        {
            if (iter->second.first == iSequenceNumber)
            {
                std::shared_ptr<clsSnapshotWrapper> poWrapper = iter->second.second;
                m_poSnapshotMap.insert(std::make_pair(iTimeStamp, std::make_pair(iSequenceNumber, poWrapper)));
                return Certain::eRetCodeOK;
            }
        }

        std::shared_ptr<clsSnapshotWrapper> poWrapper = make_shared<clsSnapshotWrapper>(m_poLevelDB, poSnapshot);
        m_poSnapshotMap.insert(std::make_pair(iTimeStamp, std::make_pair(iSequenceNumber, poWrapper)));
    }
    
    return Certain::eRetCodeOK;
}

int clsDBImpl::FindSnapshot(uint64_t iSequenceNumber, const dbtype::Snapshot* poSnapshot)
{
    clsAutoDisableHook oAuto;

    Certain::clsThreadLock oLock(&m_poSnapshotMapMutex);
    for (auto iter = m_poSnapshotMap.begin(); iter != m_poSnapshotMap.end(); ++iter)
    {
        if (iter->second.first == iSequenceNumber)
        {
            poSnapshot = iter->second.second->GetSnapshot();
            return Certain::eRetCodeOK;
        }
    }
    
    return Certain::eRetCodeSnapshotNotFoundErr;
}

void clsDBImpl::EraseSnapshot()
{
    clsAutoDisableHook oAuto;

    Certain::clsThreadLock oLock(&m_poSnapshotMapMutex);
    uint32_t iTimeStamp = time(0);
    uint32_t iCount = m_poSnapshotMap.size();
    for (auto iter = m_poSnapshotMap.begin(); iter != m_poSnapshotMap.end(); )
    {
        if ((iter->first + kSnapshotTimeOut < iTimeStamp) || (iCount > kSnapshotCount))
        {
            if (iCount > 0) --iCount;
		    m_poSnapshotMap.erase(iter++);
        } else {
            iter++;
        }
    }
}

uint64_t clsDBImpl::GetSnapshotSize()
{
    clsAutoDisableHook oAuto;

    Certain::clsThreadLock oLock(&m_poSnapshotMapMutex);
    uint64_t size = m_poSnapshotMap.size();

    return size;
}

dbtype::DB *clsDBImpl::GetDB()
{
    return m_poLevelDB;
}
