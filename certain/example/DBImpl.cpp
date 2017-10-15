#include "DBImpl.h"

#include <grpc++/grpc++.h>

#include "Coding.h"
#include "src/EntityInfoMng.h"
#include "Common.h"
#include "Configure.h"
#include "Certain.pb.h"
#include "Command.h"
#include "example.grpc.pb.h"
#include "example/CertainUserImpl.h"
#include "example/Client.h"
// db
#include "db/write_batch_internal.h"
#include "write_batch.h"

const uint32_t clsDBImpl::kSnapshotTimeOut = 60;
const uint32_t clsDBImpl::kSnapshotCount = 5;

int clsDBImpl::Put(const string &strKey, const string &strValue, string* strWriteBatch)
{
    dbtype::WriteOptions tOpt;

    string strRep = (strWriteBatch == NULL) ? "" : *strWriteBatch;
    dbtype::WriteBatch oWB(strRep);
    if (strRep.empty()) oWB.Clear();

    oWB.Put(strKey, strValue);

    if (strWriteBatch == NULL)
    {
        dbtype::Status s = m_poLevelDB->Write(tOpt, &oWB);
        if (!s.ok())
        {
            return Certain::eRetCodeDBPutErr;
        }
    }
    else 
    {
        *strWriteBatch = oWB.Data();
    }

    return 0;
}

int clsDBImpl::MultiPut(dbtype::WriteBatch* oWB) 
{
    static dbtype::WriteOptions tOpt;

    dbtype::Status s = m_poLevelDB->Write(tOpt, oWB);
    if (!s.ok())
    {
        return Certain::eRetCodeDBPutErr;
    }
    return Certain::eRetCodeOK;
}

int clsDBImpl::Get(const string &strKey, string &strValue,
        const dbtype::Snapshot* poSnapshot)
{
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
    return 0;
}

int clsDBImpl::Delete(const string &strKey, string* strWriteBatch)
{
    dbtype::WriteOptions tOpt;

    string strRep = (strWriteBatch == NULL) ? "" : *strWriteBatch;
    dbtype::WriteBatch oWB(strRep);
    if (strRep.empty()) oWB.Clear();

    oWB.Delete(strKey);

    if (strWriteBatch == NULL)
    {
        dbtype::Status s = m_poLevelDB->Write(tOpt, &oWB);
        if (!s.ok())
        {
            return Certain::eRetCodeDBPutErr;
        }
    }
    else 
    {
        *strWriteBatch = oWB.Data();
    }

    return 0;
}

int clsDBImpl::Commit(uint64_t iEntityID, uint64_t iEntry,
        const string &strWriteBatch)
{
    dbtype::WriteBatch oWB(strWriteBatch);
    if (strWriteBatch.empty()) oWB.Clear();

    string strMetaKey;
    EncodeEntityMetaKey(strMetaKey, iEntityID);

    string strMetaValue;
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
    return GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag, NULL);
}

int clsDBImpl::GetEntityMeta(uint64_t iEntityID, uint64_t &iCommitedEntry,
        uint32_t &iFlag, const dbtype::Snapshot* poSnapshot)
{
    string strMetaKey;
    EncodeEntityMetaKey(strMetaKey, iEntityID);

    string strMetaValue;
    
    int iRet = Get(strMetaKey, strMetaValue, poSnapshot);
    if (iRet != 0) return iRet;

    CertainPB::DBEntityMeta tDBEntityMeta;
    if (!tDBEntityMeta.ParseFromString(strMetaValue))
    {
        return eRetCodeParseProtoErr;
    }

    iCommitedEntry = tDBEntityMeta.max_commited_entry();
    iFlag = tDBEntityMeta.flag();

    return eRetCodeOK;
}

int clsDBImpl::SetEntityMeta(uint64_t iEntityID, uint64_t iMaxCommitedEntry, uint32_t iFlag)
{
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

    string strMetaKey;
    EncodeEntityMetaKey(strMetaKey, iEntityID);

    string strMetaValue;
    assert(tDBEntityMeta.SerializeToString(&strMetaValue));

    return Put(strMetaKey, strMetaValue);
}

int clsDBImpl::GetAllAndSet(uint64_t iEntityID, uint32_t iAcceptorID, uint64_t &iMaxCommitedEntry)
{
    CertainLogInfo("Start GetAllAndSet()");

    int iRet = 0;

    // Step 1: Sets flags for deleting.
    {
        clsAutoEntityLock oEntityLock(iEntityID);
        iRet = SetEntityMeta(iEntityID, -1, 1);
        if (iRet != 0) 
        {
            CertainLogError("SetFlag() iEntityID %lu iRet %d", iEntityID, iRet);
            return eRetCodeSetFlagErr;
        }
    }

    // Step 2: Deletes all kvs in db related to iEntityID.
    string strNextKey;
    EncodeInfoKey(strNextKey, iEntityID, 0);
    do 
    {
        iRet = Clear(iEntityID, strNextKey);
        if (iRet != 0)
        {
            CertainLogError("Clear() iEntityID %lu iRet %d", iEntityID, iRet);
            return eRetCodeClearDBErr;
        }

        if (!strNextKey.empty()) poll(NULL, 0, 1);
    } while (!strNextKey.empty());

    // Step 3: Gets local machine ID.
    Certain::clsCertainUserBase* pCertainUser = Certain::clsCertainWrapper::GetInstance()->GetCertainUser();    
    uint32_t iLocalAcceptorID = 0;
    iRet = pCertainUser->GetLocalAcceptorID(iEntityID, iLocalAcceptorID);
    if (iRet != 0)
    {
        CertainLogError("GetLocalAcceptorID() iEntityID %lu iRet %d", iEntityID, iRet);
        return eRetCodeGetLocalMachineIDErr;
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
        Certain::InetAddr_t tAddr;
        iRet = pCertainUser->GetSvrAddr(iEntityID, iAcceptorID, tAddr);
        if (iRet != 0)
        {
            CertainLogError("GetSvrAddr() iEntityID %lu iRet %d", iEntityID, iRet);
            iRet = eRetCodeGetPeerSvrAddrErr;
            continue;
        }

        char sIP[32] = {0};
        inet_ntop(AF_INET, (void*)&tAddr.tAddr.sin_addr, sIP, sizeof(sIP));

        // Step 4: Gets commited entry and sets local value.

        string strAddr = (string)sIP + ":" + 
            to_string(dynamic_cast<clsCertainUserImpl*>(pCertainUser)->GetServicePort());

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

        uint64_t iMaxCommittedEntry = oResponse.max_commited_entry();
        uint64_t iSequenceNumber = oResponse.sequence_number();

        if (iRet != 0)
        {
            CertainLogError("GetCommittedEntry() iEntityID %lu iRet %d", iEntityID, iRet);
            iRet = eRetCodeGetPeerCommitedEntryErr;
            continue;
        }

        iMaxCommitedEntry = iMaxCommittedEntry;

        {
            clsAutoEntityLock oEntityLock(iEntityID);
            iRet = SetEntityMeta(iEntityID, iMaxCommittedEntry, -1);
            if (iRet != 0)
            {
                CertainLogError("SetCommittedEntry() iEntityID %lu iRet %d", iEntityID, iRet);
                iRet = eRetCodeSetDBEntityMetaErr;
                continue;
            }
        }

        // Step 5: Gets data from peer endpoint.
        string strNextKey;
        EncodeInfoKey(strNextKey, iEntityID, 0);
        do 
        {
            string strWriteBatch;
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
                iRet = eRetCodeGetDataFromPeerErr;
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
                    iRet = eRetCodeCommitLocalDBErr;
                    break;
                }
            }

            if (!strNextKey.empty()) poll(NULL, 0, 1);
        } while (!strNextKey.empty());

        if (iRet != 0) 
        {
            // Step 6: Re-deletes all kvs in db related to iEntityID.
            string strNextKey;
            EncodeInfoKey(strNextKey, iEntityID, 0);
            do
            {
                int iDelRet = Clear(iEntityID, strNextKey);
                if (iDelRet != 0) 
                {
                    CertainLogError("Re-clear() iEntityID %lu iRet %d", iEntityID, iRet);
                    iRet = eRetCodeReClearDBErr;
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
        clsAutoEntityLock oEntityLock(iEntityID);
        iRet = SetEntityMeta(iEntityID, -1, 0);
        if (iRet != 0)
        {
            CertainLogError("SetFlag() iEntityID %lu iRet %d", iEntityID, iRet);
            return eRetCodeClearFlagErr;
        }
    }

    CertainLogInfo("Finish GetAllAndSet()");

    return eRetCodeOK;
}

int clsDBImpl::GetBatch(uint64_t iEntityID, string& strNextKey, string* strWriteBatch, 
        const dbtype::Snapshot* poSnapshot, uint32_t iMaxSize)
{
    if (strWriteBatch == NULL)
    {
        return Certain::eRetCodeGetBatchErr;
    }

    string strRep = (strWriteBatch == NULL) ? "" : *strWriteBatch;
    dbtype::WriteBatch oWB(strRep);
    if (strRep.empty()) oWB.Clear();

    dbtype::ReadOptions tOpt;
    if (poSnapshot != NULL) tOpt.snapshot = poSnapshot;
    tOpt.fill_cache = false;

    std::unique_ptr<dbtype::Iterator> iter(m_poLevelDB->NewIterator(tOpt));

    string strStartKey = strNextKey;
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

int clsDBImpl::Clear(uint64_t iEntityID, string& strNextKey, uint32_t iMaxIterateKeyCnt)
{
    dbtype::WriteBatch oWB;
    oWB.Clear();

    static dbtype::ReadOptions tROpt;

    std::unique_ptr<dbtype::Iterator> iter(m_poLevelDB->NewIterator(tROpt));

    string strStartKey = strNextKey;
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
    assert(m_poSnapshotMapMutex != NULL && m_poSnapshotMap != NULL);

	assert(0 == pthread_mutex_lock(m_poSnapshotMapMutex));
    uint32_t iTimeStamp = time(0);
    auto iter = m_poSnapshotMap->find(iTimeStamp);
    if (iter != m_poSnapshotMap->end())
    {
        iSequenceNumber = iter->second.first;
        poSnapshot = iter->second.second->GetSnapshot();
    }
    else 
    {
        poSnapshot = m_poLevelDB->GetSnapshot();
        iSequenceNumber = reinterpret_cast<const dbtype::SnapshotImpl*>(poSnapshot)->number_;

        for (auto iter = m_poSnapshotMap->begin(); iter != m_poSnapshotMap->end(); ++iter)
        {
            if (iter->second.first == iSequenceNumber)
            {
                shared_ptr<clsSnapshotWrapper> poWrapper = iter->second.second;
                m_poSnapshotMap->insert(make_pair(iTimeStamp, make_pair(iSequenceNumber, poWrapper)));
	            assert(0 == pthread_mutex_unlock(m_poSnapshotMapMutex));
                return Certain::eRetCodeOK;
            }
        }

        std::shared_ptr<clsSnapshotWrapper> poWrapper = make_shared<clsSnapshotWrapper>(m_poLevelDB, poSnapshot);
        m_poSnapshotMap->insert(make_pair(iTimeStamp, make_pair(iSequenceNumber, poWrapper)));
    }
	assert(0 == pthread_mutex_unlock(m_poSnapshotMapMutex));
    
    return Certain::eRetCodeOK;
}

int clsDBImpl::FindSnapshot(uint64_t iSequenceNumber, const dbtype::Snapshot* poSnapshot)
{
    assert(m_poSnapshotMapMutex != NULL && m_poSnapshotMap != NULL);

	assert(0 == pthread_mutex_lock(m_poSnapshotMapMutex));
    for (auto iter = m_poSnapshotMap->begin(); iter != m_poSnapshotMap->end(); ++iter)
    {
        if (iter->second.first == iSequenceNumber)
        {
            poSnapshot = iter->second.second->GetSnapshot();
	        assert(0 == pthread_mutex_unlock(m_poSnapshotMapMutex));
            return Certain::eRetCodeOK;
        }
    }
	assert(0 == pthread_mutex_unlock(m_poSnapshotMapMutex));
    
    return Certain::eRetCodeSnapshotNotFoundErr;
}

void clsDBImpl::EraseSnapshot()
{
    assert(m_poSnapshotMapMutex != NULL && m_poSnapshotMap != NULL);

	assert(0 == pthread_mutex_lock(m_poSnapshotMapMutex));
    uint32_t iTimeStamp = time(0);
    uint32_t iCount = m_poSnapshotMap->size();
    for (auto iter = m_poSnapshotMap->begin(); iter != m_poSnapshotMap->end(); )
    {
        if ((iter->first + kSnapshotTimeOut < iTimeStamp) || (iCount > kSnapshotCount))
        {
            if (iCount > 0) --iCount;
		    m_poSnapshotMap->erase(iter++);
        } else {
            iter++;
        }
    }
	assert(0 == pthread_mutex_unlock(m_poSnapshotMapMutex));
}

uint64_t clsDBImpl::GetSnapshotSize()
{
    assert(m_poSnapshotMapMutex != NULL && m_poSnapshotMap != NULL);

	assert(0 == pthread_mutex_lock(m_poSnapshotMapMutex));
    uint64_t size = m_poSnapshotMap->size();
	assert(0 == pthread_mutex_unlock(m_poSnapshotMapMutex));

    return size;
}
