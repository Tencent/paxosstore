#include "certain/Certain.h"
#include "ConnWorker.h"
#include "EntityWorker.h"
#include "PLogWorker.h"
#include "DBWorker.h"
#include "WakeUpPipeMng.h"

namespace Certain
{

int clsCertainWrapper::GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo, EntityMeta_t &tMeta)
{
    int iRet = clsEntityGroupMng::GetInstance()->GetEntityInfo(iEntityID, tEntityInfo);
    if (iRet != 0)
    {
        CertainLogError("GetEntityInfo iEntityID %lu ret %d", iEntityID, iRet);
    }

    PLogEntityMeta_t tPLogMeta = { 0 };
    iRet = m_poPLogEngine->GetPLogEntityMeta(iEntityID, tPLogMeta);
    if (iRet != 0)
    {
        CertainLogError("GetPLogEntityMeta iEntityID %lu ret %d", iEntityID, iRet);
    }
    tMeta.iMaxPLogEntry = tPLogMeta.iMaxPLogEntry;

    uint64_t iMaxCommitedEntry = 0;
    uint32_t iFlag = 0;
    iRet = m_poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
    if (iRet != 0)
    {
        CertainLogError("GetEntityMeta iEntityID %lu ret %d", iEntityID, iRet);
    }

    tMeta.iMaxCommitedEntry = iMaxCommitedEntry;
    tMeta.iDBFlag = iFlag;

    return 0;
}

int clsCertainWrapper::ExplicitGetAll(uint64_t iEntityID)
{
    // Notify entityworker to GetAll if it is not GetAlling.
    clsRecoverCmd *poCmd = new clsRecoverCmd(iEntityID, 0);
    clsAutoDelete<clsRecoverCmd> oAuto(poCmd);

    poCmd->SetTimestampUS(GetCurrTimeUS());
    poCmd->SetMaxCommitedEntry(0);
    poCmd->SetCheckGetAll(true);
    OSS::ReportCheckGetAll();

    int iRet = SyncWaitCmd(poCmd);
    if (iRet != 0)
    {
        CertainLogError("CheckGetAll iEntityID %lu SyncWaitCmd ret %d",
                iEntityID, iRet);
    }

    CertainLogZero("GetAll iEntityID %lu ret %d", iEntityID, iRet);

    return 0;
}

int clsCertainWrapper::EntityCatchUp(uint64_t iEntityID, uint64_t &iMaxCommitedEntry)
{
    const uint32_t iMaxCommitNum = GetConf()->GetMaxCommitNum();

    uint32_t iCommitCnt = 0;

    uint64_t iEntry = 0;
    std::string strWriteBatch;

    uint32_t iFlag = 0;
    iMaxCommitedEntry = 0;
    int iRet = m_poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
    if (iRet != 0 && iRet != eRetCodeNotFound)
    {
        CertainLogError("iEntityID %lu GetEntityMeta ret %d", iEntityID, iRet); 
        return iRet;
    }

    if (iFlag == kDBFlagCheckGetAll)
    {
        CertainLogError("CheckGetAll iEntityID %lu", iEntityID);

        // Notify entityworker to GetAll if it is not GetAlling.
        clsRecoverCmd *poCmd = new clsRecoverCmd(iEntityID, iMaxCommitedEntry);
        clsAutoDelete<clsRecoverCmd> oAuto(poCmd);

        poCmd->SetTimestampUS(GetCurrTimeUS());
        poCmd->SetMaxCommitedEntry(iMaxCommitedEntry);
        poCmd->SetCheckGetAll(true);
        OSS::ReportCheckGetAll();

        iRet = SyncWaitCmd(poCmd);
        if (iRet != 0)
        {
            CertainLogError("CheckGetAll iEntityID %lu SyncWaitCmd ret %d",
                    iEntityID, iRet);
            return iRet;
        }

        iFlag = 0;
        iMaxCommitedEntry = 0;
        iRet = m_poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
        if ((iRet != 0 && iRet != eRetCodeNotFound) || iFlag != 0)
        {
            CertainLogError("iEntityID %lu GetEntityMeta iFlag %u ret %d",
                    iEntityID, iFlag, iRet); 
            if (iFlag == 0)
            {
                return eRetCodeDBLoadErr;
            }
            else
            {
                return eRetCodeDBStatusErr;
            }
        }
    }
    else if (iFlag != 0)
    {
        return eRetCodeDBStatusErr;
    }

    while (iCommitCnt < iMaxCommitNum)
    {
        iRet = CheckDBStatus(iEntityID, iMaxCommitedEntry);
        if (iRet == eRetCodeOK)
        {
            break;
        }
        else if (iRet != eRetCodeDBLagBehind)
        {
            CertainLogError("CheckDBStatus iEntityID %lu ret %d",
                    iEntityID, iRet);
            return iRet;
        }

        iEntry = iMaxCommitedEntry + 1;

        OSS::ReportGetAndCommit();

        iRet = GetWriteBatch(iEntityID, iEntry, strWriteBatch);
        if (iRet != 0)
        {
            CertainLogError("E(%lu, %lu) GetWriteBatch ret %d", iEntityID, iEntry, iRet); 
            return iRet;
        }

        TIMERMS_START(iCommitUseTimeMS);
        iRet = m_poDBEngine->Commit(iEntityID, iEntry, strWriteBatch);
        TIMERMS_STOP(iCommitUseTimeMS);
        OSS::ReportDBCommit(iRet, iCommitUseTimeMS);
        if (iRet != 0)
        {
            CertainLogError("E(%lu, %lu) Commit ret %d", iEntityID, iEntry, iRet); 
            return iRet;
        }

        iCommitCnt++;
        iMaxCommitedEntry++;
    }

    if (iCommitCnt == iMaxCommitNum && CheckDBStatus(iEntityID, iMaxCommitedEntry) != eRetCodeOK)
    {
        CertainLogError("iEntityID %lu iCommitCnt == iMaxCommitNum %u", iEntityID, iMaxCommitNum);
        return Certain::eRetCodeDBCommitLimited;
    }

    return 0;
}

int clsCertainWrapper::CatchUpAndRunPaxos(uint64_t iEntityID, 
        uint16_t hSubCmdID, const vector<uint64_t> &vecWBUUID,
        const string &strWriteBatch)
{
    uint64_t iEntry = 0;

    int iRet = EntityCatchUp(iEntityID, iEntry);
    if(iRet < 0)
    {
        return iRet;
    }

    iEntry++;

    // It's estimated One uint64_t uuid is 32 bytes in pb conservatively.
    if (strWriteBatch.size() + vecWBUUID.size() * 32 > MAX_WRITEBATCH_SIZE)
    {
        CertainLogError("BUG maybe strWriteBatch.size %lu vecWBUUID.size %lu",
                strWriteBatch.size(), vecWBUUID.size());
        return eRetCodeSizeExceed;
    }

    TIMERMS_START(iUseTimeMS);

    uint64_t iUUID = 0;
    if (strWriteBatch.size() == 0)
    {
        AssertEqual(0, vecWBUUID.size());
        iUUID = clsCmdFactory::GetInstance()->GetNextUUID();
    }
    else
    {
        //AssertLess(0, vecWBUUID.size());
    }

    clsWriteBatchCmd *poWB = new clsWriteBatchCmd(hSubCmdID, iUUID,
            vecWBUUID, strWriteBatch);
    clsAutoDelete<clsWriteBatchCmd> oAuto(poWB);

    poWB->SetTimestampUS(GetCurrTimeUS());

    poWB->SetEntityID(iEntityID);
    poWB->SetEntry(iEntry);
    poWB->SetEvalOnly(true);
    poWB->SetReadOnly(strWriteBatch.size() == 0);

    iRet = SyncWaitCmd(poWB);
    TIMERMS_STOP(iUseTimeMS);
    OSS::ReportRunPaxosTimeMS(iRet, iUseTimeMS);

    return iRet;
}

// Abort if return error.
int clsCertainWrapper::Init(clsCertainUserBase *poCertainUser,
        clsPLogBase *poPLogEngine, clsDBBase *poDBEngine, clsConfigure *poConf)
{
    int iRet;

    m_poCertainUser = poCertainUser;
    m_poPLogEngine = poPLogEngine;
    m_poDBEngine = poDBEngine;
    m_poConf = poConf;

    assert(CERTAIN_IO_BUFFER_SIZE == 2 * (MAX_WRITEBATCH_SIZE + 1000));

    OSS::SetCertainOSSIDKey(m_poConf->GetOSSIDKey());

    iRet = OpenLog(m_poConf->GetLogPath().c_str(), m_poConf->GetLogLevel(),
            m_poConf->GetUseConsole(), m_poConf->GetUseCertainLog());
    if (iRet != 0)
    {
        CertainLogFatal("OpenLog ret %d", iRet);
        return -3;
    }

    m_poConf->PrintAll();

    iRet = poCertainUser->InitServerAddr(m_poConf);
    if (iRet != 0)
    {
        CertainLogFatal("poCertainUser->InitServerAddr ret %d", iRet);
        return -5;
    }

    iRet = clsEntryStateMachine::Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogFatal("clsEntryStateMachine::Init ret %d", iRet);
        return -6;
    }

    iRet = clsIOWorkerRouter::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogFatal("clsIOWorkerRouter::Init ret %d", iRet);
        return -7;
    }

    iRet = InitManagers();
    if (iRet != 0)
    {
        return -8;
    }

    iRet = clsCmdFactory::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogFatal("clsCmdFactory::GetInstance()->Init ret %d", iRet);
        return -9;
    }

    clsPLogBase::InitUseTimeStat();
    clsIOWorker::InitUseTimeStat();

    iRet = InitWorkers();
    if (iRet != 0)
    {
        CertainLogFatal("InitWorkers ret %d", iRet);
        return -10;
    }

    iRet = clsCatchUpWorker::GetInstance()->Init(m_poConf, m_poCertainUser);
    if (iRet != 0)
    {
        CertainLogFatal("clsCatchUpWorker::GetInstance()->Init ret %d", iRet);
        return -11;
    }

    m_vecWorker.push_back(clsCatchUpWorker::GetInstance());

    m_poQueueMng = clsAsyncQueueMng::GetInstance();
    m_poPipeMng = clsAsyncPipeMng::GetInstance();
    m_poEntityGroupMng = clsEntityGroupMng::GetInstance();

    return 0;
}

void clsCertainWrapper::Destroy()
{
    clsCatchUpWorker::GetInstance()->Destroy();

    clsCmdFactory::GetInstance()->Destroy();

    DestroyWorkers();

    DestroyManagers();

    delete m_poConf, m_poConf = NULL;
}

void clsCertainWrapper::TriggeRecover(uint64_t iEntityID, uint64_t iCommitedEntry)
{
    // Recover the info of entity and entry from db and plog.
    clsRecoverCmd *poCmd = new clsRecoverCmd(iEntityID, iCommitedEntry);
    clsAutoDelete<clsRecoverCmd> oAuto(poCmd);

    poCmd->SetTimestampUS(GetCurrTimeUS());

    int iRet = SyncWaitCmd(poCmd);
    CertainLogError("triggle iEntityID %lu SyncWaitCmd ret %d",
            iEntityID, iRet);
}

int clsCertainWrapper::CheckDBStatus(uint64_t iEntityID,
        uint64_t iCommitedEntry)
{
    uint64_t iMaxContChosenEntry = 0;
    uint64_t iMaxChosenEntry = 0;
    uint64_t iLeaseTimeoutMS = 0;

    bool bTriggleRecovered = false;

    int iRet = m_poEntityGroupMng->GetMaxChosenEntry(iEntityID,
            iMaxContChosenEntry, iMaxChosenEntry, iLeaseTimeoutMS);
    if (iRet == eRetCodeNotFound)
    {
        bTriggleRecovered = true;

        // Recover the info of entity and entry from db and plog.
        clsRecoverCmd *poCmd = new clsRecoverCmd(iEntityID, iCommitedEntry);
        clsAutoDelete<clsRecoverCmd> oAuto(poCmd);

        poCmd->SetTimestampUS(GetCurrTimeUS());

        iRet = SyncWaitCmd(poCmd);
        if (iRet != 0)
        {
            CertainLogError("iEntityID %lu SyncWaitCmd ret %d",
                    iEntityID, iRet);
            return iRet;
        }

        iMaxContChosenEntry = poCmd->GetMaxContChosenEntry();
        iMaxChosenEntry = poCmd->GetMaxChosenEntry();
    }
    else if (iRet != 0)
    {
        CertainLogError("iEntityID %lu GetMaxChosenEntry iRet %d",
                iEntityID, iRet);
        return iRet;
    }
    else if (iLeaseTimeoutMS > 0)
    {
        OSS::ReportLeaseWait();
        poll(NULL, 0, iLeaseTimeoutMS);

        CertainLogError("iEntityID %lu wait iLeaseTimeoutMS %lu",
                iEntityID, iLeaseTimeoutMS);

        iRet = m_poEntityGroupMng->GetMaxChosenEntry(iEntityID,
                iMaxContChosenEntry, iMaxChosenEntry, iLeaseTimeoutMS);
        if (iRet != 0)
        {
            CertainLogError("iEntityID %lu iLeaseTimeoutMS %lu ret %d",
                    iEntityID, iLeaseTimeoutMS, iRet);
            return iRet;
        }
    }

    // iMaxContChosenEntry may update posterior to iCommitedEntry.
    if (iMaxContChosenEntry < iCommitedEntry)
    {
        iMaxContChosenEntry = iCommitedEntry;
    }

    if (iCommitedEntry + m_poConf->GetMaxCatchUpNum() <= iMaxContChosenEntry)
    {
        // All entrys of the entity are eliminated, help trigger db catchup.
        if (iMaxContChosenEntry == iMaxChosenEntry)
        {
            CertainLogError("notify_db iEntityID %lu entrys: %lu %lu",
                    iEntityID, iCommitedEntry, iMaxContChosenEntry);
            clsDBWorker::NotifyDBWorker(iEntityID);
        }

        CertainLogError("iEntityID %lu entrys: %lu %lu %lu",
                iEntityID, iCommitedEntry, iMaxContChosenEntry, iMaxChosenEntry);
        if (!bTriggleRecovered)
        {
            TriggeRecover(iEntityID, iCommitedEntry);
        }
        return eRetCodeCatchUp;
    }

    if (iMaxContChosenEntry < iMaxChosenEntry)
    {
        CertainLogError("iEntityID %lu entrys: %lu %lu %lu",
                iEntityID, iCommitedEntry, iMaxContChosenEntry, iMaxChosenEntry);
        if (!bTriggleRecovered)
        {
            TriggeRecover(iEntityID, iCommitedEntry);
        }
        return eRetCodeCatchUp;
    }

    if (iCommitedEntry >= iMaxChosenEntry)
    {
        return eRetCodeOK;
    }
    else
    {
        return eRetCodeDBLagBehind;
    }
}

int clsCertainWrapper::GetWriteBatch(uint64_t iEntityID, uint64_t iEntry,
        string &strWriteBatch, uint64_t *piValueID)
{
    EntryRecord_t tRecord;
    int iRet = m_poPLogEngine->GetRecord(iEntityID, iEntry, tRecord);
    if (iRet != 0)
    {
        if (iRet != eRetCodeNotFound)
        {
            CertainLogFatal("BUG probably E(%lu, %lu) ret %d",
                    iEntityID, iEntry, iRet);
            return iRet;
        }

        CertainLogInfo("E(%lu, %lu) not found", iEntityID, iEntry);
        return eRetCodeNotFound;
    }

    if (!tRecord.bChosen)
    {
        CertainLogInfo("unchosen: %s", EntryRecordToString(tRecord).c_str());
        return eRetCodeNotFound;
    }

    if (piValueID != NULL)
    {
        *piValueID = tRecord.tValue.iValueID;
    }

    strWriteBatch = tRecord.tValue.strValue;

    return 0;
}

int clsCertainWrapper::SyncWaitCmd(clsClientCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    uint64_t iEntry = poCmd->GetEntry();

    uint32_t iPipeIdx;
    int iRet = m_poPipeMng->GetIdlePipeIdx(iPipeIdx, iEntityID);
    if (iRet != 0)
    {
        CertainLogError("E(%lu, %lu) GetIdlePipeIdx ret %d",
                iEntityID, iEntry, iRet);
        return iRet;
    }

    poCmd->SetPipeIdx(iPipeIdx);

    uint32_t iIOWorkerID = Hash(iEntityID) % m_poConf->GetIOWorkerNum();
    poCmd->SetIOTracker(IOTracker_t(0, 0, iIOWorkerID));

    uint32_t iEntityWorkerID = Hash(iEntityID) % m_poConf->GetEntityWorkerNum();
    clsIOReqQueue *poIOReqQueue = m_poQueueMng->GetIOReqQueue(iEntityWorkerID);

    iRet = poIOReqQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        m_poPipeMng->PutIdlePipeIdx(iPipeIdx);
        CertainLogError("PushByMultiThread ret %d", iRet);
        return eRetCodeQueueFailed;
    }

    uintptr_t iCheck = (uintptr_t)poCmd;

    CertainLogDebug("iPipeIdx %u iPtr %lu E(%lu, %lu) iUUID %lu",
            iPipeIdx, iCheck, iEntityID, iEntry, poCmd->GetUUID());

    bool bOneMoreTry = false;
    iRet = m_poPipeMng->SyncWaitByPipeIdx(iPipeIdx, iCheck);
    if (iRet == eRetCodePipePtrErr)
    {
        bOneMoreTry = true;
        // Try one more time, prev timeout ptr may come first.
        // There's probably some BUG in certain.
        iRet = m_poPipeMng->SyncWaitByPipeIdx(iPipeIdx, iCheck);
    }

    if (iRet != 0)
    {
        CertainLogFatal("BUG probably ibOneMoreTry %u ret %d cmd: %s",
                bOneMoreTry, iRet, poCmd->GetTextCmd().c_str());
        m_poPipeMng->PutIdlePipeIdx(iPipeIdx);
        return eRetCodePipeWaitFailed;
    }

    m_poPipeMng->PutIdlePipeIdx(iPipeIdx);

    return poCmd->GetResult();
}

int clsCertainWrapper::RunPaxos(uint64_t iEntityID, uint64_t iEntry,
        uint16_t hSubCmdID, const vector<uint64_t> &vecWBUUID,
        const string &strWriteBatch)
{
    if (m_poConf->GetEnableLearnOnly())
    {
        CertainLogError("E(%lu, %lu) reject if learn only %u",
                iEntityID, iEntry, m_poConf->GetEnableLearnOnly());
        return eRetCodeRejectAll;
    }

    for (uint32_t i = 0; i < vecWBUUID.size(); ++i)
    {
        if (clsUUIDGroupMng::GetInstance()->IsUUIDExist(iEntityID, vecWBUUID[i]))
        {
            return eRetCodeDupUUID;
        }
    }

    // It's estimated One uint64_t uuid is 32 bytes in pb conservatively.
    if (strWriteBatch.size() + vecWBUUID.size() * 32 > MAX_WRITEBATCH_SIZE)
    {
        CertainLogError("BUG maybe strWriteBatch.size %lu vecWBUUID.size %lu",
                strWriteBatch.size(), vecWBUUID.size());
        return eRetCodeSizeExceed;
    }

    TIMERMS_START(iUseTimeMS);

    uint64_t iUUID = 0;
    if (strWriteBatch.size() == 0)
    {
        AssertEqual(0, vecWBUUID.size());
        iUUID = clsCmdFactory::GetInstance()->GetNextUUID();
    }
    else
    {
        //AssertLess(0, vecWBUUID.size());
    }

    clsWriteBatchCmd *poWB = new clsWriteBatchCmd(hSubCmdID, iUUID,
            vecWBUUID, strWriteBatch);
    clsAutoDelete<clsWriteBatchCmd> oAuto(poWB);

    poWB->SetTimestampUS(GetCurrTimeUS());

    poWB->SetEntityID(iEntityID);
    poWB->SetEntry(iEntry);
    poWB->SetEvalOnly(true);
    poWB->SetReadOnly(strWriteBatch.size() == 0);

    int iRet = SyncWaitCmd(poWB);
    TIMERMS_STOP(iUseTimeMS);
    OSS::ReportRunPaxosTimeMS(iRet, iUseTimeMS);

    return iRet;
}

int clsCertainWrapper::InitWorkers()
{
    clsThreadBase *poWorker = NULL;
    m_vecWorker.clear();

    poWorker = new clsConnWorker(m_poConf);
    m_vecWorker.push_back(poWorker);

    for (uint32_t i = 0; i < m_poConf->GetIOWorkerNum(); ++i)
    {
        poWorker = new clsIOWorker(i, m_poConf);
        m_vecWorker.push_back(poWorker);
    }

    assert(m_poConf->GetIOWorkerNum() < MAX_IO_WORKER_NUM);
    assert(m_poConf->GetEntityWorkerNum() < MAX_ENTITY_WORKER_NUM);
    for (uint32_t i = 0; i < m_poConf->GetEntityWorkerNum(); ++i)
    {
        poWorker = new clsEntityWorker(i, m_poConf);
        m_vecWorker.push_back(poWorker);
    }

    uint32_t iStartRoutineID = m_poCertainUser->GetStartRoutineID();
    //assert(iStartRoutineID > 0);
    printf("iStartRoutineID %u\n", iStartRoutineID);

    for (uint32_t i = 0; i < m_poConf->GetDBWorkerNum(); ++i)
    {
        poWorker = new clsDBWorker(i, m_poConf, m_poDBEngine, iStartRoutineID);
        m_vecWorker.push_back(poWorker);
        iStartRoutineID += m_poConf->GetDBRoutineCnt();
    }

    for (uint32_t i = 0; i < m_poConf->GetGetAllWorkerNum(); ++i)
    {
        poWorker = new clsGetAllWorker(i, m_poConf, iStartRoutineID);
        m_vecWorker.push_back(poWorker);
        iStartRoutineID += m_poConf->GetGetAllRoutineCnt();
    }

    for (uint32_t i = 0; i < m_poConf->GetPLogWorkerNum(); ++i)
    {
        poWorker = new clsPLogWorker(i, m_poConf, m_poPLogEngine, iStartRoutineID);
        m_vecWorker.push_back(poWorker);
        iStartRoutineID += m_poConf->GetPLogRoutineCnt();
    }

    return 0;
}

int clsCertainWrapper::InitManagers()
{
    int iRet;

    CertainLogImpt("Start InitManagers...");
    fprintf(stderr, "Start InitManagers...\n");

    iRet = clsUUIDGroupMng::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogError("clsUUIDGroupMng init ret %d", iRet);
        return -1;
    }

    iRet = clsAsyncPipeMng::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogError("clsAsyncPipeMng init ret %d", iRet);
        return -2;
    }

    iRet = clsAsyncQueueMng::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogError("clsAsyncQueueMng init ret %d", iRet);
        return -3;
    }

    iRet = clsConnInfoMng::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogError("clsConnInfoMng init ret %d", iRet);
        return -4;
    }

    iRet = clsEntityGroupMng::GetInstance()->Init(m_poConf);
    if (iRet != 0)
    {
        CertainLogError("clsEntityGroupMng init ret %d", iRet);
        return -5;
    }

    return 0;
}

void clsCertainWrapper::DestroyManagers()
{
    clsEntityGroupMng::GetInstance()->Destroy();
    clsConnInfoMng::GetInstance()->Destroy();
    clsAsyncQueueMng::GetInstance()->Destroy();
    clsAsyncPipeMng::GetInstance()->Destroy();
    clsUUIDGroupMng::GetInstance()->Destroy();
}

int clsCertainWrapper::StartWorkers()
{
    vector<clsThreadBase *>::iterator iter;
    for (iter = m_vecWorker.begin();
            iter != m_vecWorker.end(); ++iter)
    {
        (*iter)->Start();
    }
    return 0;
}

void clsCertainWrapper::StopWorkers()
{
    vector<clsThreadBase *>::reverse_iterator riter;
    for (riter = m_vecWorker.rbegin();
            riter != m_vecWorker.rend(); ++riter)
    {
        (*riter)->SetStopFlag();
    }
}

bool clsCertainWrapper::CheckIfAllWorkerExited()
{
    uint32_t iExitedCnt = 0;

    vector<clsThreadBase *>::iterator iter;
    for (iter = m_vecWorker.begin();
            iter != m_vecWorker.end(); ++iter)
    {
        if ((*iter)->IsExited())
        {
            iExitedCnt++;
        }
    }

    CertainLogInfo("iExitedCnt %u m_vecWorker.size %lu",
            iExitedCnt, m_vecWorker.size());

    return iExitedCnt == m_vecWorker.size();
}

void clsCertainWrapper::DestroyWorkers()
{
    vector<clsThreadBase *>::reverse_iterator riter;
    for (riter = m_vecWorker.rbegin();
            riter != m_vecWorker.rend(); ++riter)
    {
        Assert((*riter)->IsExited());
        delete *riter, *riter = NULL;
    }
    m_vecWorker.clear();
}

int clsCertainWrapper::EvictEntity(uint64_t iEntityID)
{
    // Notify entityworker to GetAll if it is not GetAlling.
    clsRecoverCmd *poCmd = new clsRecoverCmd(iEntityID, 0);
    clsAutoDelete<clsRecoverCmd> oAuto(poCmd);

    poCmd->SetTimestampUS(GetCurrTimeUS());
    poCmd->SetEvictEntity(true);

    int iRet = SyncWaitCmd(poCmd);
    if (iRet != 0)
    {
        CertainLogError("EvictEntity iEntityID %lu SyncWaitCmd ret %d",
                iEntityID, iRet);
    }

    return iRet;
}

bool clsCertainWrapper::CheckIfEntryDeletable(uint64_t iEntityID,
        uint64_t iEntry, uint64_t iTimestampMS)
{
    uint64_t iCurrTimeMS = GetCurrTimeMS();
    if (iCurrTimeMS < iTimestampMS + m_poConf->GetPLogExpireTimeMS())
    {
        return false;
    }

    uint64_t iMaxCommitedEntry = 0;
    uint32_t iFlag = 0;
    int iRet = m_poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
    if (iRet != 0)
    {
        CertainLogError("GetEntityMeta iEntityID %lu ret %d", iEntityID, iRet);
        return false;
    }

    if (iMaxCommitedEntry < iEntry)
    {
        return false;
    }

    CertainLogImpt("E(%lu, %lu) iMaxCommitedEntry %lu",
            iEntityID, iEntry, iMaxCommitedEntry);

    return true;
}

void clsCertainWrapper::Run()
{
    int iRet = StartWorkers();
    if (iRet != 0)
    {
        CertainLogFatal("StartWorkers ret %d", iRet);
        Assert(false);
    }

    CertainLogImpt("%lu workers started.", m_vecWorker.size());
    printf("%lu workers started.\n", m_vecWorker.size());

    m_poCertainUser->OnReady();

    bool bStopWorkersCalled = false;

    int iSleepCnt = 0;
    while (1)
    {
        // 100us
        usleep(100);
        iSleepCnt++;

        // every 500us
        uint32_t iWakeUp = m_poConf->GetWakeUpTimeoutUS() / 100;
        if (iWakeUp > 0 && iSleepCnt % iWakeUp == 0)
        {
            clsWakeUpPipeMng::GetInstance()->WakeupAll();
        }

        // every 5s
        if (iSleepCnt % 50000 == 0)
        {
            clsPLogBase::PrintUseTimeStat();
            clsIOWorker::PrintUseTimeStat();
            clsAsyncQueueMng::GetInstance()->PrintAllStat();
        }

        // for sleep 1s
        if (iSleepCnt % 10000 != 0)
        {
            continue;
        }

        m_poCertainUser->UpdateServerAddr(m_poConf);

        m_poConf->LoadAgain();

        if (clsThreadBase::IsStopFlag())
        {
            if (!bStopWorkersCalled)
            {
                StopWorkers();
                bStopWorkersCalled = true;
            }
            else if (CheckIfAllWorkerExited())
            {
                break;
            }
        }
    }

    clsThreadBase::SetExiting();
}

} // namespace Certain
