#include "DBWorker.h"
#include "EntityWorker.h"
#include "EntityInfoMng.h"

#include "co_routine.h"

namespace Certain
{

int clsDBWorker::EnterDBReqQueue(clsClientCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
    clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

    clsDBReqQueue *poQueue = poQueueMng->GetDBReqQueue(Hash(iEntityID)
            % poConf->GetDBWorkerNum());

    int iRet = poQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        OSS::ReportDBQueueErr();
        CertainLogError("PushByMultiThread ret %d cmd: %s",
                iRet, poCmd->GetTextCmd().c_str());
        return -1;
    }

    return 0;
}

int clsDBBase::MultiCommit(uint64_t iEntityID, uint64_t iMaxCommitedEntry, uint64_t iMaxTaskEntry)
{
    int iRet;

    uint64_t iMaxContChosenEntry = 0;
    uint64_t iMaxChosenEntry = 0;

    if (iMaxTaskEntry == 0)
    {
        // Apply up to the newest plog when receive a notify.
        iRet = clsEntityGroupMng::GetInstance()->GetMaxChosenEntry(iEntityID,
                iMaxContChosenEntry, iMaxChosenEntry);
        if (iRet != 0)
        {
            CertainLogError("iEntityID %lu GetMaxChosenEntry ret %d", iEntityID, iRet);
            return iRet;
        }
        iMaxTaskEntry = iMaxContChosenEntry;
    }

    vector<EntryValue_t> vecRec;

    for (uint64_t iEntry = iMaxCommitedEntry + 1; iEntry <= iMaxTaskEntry; ++iEntry)
    {
        string strWriteBatch;
        iRet = clsCertainWrapper::GetInstance()->GetWriteBatch(iEntityID, iEntry, strWriteBatch);
        if (iRet != 0)
        {
            if (iRet == eRetCodeNotFound && iEntry <= iMaxContChosenEntry)
            {
                CertainLogFatal("E(%lu, %lu) %lu m_poCertain->GetWriteBatch ret %d",
                        iEntityID, iEntry, iMaxContChosenEntry, iRet);
                return eRetCodeStorageCorrupt;
            }
            break;
        }

        EntryValue_t tKV;
        tKV.iEntityID = iEntityID;
        tKV.iEntry = iEntry;
        tKV.strValue = strWriteBatch;
        vecRec.push_back(tKV);

        if (iMaxCommitedEntry + 50 < iEntry)
        {
            CertainLogFatal("Check why DB lag behind much iEntityID %lu iMaxTaskEntry %lu",
                    iEntityID, iMaxTaskEntry);
            break;
        }
    }

    if (vecRec.size() == 0)
    {
        return 0;
    }

    OSS::ReportMultiCmd();
    OSS::ReportMultiCmdInner(vecRec.size());

    TIMERMS_START(iCommitUseTimeMS);
    iRet = clsCertainWrapper::GetInstance()->GetDBEngine()->MultiCommit(vecRec);
    TIMERMS_STOP(iCommitUseTimeMS);
    OSS::ReportDBCommit(iRet, iCommitUseTimeMS);
    if (iRet != 0)
    {
        CertainLogFatal("m_poDBEngine->MultiCommit iEntityID %lu ret %d", iEntityID, iRet);
        return iRet;
    }

    return 0;
}

void clsDBWorker::RunApplyTask(clsClientCmd *poCmd, uint64_t &iPLogGetCnt)
{
    int iRet;

    uint64_t iEntityID = poCmd->GetEntityID();
    uint64_t iEntry = poCmd->GetEntry();

    string strWriteBatch;
    uint32_t iExtraPLogGetCnt = 0;
    uint64_t iMaxCommitedEntry = 0;
    uint64_t iMaxContChosenEntry = 0;
    uint64_t iMaxChosenEntry = 0;

    uint64_t iMaxTaskEntry = poCmd->GetEntry();
    if (iMaxTaskEntry == 0)
    {
        // Apply up to the newest plog when receive a notify.
        iRet = clsEntityGroupMng::GetInstance()->GetMaxChosenEntry(iEntityID,
                iMaxContChosenEntry, iMaxChosenEntry);
        if (iRet != 0)
        {
            CertainLogError("cmd: %s GetMaxChosenEntry ret %d",
                    poCmd->GetTextCmd().c_str(), iRet);
            return;
        }
        iMaxTaskEntry = iMaxContChosenEntry;
    }

    bool bSleeping = false;
    uint32_t iDBCtrlPLogGetCnt = m_poConf->GetDBCtrlPLogGetCnt();
    uint32_t iDBCtrlSleepMS = m_poConf->GetDBCtrlSleepMS();

    bool bNeedEvictEntity = false;
    while (1)
    {
        if (bSleeping)
        {
            poll(NULL, 0, iDBCtrlSleepMS);
            bSleeping = false;
        }

        clsAutoEntityLock oEntityLock(iEntityID);

        uint32_t iFlag = 0;
        iMaxCommitedEntry = 0;
        iRet = m_poDBEngine->LoadMaxCommitedEntry(iEntityID, iMaxCommitedEntry, iFlag);
        if ((iRet != 0 && iRet != eRetCodeNotFound) || iFlag != 0)
        {
            if (iFlag == 0)
            {
                CertainLogFatal("LoadMaxCommitedEntry ret %d EntityID %lu", iRet, iEntityID);
            }
            else
            {
                CertainLogError("LoadMaxCommitedEntry ret %d EntityID %lu", iRet, iEntityID);
            }
            return;
        }

        CertainLogInfo("E(%lu, %lu) iMaxCommitedEntry %lu",
                iEntityID, iEntry, iMaxCommitedEntry);

        uint32_t iMaxMultiCmdSize = 0;
        uint32_t iMaxMultiCmdHourCnt = m_poConf->GetMaxMultiCmdHourCnt();

        if (iMaxMultiCmdHourCnt > 0 && m_poConf->GetMaxMultiCmdSizeForC() > 0)
        {
            uint32_t iLocalAcceptorID = INVALID_ACCEPTOR_ID;
            m_poCertain->GetCertainUser()->GetLocalAcceptorID(iEntityID, iLocalAcceptorID);

            if (iLocalAcceptorID == 2)
            {
                uint32_t iStartHour = m_poConf->GetMaxMultiCmdStartHour();
                uint32_t iEndHour = iStartHour + iMaxMultiCmdHourCnt - 1;
                uint32_t iCurrHour = GetCurrentHour();

                if ((iStartHour <= iCurrHour && iCurrHour <= iEndHour) || iCurrHour + 24 <= iEndHour)
                {
                    iMaxMultiCmdSize = m_poConf->GetMaxMultiCmdSizeForC();
                }
            }
        }

        if (iMaxMultiCmdSize > 0 && iMaxCommitedEntry < iMaxTaskEntry
                && (iMaxCommitedEntry + iMaxMultiCmdSize <= iMaxTaskEntry || poCmd->GetEntry() == 0))
        {
            CertainLogError("E(%lu, %lu) iMaxMultiCmdSize %u iMaxCommitedEntry %lu",
                    iEntityID, poCmd->GetEntry(), iMaxMultiCmdSize, iMaxCommitedEntry);

            iRet = clsCertainWrapper::GetInstance()->GetDBEngine()->MultiCommit(iEntityID, iMaxCommitedEntry, iMaxTaskEntry);
            if (iRet == eRetCodeStorageCorrupt)
            {
                bNeedEvictEntity = true;
            }
        }

        if (iMaxMultiCmdSize > 0)
        {
            break;
        }

        while (iMaxCommitedEntry < iMaxTaskEntry && !bSleeping)
        {
            iEntry = iMaxCommitedEntry + 1;

            if (poCmd->GetEntry() == iEntry)
            {
                iRet = 0;
                strWriteBatch = poCmd->GetWriteBatch();
            }
            else
            {
                iRet = m_poCertain->GetWriteBatch(iEntityID, iEntry, strWriteBatch);

                iPLogGetCnt++;
                iExtraPLogGetCnt++;

                if (iDBCtrlSleepMS > 0 && iDBCtrlPLogGetCnt > 0 && iPLogGetCnt % iDBCtrlPLogGetCnt == 0)
                {
                    bSleeping = true;
                }
            }
            if (iRet != 0)
            {
                if (iRet == eRetCodeNotFound && iEntry <= iMaxContChosenEntry)
                {
                    CertainLogFatal("E(%lu, %lu) %lu m_poCertain->GetWriteBatch ret %d",
                            iEntityID, iEntry, iMaxContChosenEntry, iRet);
                    bNeedEvictEntity = true;
                }
                break;
            }

            TIMERMS_START(iCommitUseTimeMS);
            iRet = m_poDBEngine->Commit(iEntityID, iEntry, strWriteBatch);
            TIMERMS_STOP(iCommitUseTimeMS);
            OSS::ReportDBCommit(iRet, iCommitUseTimeMS);
            if (iRet != 0)
            {
                CertainLogFatal("E(%lu, %lu) m_poDBEngine->Commit ret %d",
                        iEntityID, iEntry, iRet);
                break;
            }

            iMaxCommitedEntry++;
        }

        if (iMaxCommitedEntry >= iMaxTaskEntry)
        {
            CertainLogInfo("Check if E(%lu, %lu) commited outside Certain",
                    iEntityID, iEntry);
            break;
        }

        if (!bSleeping)
        {
            break;
        }
    }

    OSS::ReportExtraPLogGet(iExtraPLogGetCnt);

    if (bNeedEvictEntity)
    {
        m_poCertain->EvictEntity(iEntityID);
    }

    if (iExtraPLogGetCnt > m_poConf->GetMaxCatchUpNum())
    {
        CertainLogError("iEntityID %lu iExtraPLogGetCnt %u entrys: %lu %lu %lu %lu",
                iEntityID, iExtraPLogGetCnt, iMaxCommitedEntry, poCmd->GetEntry(),
                iMaxContChosenEntry, iMaxChosenEntry);
    }
    else
    {
        CertainLogInfo("iEntityID %lu iExtraPLogGetCnt %u entrys: %lu %lu %lu %lu",
                iEntityID, iExtraPLogGetCnt, iMaxCommitedEntry, poCmd->GetEntry(),
                iMaxContChosenEntry, iMaxChosenEntry);
    }
}

void *clsDBWorker::DBRoutine(void * arg)
{
    DBRoutine_t * pDBRoutine = (DBRoutine_t *)arg;
    //co_enable_hook_sys();

    SetRoutineID(pDBRoutine->iRoutineID);

    uint64_t iPLogGetCnt = 0;

    while(1)
    {
        clsDBWorker * pDBWorker = (clsDBWorker * )pDBRoutine->pSelf;

        if(!pDBRoutine->bHasJob)
        {
            AssertEqual(pDBRoutine->pData, NULL);
            pDBWorker->m_poCoWorkList->push(pDBRoutine);
            co_yield_ct();
            continue;
        }

        {
            AssertNotEqual(pDBRoutine->pData, NULL);
            clsClientCmd *poCmd = (clsClientCmd*) pDBRoutine->pData;

            pDBWorker->RunApplyTask(poCmd, iPLogGetCnt);

            pDBWorker->m_tBusyEntitySet.erase(poCmd->GetEntityID());

            delete poCmd, poCmd = NULL;
        }

        pDBRoutine->bHasJob = false;
        pDBRoutine->pData = NULL;
    }

    return NULL;
}

int clsDBWorker::CoEpollTick(void * arg)
{
    clsDBWorker * pDBWorker = (clsDBWorker * ) arg;

    if (pDBWorker->CheckIfExiting(0))
    {
        return -1;
    }

    return DBSingle(arg);
}

int clsDBWorker::DBSingle(void * arg)
{
    clsDBWorker * pDBWorker = (clsDBWorker*)arg;
    stack<DBRoutine_t *> & IdleCoList = *(pDBWorker->m_poCoWorkList);

    static __thread uint64_t iLoopCnt = 0;

    while( !IdleCoList.empty() )
    {
        clsClientCmd *poCmd = NULL;
        int iRet = pDBWorker->m_poDBReqQueue->TakeByOneThread(&poCmd);
        if(iRet == 0 && poCmd)
        {
            if (pDBWorker->m_tBusyEntitySet.find(poCmd->GetEntityID())
                    != pDBWorker->m_tBusyEntitySet.end())
            {
                // Some entity in db may not be newly because of dropping.
                CertainLogInfo("drop cmd: %s", poCmd->GetTextCmd().c_str());
                delete poCmd, poCmd = NULL;
                continue;
            }

            if( ( (++iLoopCnt) % 10000) == 0)
            {
                CertainLogError("DBWorkerID %u DBQueue size %u",
                        pDBWorker->m_iWorkerID, pDBWorker->m_poDBReqQueue->Size());
            }

            pDBWorker->m_tBusyEntitySet.insert(poCmd->GetEntityID());

            DBRoutine_t * w = IdleCoList.top();
            w->pData = (void*)poCmd;
            w->bHasJob = true;
            IdleCoList.pop();
            co_resume( (stCoRoutine_t*)(w->pCo) );
        }
        else
        {
            break;
        }
    }

    //clsCertainUserBase * pCertainUser = clsCertainWrapper::GetInstance()->GetCertainUser();
    //pCertainUser->HandleLockCallBack()();

    return 0;
}

int clsDBWorker::NotifyDBWorker(uint64_t iEntityID)
{
    static vector<uint64_t> vecWBUUID;
    static string strWriteBatch;

    clsWriteBatchCmd *poWB = new clsWriteBatchCmd(0, 0, vecWBUUID, strWriteBatch);

    poWB->SetEntityID(iEntityID);
    poWB->SetEntry(0);

    int iRet = EnterDBReqQueue(poWB);
    if (iRet != 0)
    {
        CertainLogError("iEntityID %lu EnterDBReqQueue ret %d",
                iEntityID, iRet);
        delete poWB, poWB = NULL;
        return -1;
    }

    return 0;
}

void clsDBWorker::Run()
{
    int cpu_cnt = GetCpuCount();

    if (cpu_cnt == 48)
    {
        SetCpu(8, cpu_cnt);
    }
    else
    {
        SetCpu(4, cpu_cnt);
    }

    uint32_t iLocalServerID = m_poConf->GetLocalServerID();
    SetThreadTitle("db_%u_%u", iLocalServerID, m_iWorkerID);
    CertainLogInfo("db_%u_%u run", iLocalServerID, m_iWorkerID);

    //co_enable_hook_sys();
    stCoEpoll_t * ev = co_get_epoll_ct();

    for (int i = 0; i < int(m_poConf->GetDBRoutineCnt()); ++i)
    {
        DBRoutine_t *w = (DBRoutine_t*)calloc( 1,sizeof(DBRoutine_t) );
        stCoRoutine_t *co = NULL;
        co_create( &co, NULL, DBRoutine, w );

        int iRoutineID = m_iStartRoutineID + i;
        w->pCo = (void*)co;
        w->pSelf = this;
        w->pData = NULL;
        w->bHasJob = false;
        w->iRoutineID = iRoutineID;
        co_resume( (stCoRoutine_t *)(w->pCo) );
        printf("DBWorker idx %d Routine idx %d\n", m_iWorkerID,  iRoutineID);
        CertainLogError("DBWorker idx %d Routine idx %d", m_iWorkerID,  iRoutineID);
    }

    co_eventloop( ev, CoEpollTick, this);
}

} // namespace Certain
