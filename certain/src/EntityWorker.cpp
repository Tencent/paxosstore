#include "EntityWorker.h"

namespace Certain
{

void clsEntityWorker::Run()
{
    // Bind cpu affinity here.
    int iRet;
    uint32_t iLocalServerID = m_poConf->GetLocalServerID();
    SetThreadTitle("entity_%u_%u", iLocalServerID, m_iWorkerID);
    CertainLogInfo("entity_%u_%u run", iLocalServerID, m_iWorkerID);

    clsSmartSleepCtrl oSleepCtrl(200, 1000);
    uint64_t iCleanUpCalledCnt = 0;

    while (1)
    {
        if (CheckIfExiting(1000))
        {
            printf("entity_%u_%u exit\n", iLocalServerID, m_iWorkerID);
            CertainLogInfo("entity_%u_%u exit", iLocalServerID, m_iWorkerID);
            break;
        }

        bool bHasWork = false;
        clsCmdBase *poCmd = NULL;

        // 1.Do with PLog response.
        // Resolve uncertain status AFSP.
        for (uint32_t i = 0; i < m_poConf->GetMaxCatchUpNum() + 1; ++i)
        {
            poCmd = NULL;
            iRet = m_poPLogRspQueue->TakeByOneThread(&poCmd);
            if (iRet == 0)
            {
                bHasWork = true;

                Assert(poCmd != NULL);
                iRet = DoWithPLogRsp(poCmd);
                if (iRet < 0)
                {
                    CertainLogError("DoWithPLogRsp ret %d cmd: %s",
                            iRet, poCmd->GetTextCmd().c_str());
                }

                if (iRet != eRetCodePtrReuse)
                {
                    delete poCmd, poCmd = NULL;
                }
            }
            else
            {
                break;
            }
        }

        // 2.Do with IO request.
        poCmd = NULL;
        iRet = m_poIOReqQueue->TakeByOneThread(&poCmd);
        if (iRet == 0)
        {
            Assert(poCmd != NULL);
            bHasWork = true;

            iRet = DoWithIOReq(poCmd);
            if (iRet < 0)
            {
                CertainLogError("DoWithIOReq ret %d cmd %s",
                        iRet, poCmd->GetTextCmd().c_str());
            }

            if (iRet != eRetCodePtrReuse)
            {
                delete poCmd, poCmd = NULL;
            }
        }

        // 3.Do with Timeout Event.
        for (uint32_t i = 0; i < m_poConf->GetMaxCatchUpNum() + 1; ++i)
        {
            EntryInfo_t *ptInfo = m_poEntryMng->TakeTimeout();
            if (ptInfo != NULL)
            {
                AssertEqual(DoWithTimeout(ptInfo), eRetCodePtrReuse);
                bHasWork = true;
            }
            else
            {
                break;
            }
        }

        // 4.Do with GetAll Rsp
        clsPaxosCmd *poPaxosCmd = NULL;
        iRet = m_poGetAllRspQueue->TakeByOneThread(&poPaxosCmd);
        if (iRet == 0)
        {
            bHasWork = true;

            iRet = DoWithGetAllRsp(poPaxosCmd);
            if (iRet < 0)
            {
                CertainLogError("DoWithGetAllRsp ret %d EntityID %lu MaxCommitPos %lu",
                        iRet, poPaxosCmd->GetEntityID(), poPaxosCmd->GetEntry());
            }

            AssertNotEqual(iRet, eRetCodePtrReuse);
            delete poPaxosCmd, poPaxosCmd = NULL;
        }

        // 5.Clean up entrys that probably unused in the future.
        iCleanUpCalledCnt++;
        if (CleanUpCommitedDeadEntry(iCleanUpCalledCnt % 10000 == 0))
        {
            bHasWork = true;
        }

        // 6.The list store cmds in case of IORspQueue full.
        if (!m_poWaitingGoList.empty())
        {
            clsClientCmd *poClientCmd = m_poWaitingGoList.front();
            m_poWaitingGoList.pop_front();

            CertainLogError("Do with cmd in waiting go list, cmd: %s",
                    poClientCmd->GetTextCmd().c_str());

            if (InvalidClientCmd(poClientCmd, poClientCmd->GetResult()))
            {
                bHasWork = true;
            }
        }

        // Try to eliminate entitys slowly if MaxMemEntityNum reduce.
        m_poEntityMng->CheckAndEliminate();

        ElimateEntryForRoom();

        if (!bHasWork)
        {
            oSleepCtrl.Sleep();
        }
        else
        {
            oSleepCtrl.Reset();
        }
    }
}

// For health check also.
bool clsEntityWorker::CleanUpCommitedDeadEntry(bool bPrintOldestEntry)
{
    uint32_t iDeadEntryCnt = 0;
    PLogPos_t tPos;
    EntryInfo_t *ptInfo = NULL;

    while (1)
    {
        ptInfo = NULL;
        if (!m_poEntryMng->PeekOldest(tPos, ptInfo))
        {
            break;
        }

        if (iDeadEntryCnt > m_poConf->GetMaxCatchUpNum())
        {
            break;
        }

        if (ptInfo->bUncertain || m_poEntryMng->WaitForTimeout(ptInfo))
        {
            break;
        }

        if (ptInfo->poMachine->GetEntryState() != kEntryStateChosen)
        {
            EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
            CertainLogError("iWorkerID %u E(%lu, %lu) st %d entrys: %lu %lu %lu",
                    m_iWorkerID, ptEntityInfo->iEntityID, ptInfo->iEntry,
                    ptInfo->poMachine->GetEntryState(),
                    ptEntityInfo->iMaxContChosenEntry,
                    ptEntityInfo->iMaxChosenEntry,
                    ptEntityInfo->iMaxPLogEntry);

            if (ptEntityInfo->iMaxContChosenEntry >= ptInfo->iEntry)
            {
                CertainLogError("CheckIf GetAll E(%lu, %lu)",
                        ptEntityInfo->iEntityID, ptInfo->iEntry);
            }

            ActivateEntry(ptInfo);
            ptInfo = NULL;
            break;
        }

        if (ptInfo->iEntry > ptInfo->ptEntityInfo->iMaxContChosenEntry)
        {
            EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
            CertainLogInfo("iWorkerID %u E(%lu, %lu) st %d entrys: %lu %lu %lu",
                    m_iWorkerID, ptEntityInfo->iEntityID, ptInfo->iEntry,
                    ptInfo->poMachine->GetEntryState(),
                    ptEntityInfo->iMaxContChosenEntry,
                    ptEntityInfo->iMaxChosenEntry,
                    ptEntityInfo->iMaxPLogEntry);
            CheckForCatchUp(ptInfo->ptEntityInfo, INVALID_ACCEPTOR_ID, 0);
            ptInfo = NULL;

            break;
        }

        iDeadEntryCnt++;
        CleanUpEntry(ptInfo);
    }

    if (ptInfo != NULL && bPrintOldestEntry)
    {
        CertainLogImpt("iWorkerID %u E(%lu, %lu) st %d uncertain %u wait %u",
                m_iWorkerID, ptInfo->ptEntityInfo->iEntityID, ptInfo->iEntry,
                ptInfo->poMachine->GetEntryState(), ptInfo->bUncertain,
                m_poEntryMng->WaitForTimeout(ptInfo));
    }

    if (iDeadEntryCnt > 0)
    {
        CertainLogError("iWorkerID %u iDeadEntryCnt %u size %u",
                m_iWorkerID, iDeadEntryCnt, m_poEntryMng->GetEntrySize());
        return true;
    }

    return false;
}

int clsEntityWorker::RecoverEntry(EntryInfo_t *ptInfo,
        const EntryRecord_t &tRecord)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    AssertNotEqual(ptEntityInfo, NULL);

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;
    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;

    // (TODO)rock: check mem limit here
    // May have mem more than limit if eliminate slow.
    int iRet = ptInfo->poMachine->Update(iEntityID, iEntry,
            iLocalAcceptorID, iLocalAcceptorID, tRecord);
    m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
    if (iRet < 0)
    {
        CertainLogFatal("E(%lu, %lu) state: %s",
                iEntityID, iEntry, ptInfo->poMachine->ToString().c_str());
        return -1;
    }

    int iEntryState = ptInfo->poMachine->GetEntryState();
    CertainLogInfo("E(%lu, %lu) iEntryState %d active: %s",
            iEntityID, iEntry, iEntryState,
            EntryRecordToString(tRecord).c_str());

    return iEntryState;
}

void clsEntityWorker::ElimateEntryForRoom()
{
    if (!m_poMemCacheCtrl->IsAlmostOverLoad())
    {
        return;
    }

    uint32_t iDelCount = 0;
    for ( ; iDelCount < 100; ++iDelCount)
    {
        // call with bIngoreMemOverLoad true.
        if (CheckIfEntryNumLimited(0, true, true))
        {
            break;
        }
    }

    CertainLogError("m_iWorkerID %u OverLoad %u iDelCount %u TotalSize %lu",
            m_iWorkerID, m_poMemCacheCtrl->IsOverLoad(), iDelCount,
            m_poMemCacheCtrl->GetTotalSize());
}

bool clsEntityWorker::CheckIfEntryNumLimited(uint64_t iSkippedEntityID,
        bool bEliminateMidState, bool bIngoreMemOverLoad)
{
    // Not accurate, may have one request redundant.
    if (!m_poEntryMng->IsOverLoad() && !m_poMemCacheCtrl->IsOverLoad(false))
    {
        return false;
    }

    PLogPos_t tPos = { 0 };
    EntryInfo_t *ptInfo = NULL;

    AssertEqual(m_poEntryMng->PeekOldest(tPos, ptInfo), true);

    for (uint32_t i = 0; i < m_poConf->GetMaxCatchUpNum(); ++i)
    {
        if (!ptInfo->bCatchUpFlag)
        {
            break;
        }

        m_poEntryMng->ReduceEliminatePriority(ptInfo);

        AssertEqual(m_poEntryMng->PeekOldest(tPos, ptInfo), true);
    }

    if (ptInfo->bUncertain || ptInfo->bCatchUpFlag)
    {
        m_poEntryMng->ReduceEliminatePriority(ptInfo);

        clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
        clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

        clsPLogReqQueue *poQueue = poQueueMng->GetPLogReqQueue(
                Hash(tPos.iEntityID) % poConf->GetPLogWorkerNum());

        CertainLogError("Check if busy E(%lu, %lu) (%u, %u) %u -> %u",
                tPos.iEntityID, tPos.iEntry, ptInfo->bUncertain, ptInfo->bCatchUpFlag,
                poQueue->Size(), m_poPLogRspQueue->Size());
        OSS::ReportElimFailed();
        return true;
    }
    else if (ptInfo->iEntry <= ptInfo->ptEntityInfo->iMaxContChosenEntry
            || (ptInfo->iEntry > ptInfo->ptEntityInfo->iMaxChosenEntry
                && ptInfo->poMachine->GetEntryState() == kEntryStateNormal))
    {
        // It's health.
        CertainLogInfo("remove health E(%lu, %lu) status %u for room",
                tPos.iEntityID, tPos.iEntry, ptInfo->poMachine->GetEntryState());
    }
    else if (iSkippedEntityID == tPos.iEntityID)
    {
        CertainLogError("skip unhealth E(%lu, %lu) status %u for room",
                tPos.iEntityID, tPos.iEntry, ptInfo->poMachine->GetEntryState());
        return true;
    }
    else if (ptInfo->poMachine->GetEntryState() == kEntryStateChosen)
    {
        CertainLogError("remove unhealth but chosen E(%lu, %lu) status %u for room",
                tPos.iEntityID, tPos.iEntry, ptInfo->poMachine->GetEntryState());
        OSS::ReportChosenElim();
    }
    else if (!bEliminateMidState)
    {
        CertainLogError("can't remove mid E(%lu, %lu) status %u for room",
                tPos.iEntityID, tPos.iEntry, ptInfo->poMachine->GetEntryState());
        OSS::ReportMidStateElimFailed();
    }
    else if (ptInfo->poMachine->GetEntryState() == kEntryStateNormal)
    {
        CertainLogError("remove empty E(%lu, %lu) status %u for room",
                tPos.iEntityID, tPos.iEntry, ptInfo->poMachine->GetEntryState());
        OSS::ReportEmptyElim();
    }
    else
    {
        // Mid state is eliminated.
        CertainLogError("remove unhealth E(%lu, %lu) status %u for room",
                tPos.iEntityID, tPos.iEntry, ptInfo->poMachine->GetEntryState());
        OSS::ReportMidStateElim();
    }

    CleanUpEntry(ptInfo);

    if (!bIngoreMemOverLoad && m_poMemCacheCtrl->IsOverLoad())
    {
        CertainLogError("m_iWorkerID %u TotalSize %lu",
                m_iWorkerID, m_poMemCacheCtrl->GetTotalSize());
        return true;
    }

    return false;
}

int clsEntityWorker::EnterGetAllRspQueue(clsPaxosCmd *poCmd)
{   
    uint64_t iEntityID = poCmd->GetEntityID();
    clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
    clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

    clsGetAllRspQueue *poQueue = poQueueMng->GetGetAllRspQueue(Hash(iEntityID)
            % poConf->GetEntityWorkerNum());

    int iRet = poQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        CertainLogError("PushByMultiThread ret %d cmd: %s",
                iRet, poCmd->GetTextCmd().c_str());
        return -1;
    }   

    return 0;
}

void clsEntityWorker::BroadcastToRemote(EntryInfo_t *ptInfo,
        clsEntryStateMachine *poMachine, clsClientCmd *poCmd)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    AssertNotEqual(ptEntityInfo, NULL);

    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;

    bool bAllDestSame = false;

    for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
    {
        if (i == iLocalAcceptorID)
        {
            continue;
        }

        clsPaxosCmd *po;
        if (poMachine == NULL)
        {
            po = new clsPaxosCmd(iLocalAcceptorID, iEntityID, iEntry);
        }
        else
        {
            const EntryRecord_t &tSrc = poMachine->GetRecord(
                    iLocalAcceptorID);
            const EntryRecord_t &tDest = poMachine->GetRecord(i);

            po = new clsPaxosCmd(iLocalAcceptorID, iEntityID, iEntry,
                    &tSrc, &tDest);
        }

        if (poCmd != NULL)
        {
            po->SetUUID(poCmd->GetUUID());
            if (poCmd->IsReadOnly())
            {
                po->SetQuickRsp(true);
            }
        }
        po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));

        if (bAllDestSame)
        {
            po->SetDestAcceptorID(INVALID_ACCEPTOR_ID);
            m_poIOWorkerRouter->GoAndDeleteIfFailed(po);
            break;
        }
        else
        {
            po->SetDestAcceptorID(i);
            m_poIOWorkerRouter->GoAndDeleteIfFailed(po);
        }
    }
}

int clsEntityWorker::ProposeNoop(EntityInfo_t *ptEntityInfo, EntryInfo_t *ptInfo)
{
    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;
    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;

    clsEntryStateMachine *poMachine = ptInfo->poMachine;

    uint32_t iProposalNum = poMachine->GetNextPreparedNum(iLocalAcceptorID);

    EntryRecord_t tTempRecord;
    InitEntryRecord(&tTempRecord);

    if (iProposalNum == iLocalAcceptorID + 1)
    {
        iProposalNum = poMachine->GetNextPreparedNum(iLocalAcceptorID);
    }

    tTempRecord.iPreparedNum = iProposalNum;
    tTempRecord.iPromisedNum = iProposalNum;

    if (m_poMemCacheCtrl->IsOverLoad())
    {
        CertainLogError("E(%lu, %lu) TotalSize %lu",
                iEntityID, iEntry, m_poMemCacheCtrl->GetTotalSize());
        return eRetCodeMemCacheLimited;
    }

    int iEntryState = poMachine->Update(iEntityID, iEntry, iLocalAcceptorID,
            iLocalAcceptorID, tTempRecord);
    m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
    if (iEntryState < 0)
    {
        CertainLogFatal("poMachine->Update ret %d state: %s",
                iEntryState, poMachine->ToString().c_str());
        return -1;
    }

    const EntryRecord_t &tUpdatedRecord = poMachine->GetRecord(
            iLocalAcceptorID);

    clsPaxosCmd *poPaxosCmd = new clsPaxosCmd(iLocalAcceptorID,
            iEntityID, iEntry, &tUpdatedRecord, NULL);
    poPaxosCmd->SetMaxPLogEntry((uint64_t)ptEntityInfo->iMaxPLogEntry);

    CertainLogError("record: %s state %d",
            EntryRecordToString(tUpdatedRecord).c_str(), iEntryState);

    ptInfo->bUncertain = true;
    ptInfo->bBroadcast = true;

    int iRet = clsPLogWorker::EnterPLogReqQueue(poPaxosCmd);
    if (iRet != 0)
    {
        CertainLogError("EnterPLogReqQueue ret %d", iRet);

        delete poPaxosCmd, poPaxosCmd = NULL;
        ptInfo->bUncertain = false;
        CleanUpEntry(ptInfo);
    }

    return 0;
}

int clsEntityWorker::DoWithClientCmd(clsClientCmd *poCmd)
{
    int iRet;
    uint64_t iEntityID = poCmd->GetEntityID();
    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    if (ptEntityInfo == NULL)
    {
        // CheckDBStatus may not refresh the entity LRU,
        // and the ptEntityInfo is at the tail of the entity LRU.
        CertainLogError("BUG maybe check it cmd: %s", poCmd->GetTextCmd().c_str());

        InvalidClientCmd(poCmd, eRetCodeEntityEvicted);
        return eRetCodePtrReuse;
    }

    if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY)
    {
        InvalidClientCmd(poCmd, eRetCodeRecovering);
        return eRetCodePtrReuse;
    }

    if(ptEntityInfo->bGetAllPending)
    {
        InvalidClientCmd(poCmd, eRetCodeGetAllPending);
        return eRetCodePtrReuse;
    }

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;

    if (iLocalAcceptorID == INVALID_ACCEPTOR_ID)
    {
        InvalidClientCmd(poCmd, eRetCodeRouteErr);
        return eRetCodePtrReuse;
    }

    if (ptEntityInfo->poClientCmd != NULL)
    {
        CertainLogFatal("BUG probably cmd: %s",
                ptEntityInfo->poClientCmd->GetTextCmd().c_str());
        InvalidClientCmd(poCmd, eRetCodeUnkown);
        return eRetCodePtrReuse;
    }

    if (poCmd->GetCmdID() == kWriteBatchCmd || poCmd->IsEvalOnly())
    {
        if (poCmd->GetEntry() != ptEntityInfo->iMaxChosenEntry + 1)
        {
            // Use suggested lease to avoid this error.
            CertainLogError("Check iEntityID %lu Entrys: %lu %lu cmd: %s",
                    iEntityID, ptEntityInfo->iMaxContChosenEntry,
                    ptEntityInfo->iMaxChosenEntry,
                    poCmd->GetTextCmd().c_str());
            InvalidClientCmd(poCmd, eRetCodeTurnErr);
            return eRetCodePtrReuse;
        }
    }

    AssertEqual(ptEntityInfo->poClientCmd, NULL);

    Assert(m_poEntityMng->Refresh(ptEntityInfo));

    uint64_t iEntry = ptEntityInfo->iMaxChosenEntry + 1;

    EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
    if (ptInfo == NULL)
    {
        if (CheckIfEntryNumLimited())
        {
            InvalidClientCmd(poCmd, eRetCodeEntryLimited);
            return eRetCodePtrReuse;
        }

        ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);
        m_poEntryMng->AddTimeout(ptInfo, m_poConf->GetCmdTimeoutMS());

        if (iEntry <= ptEntityInfo->iMaxPLogEntry)
        {
            if (LoadFromPLogWorker(ptInfo) != 0)
            {
                m_poEntryMng->DestroyEntryInfo(ptInfo);
            }

            CertainLogError("E(%lu, %lu) iMaxPLogEntry %lu ref %d Check if Busy",
                    iEntityID, iEntry, ptEntityInfo->iMaxPLogEntry, ptEntityInfo->iRefCount);

            InvalidClientCmd(poCmd, eRetCodeBusy);
            return eRetCodePtrReuse;
        }
    }

    if (ptInfo->bUncertain)
    {
        InvalidClientCmd(poCmd, eRetCodePLogPending);
        return eRetCodePtrReuse;
    }

    clsEntryStateMachine *poMachine = ptInfo->poMachine;
    int iEntryState = poMachine->GetEntryState();

    CertainLogInfo("E(%lu, %lu) state %d", iEntityID, iEntry, iEntryState);
    AssertNotEqual(iEntryState, kEntryStateChosen);

    AssertEqual(ptInfo->bRemoteUpdated, false);
    AssertEqual(ptInfo->bBroadcast, false);

    poCmd->SetEntry(iEntry);
    m_poEntryMng->AddTimeout(ptInfo, m_poConf->GetCmdTimeoutMS());

    if (m_poConf->GetPLogType() == kPLogTypeRes)
    {
        if (!poCmd->IsEvalOnly())
        {
            AssertEqual(poCmd->IsNeedRsp(), false);
            poCmd->SetEvalOnly(true);

            iRet = clsDBWorker::EnterDBReqQueue(poCmd);
            if (iRet != 0)
            {
                CertainLogError("PushByMultiThread ret %d", iRet);
                InvalidClientCmd(ptEntityInfo, eRetCodeQueueFailed);
            }

            return eRetCodePtrReuse;
        }

        poCmd->SetEvalOnly(false);
    }

    ptEntityInfo->poClientCmd = poCmd;

    if (poCmd->IsReadOnly())
    {
        if (poMachine->IsLocalEmpty())
        {
            poMachine->ResetAllCheckedEmpty();
            poMachine->SetCheckedEmpty(iLocalAcceptorID);
            BroadcastToRemote(ptInfo, NULL, poCmd);
            m_poEntryMng->AddTimeout(ptInfo, m_poConf->GetCmdTimeoutMS());
            OSS::ReportCheckEmpty();
            return eRetCodePtrReuse;
        }
        else
        {
            OSS::ReportPaxosForRead();
        }
    }
    else
    {
        OSS::ReportPaxosForWrite();
    }

    if (m_poEntityMng->IsWaitLeaseTimeout(ptEntityInfo))
    {
        if (ptEntityInfo->iLocalAcceptorID != 0)
        {
            CertainLogError("Check if master busy E(%lu, %lu)",
                    iEntityID, iEntry);
        }
        ptEntityInfo->poClientCmd = NULL;
        m_poEntryMng->AddTimeout(ptInfo, 10000);
        InvalidClientCmd(poCmd, eRetCodeLeaseReject);
        OSS::ReportLeaseReject();
        return eRetCodePtrReuse;
    }

    uint32_t iProposalNum = poMachine->GetNextPreparedNum(iLocalAcceptorID);

    EntryRecord_t tTempRecord;
    InitEntryRecord(&tTempRecord);

    bool bSetValue = false;
    // PreAuth to a empty entry.
    if (m_poConf->GetEnablePreAuth() && iProposalNum == iLocalAcceptorID + 1
            && iEntry == ptEntityInfo->iLocalPreAuthEntry)
    {
        if (!poMachine->IsLocalEmpty())
        {
            CertainLogFatal("BUG probably Check E(%lu, %lu)",
                    iEntityID, iEntry);
        }
        if (m_poConf->GetLocalAcceptFirst())
        {
            tTempRecord.iAcceptedNum = iProposalNum;
        }
        bSetValue = true;
    }
    else if (iProposalNum == iLocalAcceptorID + 1)
    {
        iProposalNum = poMachine->GetNextPreparedNum(iLocalAcceptorID);
    }

    tTempRecord.iPreparedNum = iProposalNum;
    tTempRecord.iPromisedNum = iProposalNum;

    poCmd->SetWriteBatchID(clsEntityInfoMng::GenerateValueID(
                ptEntityInfo, tTempRecord.iPreparedNum));

    if (bSetValue)
    {
        tTempRecord.tValue = PaxosValue_t(poCmd->GetWriteBatchID(),
                poCmd->GetWBUUID(), true, poCmd->GetWriteBatch());
    }

    if (m_poMemCacheCtrl->IsOverLoad())
    {
        CertainLogError("E(%lu, %lu) TotalSize %lu",
                iEntityID, iEntry, m_poMemCacheCtrl->GetTotalSize());
        InvalidClientCmd(ptEntityInfo, eRetCodeMemCacheLimited);
        return eRetCodePtrReuse;
    }

    iEntryState = poMachine->Update(iEntityID, iEntry, iLocalAcceptorID,
            iLocalAcceptorID, tTempRecord);
    m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
    if (iEntryState < 0)
    {
        CertainLogFatal("poMachine->Update ret %d cmd: %s state: %s",
                iEntryState, poCmd->GetTextCmd().c_str(), poMachine->ToString().c_str());
        InvalidClientCmd(ptEntityInfo, eRetCodeUnkown);
        return eRetCodePtrReuse;
    }

    const EntryRecord_t &tUpdatedRecord = poMachine->GetRecord(
            iLocalAcceptorID);

    clsPaxosCmd *poPaxosCmd = new clsPaxosCmd(iLocalAcceptorID,
            iEntityID, iEntry, &tUpdatedRecord, NULL);
    poPaxosCmd->SetUUID(poCmd->GetUUID());
    poPaxosCmd->SetMaxPLogEntry((uint64_t)ptEntityInfo->iMaxPLogEntry);

    CertainLogDebug("record: %s state %d",
            EntryRecordToString(tUpdatedRecord).c_str(), iEntryState);

    // It's uncertain even if fail to push to PLogReqQueue.
    // It will retry when timeout in DoWithTimeout.
    ptInfo->bUncertain = true;
    ptInfo->bBroadcast = true;

    iRet = clsPLogWorker::EnterPLogReqQueue(poPaxosCmd);
    if (iRet != 0)
    {
        CertainLogError("EnterPLogReqQueue ret %d", iRet);

        delete poPaxosCmd, poPaxosCmd = NULL;
        ptInfo->bUncertain = false;
        CleanUpEntry(ptInfo);
    }

    return eRetCodePtrReuse;
}

int clsEntityWorker::LoadFromPLogWorker(EntryInfo_t *ptInfo)
{
    AssertEqual(ptInfo->bUncertain, false);
    ptInfo->bUncertain = true;

    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    CertainLogError("E(%lu, %lu) entrys: %lu %lu %lu %lu loading %u",
            ptEntityInfo->iEntityID, ptInfo->iEntry,
            ptEntityInfo->iMaxContChosenEntry, ptEntityInfo->iCatchUpEntry,
            ptEntityInfo->iMaxChosenEntry, ptEntityInfo->iMaxPLogEntry,
            ptEntityInfo->bRangeLoading);

    clsPaxosCmd *poPaxosCmd = new clsPaxosCmd(
            0, ptInfo->ptEntityInfo->iEntityID, ptInfo->iEntry);

    poPaxosCmd->SetPLogLoad(true);

    int iRet = clsPLogWorker::EnterPLogReqQueue(poPaxosCmd);
    if (iRet != 0)
    {
        CertainLogError("EnterPLogReqQueue ret %d", iRet);

        delete poPaxosCmd, poPaxosCmd = NULL;
        ptInfo->bUncertain = false;

        return -1;
    }

    if (!m_poEntryMng->WaitForTimeout(ptInfo))
    {
        m_poEntryMng->AddTimeout(ptInfo, 30000);
    }

    return 0;
}

int clsEntityWorker::RecoverFromPLogWorker(clsPaxosCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    uint64_t iEntry = poCmd->GetEntry();

    CertainLogInfo("cmd: %s", poCmd->GetTextCmd().c_str());

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    AssertNotEqual(ptEntityInfo, NULL);

    EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
    AssertNotEqual(ptInfo, NULL);

    AssertEqual(ptInfo->bUncertain, true);
    ptInfo->bUncertain = false;

    AssertEqual(poCmd->IsPLogError(), false);

    CertainLogError("E(%lu, %lu) entrys: %lu %lu %lu %lu loading %u",
            ptEntityInfo->iEntityID, ptInfo->iEntry,
            ptEntityInfo->iMaxContChosenEntry, ptEntityInfo->iCatchUpEntry,
            ptEntityInfo->iMaxChosenEntry, ptEntityInfo->iMaxPLogEntry,
            ptEntityInfo->bRangeLoading);

    if (poCmd->GetResult() == eRetCodeNotFound && ptEntityInfo->iMaxContChosenEntry >= iEntry)
    {
        ptInfo->bNotFound = true;
    }

    // (TODO)rock: not need recover when not found
    const EntryRecord_t &tSrcRecord = poCmd->GetSrcRecord();
    AssertNotMore(0, RecoverEntry(ptInfo, tSrcRecord));
    UpdateMaxChosenEntry(ptEntityInfo, ptInfo);

    AssertEqual(poCmd->IsCheckHasMore(), false);

    uint32_t iWaitingMsgPtrSize = sizeof(clsPaxosCmd*) * m_iAcceptorNum;
    clsPaxosCmd **apWaitingMsg = (clsPaxosCmd**)malloc(iWaitingMsgPtrSize);
    std::unique_ptr<char> oAutoFreeWaitingMsgPtr((char*)apWaitingMsg);

    memcpy(apWaitingMsg, ptInfo->apWaitingMsg, iWaitingMsgPtrSize);
    memset(ptInfo->apWaitingMsg, 0, iWaitingMsgPtrSize);

    int iRet = DoWithWaitingMsg(apWaitingMsg, m_iAcceptorNum);
    if (iRet != 0)
    {
        CertainLogError("DoWithWaitingMsg ret %d", iRet);
    }
    else 
    {
        m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
    }

    // ptInfo may eliminated in DoWithWaitingMsg
    ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);

    if (ptInfo != NULL && !ptInfo->bUncertain
            && ptInfo->poMachine->GetEntryState() != kEntryStateChosen)
    {
        m_poEntryMng->RemoveTimeout(ptInfo);
        ActivateEntry(ptInfo);
    }

    CheckForCatchUp(ptEntityInfo, INVALID_ACCEPTOR_ID, 0);

    return 0;
}

int clsEntityWorker::RangeLoadFromPLog(EntityInfo_t *ptEntityInfo)
{
    if (CheckIfEntryNumLimited(ptEntityInfo->iEntityID))
    {
        CertainLogError("iEntityID %lu stop loading by entry limit",
                ptEntityInfo->iEntityID);
        return 0;
    }

    if (ptEntityInfo->iMaxContChosenEntry < ptEntityInfo->iMaxChosenEntry)
    {
        if (m_poEntryMng->CheckIfCatchUpLimited(NULL))
        {
            CertainLogError("iEntityID %lu stop loading by catchup limit",
                    ptEntityInfo->iEntityID);
            return 0;
        }
    }

    AssertEqual(ptEntityInfo->bRangeLoading, false);
    ptEntityInfo->bRangeLoading = true;

    clsRecoverCmd *poCmd = new clsRecoverCmd(ptEntityInfo->iEntityID,
            ptEntityInfo->iMaxContChosenEntry);

    poCmd->SetRangeLoaded(ptEntityInfo->bRangeLoaded);
    poCmd->SetMaxPLogEntry((uint64_t)ptEntityInfo->iMaxPLogEntry);
    poCmd->SetMaxNum(m_poConf->GetMaxCatchUpNum());

    CertainLogInfo("cmd: %s", poCmd->GetTextCmd().c_str());

    int iRet = clsPLogWorker::EnterPLogReqQueue(poCmd);
    if (iRet != 0)
    {
        ptEntityInfo->bRangeLoading = false;
        delete poCmd, poCmd = NULL;

        CertainLogError("iEntityID %lu EnterPLogReqQueue ret %d",
                ptEntityInfo->iEntityID, iRet);
        return -2;
    }

    return 0;
}

void clsEntityWorker::UpdateMaxChosenEntry(EntityInfo_t *ptEntityInfo,
        EntryInfo_t *ptInfo)
{
    if (ptInfo->poMachine->GetEntryState() == kEntryStateChosen)
    {
        if (ptEntityInfo->iMaxChosenEntry < ptInfo->iEntry)
        {
            ptEntityInfo->iMaxChosenEntry = ptInfo->iEntry;
        }
    }
    else if (ptEntityInfo->iMaxChosenEntry < ptInfo->iEntry - 1)
    {
        ptEntityInfo->iMaxChosenEntry = ptInfo->iEntry - 1;
    }
}

int clsEntityWorker::RangeRecoverFromPLog(clsRecoverCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    uint64_t iEntry = 0;

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    AssertNotEqual(ptEntityInfo, NULL);

    AssertEqual(ptEntityInfo->bRangeLoading, true);
    ptEntityInfo->bRangeLoading = false;

    if (poCmd->GetResult() != eRetCodeOK)
    {
        if (ptEntityInfo->poClientCmd != NULL
                && ptEntityInfo->poClientCmd->GetCmdID() == kRecoverCmd)
        {
            InvalidClientCmd(ptEntityInfo, poCmd->GetResult());
        }

        if (!ptEntityInfo->bGetAllPending && poCmd->IsCheckGetAll()
                && GetCurrTimeMS() >= ptEntityInfo->iGetAllFinishTimeMS + 1000)
        {
            clsPaxosCmd *po = new clsPaxosCmd(GetPeerAcceptorID(ptEntityInfo), iEntityID, 0, NULL, NULL);

            int iRet = clsGetAllWorker::EnterReqQueue(po);
            if(iRet != 0)
            {
                delete po, po = NULL;
            }

            CertainLogError("iEntityID %lu triggle getall by peer ret %d", iEntityID, iRet);

            ptEntityInfo->bGetAllPending = true;
        }
        return 0;
    }

    typedef vector< pair<uint64_t, EntryRecord_t > > EntryRecordList_t;
    const EntryRecordList_t &tEntryRecordList = poCmd->GetEntryRecordList();
    AssertNotMore(tEntryRecordList.size(), m_poConf->GetMaxCatchUpNum());

    // Update iMaxChosenEntry
    if (ptEntityInfo->iMaxChosenEntry < poCmd->GetMaxCommitedEntry())
    {
        ptEntityInfo->iMaxChosenEntry = poCmd->GetMaxCommitedEntry();
    }
    if (poCmd->IsHasMore())
    {
        if (ptEntityInfo->iMaxChosenEntry < poCmd->GetMaxLoadingEntry())
        {
            ptEntityInfo->iMaxChosenEntry = poCmd->GetMaxLoadingEntry();
        }
    }
    if (tEntryRecordList.size() > 0)
    {
        uint64_t iEntry = tEntryRecordList.rbegin()->first;
        if (tEntryRecordList.rbegin()->second.bChosen)
        {
            if (ptEntityInfo->iMaxChosenEntry < iEntry)
            {
                ptEntityInfo->iMaxChosenEntry = iEntry;
            }
        }
        else
        {
            if (ptEntityInfo->iMaxChosenEntry < iEntry - 1)
            {
                ptEntityInfo->iMaxChosenEntry = iEntry - 1;
            }
        }
    }

    // Update iMaxContChosenEntry, iCatchUpEntry
    AssertNotMore(ptEntityInfo->iMaxContChosenEntry,
            ptEntityInfo->iCatchUpEntry);
    if (ptEntityInfo->iMaxContChosenEntry < poCmd->GetMaxCommitedEntry())
    {
        ptEntityInfo->iMaxContChosenEntry = poCmd->GetMaxCommitedEntry();
    }
    if (ptEntityInfo->iCatchUpEntry < ptEntityInfo->iMaxContChosenEntry)
    {
        ptEntityInfo->iCatchUpEntry = ptEntityInfo->iMaxContChosenEntry;
    }
    if (ptEntityInfo->iNotifyedEntry < poCmd->GetMaxCommitedEntry())
    {
        ptEntityInfo->iNotifyedEntry = poCmd->GetMaxCommitedEntry();
    }

    for (uint32_t i = 0; i < tEntryRecordList.size(); ++i)
    {
        iEntry = tEntryRecordList[i].first;
        AssertLess(poCmd->GetMaxCommitedEntry(), iEntry);
        AssertNotMore(iEntry, poCmd->GetMaxLoadingEntry());

        EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
        if (ptInfo == NULL)
        {
            const EntryRecord_t &tRecord = tEntryRecordList[i].second;

            if (!(tRecord.bChosen && ptEntityInfo->iMaxContChosenEntry + 1 == iEntry))
            {
                if (CheckIfEntryNumLimited(iEntityID))
                {
                    CertainLogError("E(%lu, %lu) catchup limited by entry", iEntityID, iEntry);
                    continue;
                }
            }

            ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);

            AssertNotMore(0, RecoverEntry(ptInfo, tRecord));
            UpdateMaxChosenEntry(ptEntityInfo, ptInfo);
        }
        else
        {
            CertainLogError("E(%lu, %lu) st %d maybe loaded by plog",
                    iEntityID, iEntry, ptInfo->poMachine->GetEntryState());
        }

        // Speed up applying DB.
        if (!ptInfo->bUncertain
                && ptInfo->poMachine->GetEntryState() == kEntryStateChosen)
        {
            if (ptEntityInfo->iMaxContChosenEntry + 1 == iEntry)
            {
                PushCmdToDBWorker(ptInfo);

                ptEntityInfo->iMaxContChosenEntry++;

                if (ptEntityInfo->iCatchUpEntry
                        < ptEntityInfo->iMaxContChosenEntry)
                {
                    AssertEqual(ptEntityInfo->iCatchUpEntry + 1,
                            ptEntityInfo->iMaxContChosenEntry);
                    ptEntityInfo->iCatchUpEntry++;
                }
                AssertNotMore(ptEntityInfo->iCatchUpEntry,
                        ptEntityInfo->iMaxChosenEntry);
            }
        }
    }

    if (!poCmd->IsHasMore())
    {
        // Update iMaxPLogEntry
        if (tEntryRecordList.size() > 0)
        {
            ptEntityInfo->iMaxPLogEntry = tEntryRecordList.rbegin()->first;
        }
        else
        {
            ptEntityInfo->iMaxPLogEntry = poCmd->GetMaxCommitedEntry();
        }
    }
    else if (!ptEntityInfo->bRangeLoaded && poCmd->GetMaxPLogEntry() != INVALID_ENTRY)
    {
        CertainLogImpt("iEntityID %lu iMaxPLogEntry %lu -> %lu",
                iEntityID, ptEntityInfo->iMaxPLogEntry, poCmd->GetMaxPLogEntry());
        Assert(!poCmd->IsRangeLoaded());

        if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY
                || ptEntityInfo->iMaxPLogEntry < poCmd->GetMaxPLogEntry())
        {
            ptEntityInfo->iMaxPLogEntry = poCmd->GetMaxPLogEntry();
        }
        else
        {
            CertainLogFatal("iEntityID %lu iMaxPLogEntry %lu -> %lu",
                    iEntityID, ptEntityInfo->iMaxPLogEntry, poCmd->GetMaxPLogEntry());
        }
    }

    ptEntityInfo->bRangeLoaded = true;

    uint32_t iEmptyEntryCnt = 0;
    for (uint64_t iEntry = ptEntityInfo->iMaxContChosenEntry + 1;
            iEntry <= poCmd->GetMaxLoadingEntry(); ++iEntry)
    {
        if (ptEntityInfo->iMaxPLogEntry != INVALID_ENTRY
                && iEntry > ptEntityInfo->iMaxPLogEntry)
        {
            break;
        }

        if (CheckIfEntryNumLimited(iEntityID, false))
        {
            CertainLogError("E(%lu, %lu) catchup limited by entry", iEntityID, iEntry);
            break;
        }

        EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
        if (ptInfo == NULL)
        {
            // (TODO)rock: remove this recover
            EntryRecord_t tRecord, tEmptyRecord;
            InitEntryRecord(&tEmptyRecord);
            ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);
            AssertNotMore(0, RecoverEntry(ptInfo, tEmptyRecord));
            iEmptyEntryCnt++;
        }
    }

    CertainLogInfo("iEntityID %lu entrys: %lu %lu %lu iEmptyEntryCnt %u",
            iEntityID, poCmd->GetMaxCommitedEntry(),
            poCmd->GetMaxLoadingEntry(), ptEntityInfo->iMaxPLogEntry,
            iEmptyEntryCnt);

    uint32_t iWaitingMsgPtrSize = sizeof(clsPaxosCmd*) * m_iAcceptorNum;
    clsPaxosCmd** apWaitingMsg = (clsPaxosCmd**)malloc(iWaitingMsgPtrSize);
    std::unique_ptr<char> oAutoFreeWaitingMsgPtr((char*)apWaitingMsg);

    memcpy(apWaitingMsg, ptEntityInfo->apWaitingMsg, iWaitingMsgPtrSize);
    memset(ptEntityInfo->apWaitingMsg, 0, iWaitingMsgPtrSize);

    int iRet = DoWithWaitingMsg(apWaitingMsg, m_iAcceptorNum);
    m_poMemCacheCtrl->UpdateTotalSize(ptEntityInfo);
    if (iRet != 0)
    {
        CertainLogError("DoWithWaitingMsg ret %d", iRet);
    }

    CheckForCatchUp(ptEntityInfo, INVALID_ACCEPTOR_ID, 0);

    if (ptEntityInfo->poClientCmd != NULL
            && ptEntityInfo->poClientCmd->GetCmdID() == kRecoverCmd)
    {
        clsRecoverCmd *po = dynamic_cast<clsRecoverCmd *>(
                ptEntityInfo->poClientCmd);

        po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));

        if (ptEntityInfo->iMaxContChosenEntry < ptEntityInfo->iMaxChosenEntry)
        {
            InvalidClientCmd(ptEntityInfo, eRetCodeCatchUp);
        }
        else
        {
            InvalidClientCmd(ptEntityInfo, eRetCodeOK);
        }
    }

    return 0;
}

int clsEntityWorker::LimitedCatchUp(EntityInfo_t *ptEntityInfo,
        uint32_t iDestAcceptorID)
{
    AssertNotMore(ptEntityInfo->iMaxContChosenEntry,
            ptEntityInfo->iCatchUpEntry);
    AssertNotMore(ptEntityInfo->iCatchUpEntry, ptEntityInfo->iMaxChosenEntry);

    // Used to make LimitedCatchUp amortized complexity O(1).
    bool bExtend = false;

    uint64_t iEntityID = ptEntityInfo->iEntityID;

    if (ptEntityInfo->iMaxContChosenEntry == ptEntityInfo->iCatchUpEntry)
    {
        uint64_t iOldCatchUpEntry = ptEntityInfo->iCatchUpEntry;
        CertainLogInfo("extend iEntityID %lu entrys: %lu %lu %lu %lu",
                iEntityID, ptEntityInfo->iMaxContChosenEntry,
                ptEntityInfo->iCatchUpEntry, ptEntityInfo->iMaxChosenEntry,
                ptEntityInfo->iMaxPLogEntry);
        ptEntityInfo->iCatchUpEntry = min(
                ptEntityInfo->iCatchUpEntry + m_poConf->GetMaxCatchUpNum(),
                uint64_t(ptEntityInfo->iMaxChosenEntry));
        bExtend = true;

        OSS::ReportBatchCatchUp(ptEntityInfo->iCatchUpEntry - iOldCatchUpEntry);
    }

    if (iDestAcceptorID != INVALID_ACCEPTOR_ID)
    {
        ptEntityInfo->iActiveAcceptorID = iDestAcceptorID;
    }

    int iReadyCnt = 0, iCatchUpCnt = 0;

    for (uint64_t iEntry = ptEntityInfo->iMaxContChosenEntry + 1;
            iEntry <= ptEntityInfo->iCatchUpEntry; ++iEntry)
    {
        AssertEqual(ptEntityInfo->bRangeLoading, false);

        EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
        if (ptInfo == NULL)
        {
            if (bExtend)
            {
                RangeLoadFromPLog(ptEntityInfo);
                break;
            }

            if (CheckIfEntryNumLimited(iEntityID, false))
            {
                CertainLogError("E(%lu, %lu) catchup limited by entry", iEntityID, iEntry);
                break;
            }

            // It means the entry may be eliminated from LRU.
            if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY
                    || iEntry <= ptEntityInfo->iMaxPLogEntry)
            {
                ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);
                if (LoadFromPLogWorker(ptInfo) != 0)
                {
                    m_poEntryMng->DestroyEntryInfo(ptInfo);
                    break;
                }
            }
            else
            {
                ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);
            }

            CertainLogError("Check it E(%lu, %lu)", iEntityID, iEntry);
        }

        if (!ptInfo->bUncertain
                && ptInfo->poMachine->GetEntryState() == kEntryStateChosen)
        {
            if (ptEntityInfo->iMaxContChosenEntry + 1 == iEntry)
            {
                PushCmdToDBWorker(ptInfo);
                ptEntityInfo->iMaxContChosenEntry++;
                iCatchUpCnt++;
            }
            continue;
        }

        if (ptInfo->iActiveAcceptorID == INVALID_ACCEPTOR_ID)
        {
            ptInfo->iActiveAcceptorID = ptEntityInfo->iActiveAcceptorID;
        }

        if (!ptInfo->bUncertain && !m_poEntryMng->WaitForTimeout(ptInfo))
        {
            if (!ActivateEntry(ptInfo))
            {
                break;
            }
            iReadyCnt++;
        }

        if (!bExtend)
        {
            break;
        }
    }

    if (iCatchUpCnt > 1 || iReadyCnt > 0)
    {
        CertainLogInfo("iEntityID %lu Entrys: %lu <= %lu <= %lu "
                "iCatchUpCnt %d iReadyCnt %d",
                ptEntityInfo->iEntityID, ptEntityInfo->iMaxContChosenEntry,
                ptEntityInfo->iCatchUpEntry, ptEntityInfo->iMaxChosenEntry,
                iCatchUpCnt, iReadyCnt);
    }

    return iCatchUpCnt;
}

// CheckForCatchUp may elimate current EntryInfo_t.
// So it must be called in the end of use of EntryInfo_t.
void clsEntityWorker::CheckForCatchUp(EntityInfo_t *ptEntityInfo,
        uint32_t iDestAcceptorID, uint64_t iDestMaxChosenEntry)
{
    uint64_t iEntityID = ptEntityInfo->iEntityID;

    if (ptEntityInfo->iMaxChosenEntry < iDestMaxChosenEntry)
    {
        CertainLogInfo("iMaxChosenEntry %lu iDestMaxChosenEntry %lu",
                ptEntityInfo->iMaxChosenEntry, iDestMaxChosenEntry);

        ptEntityInfo->iMaxChosenEntry = iDestMaxChosenEntry;
    }

    // Pre open for some empty entry.
    if (ptEntityInfo->iMaxPLogEntry != INVALID_ENTRY
            && ptEntityInfo->iMaxPLogEntry < ptEntityInfo->iMaxChosenEntry)
    {
        uint32_t iPreOpenCnt = 0;
        for (uint32_t i = 1; i <= m_poConf->GetMaxCatchUpNum(); ++i)
        {
            uint64_t iEntry = ptEntityInfo->iMaxPLogEntry + i;
            if (iEntry > ptEntityInfo->iMaxChosenEntry)
            {
                break;
            }

            if (CheckIfEntryNumLimited(iEntityID, false))
            {
                break;
            }

            EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
            if (ptInfo == NULL)
            {
                ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);
                iPreOpenCnt++;
            }
            else
            {
                break;
            }
        }
        if (iPreOpenCnt > 0)
        {
            CertainLogError("E(%lu, %lu) iPreOpenCnt %u",
                    iEntityID, ptEntityInfo->iMaxChosenEntry, iPreOpenCnt);
        }
    }

    if (ptEntityInfo->bRangeLoading)
    {
        CertainLogInfo("iEntityID %lu entrys: %lu %lu %lu",
                iEntityID, ptEntityInfo->iMaxContChosenEntry,
                ptEntityInfo->iCatchUpEntry, ptEntityInfo->iMaxChosenEntry);
        return;
    }

    if(ptEntityInfo->bGetAllPending)
    {
        CertainLogError("EntityID %lu GetAllPending, not need catch up", iEntityID);
        return;
    }

    // RangeLoadFromPLog --> iMaxPLogEntry == INVALID_ENTRY
    // LimitedCatchUp --> iMaxContChosenEntry < iMaxChosenEntry
    int iLoopCnt = 0, iCatchUpCnt = 0;
    while (ptEntityInfo->iMaxContChosenEntry < ptEntityInfo->iMaxChosenEntry)
    {
        int iRet = LimitedCatchUp(ptEntityInfo, iDestAcceptorID);
        if (iRet < 0)
        {
            CertainLogError("LimitedCatchUp ret %d", iRet);
            break;
        }

        iLoopCnt++;
        iCatchUpCnt += iRet;

        if (ptEntityInfo->bRangeLoading)
        {
            break;
        }

        if (ptEntityInfo->iMaxContChosenEntry < ptEntityInfo->iCatchUpEntry)
        {
            break;
        }

        if (iRet == 0)
        {
            break;
        }
    }

    // One remain group and one newly extend group is processed at most.
    // But it is possible when db lags badly, and entry increases quickly.
    if (iLoopCnt >= 3)
    {
        CertainLogError("Check if db lags badly Cnt(%d, %d) E(%lu, %lu)",
                iLoopCnt, iCatchUpCnt, iEntityID,
                ptEntityInfo->iMaxChosenEntry);
    }

    if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY
            && ptEntityInfo->iMaxContChosenEntry == ptEntityInfo->iMaxChosenEntry
            && !ptEntityInfo->bRangeLoading)
    {
        RangeLoadFromPLog(ptEntityInfo);
    }
}

int clsEntityWorker::UpdateRecord(clsPaxosCmd *poPaxosCmd)
{
    uint32_t iAcceptorID = poPaxosCmd->GetSrcAcceptorID();
    uint64_t iEntityID = poPaxosCmd->GetEntityID();
    uint64_t iEntry = poPaxosCmd->GetEntry();

    CertainLogDebug("iAcceptorID %u E(%lu, %lu)",
            iAcceptorID, iEntityID, iEntry);

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    AssertNotEqual(ptEntityInfo, NULL);

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;

    EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
    AssertNotEqual(ptInfo, NULL);

    clsEntryStateMachine *poMachine = ptInfo->poMachine;
    const EntryRecord_t tOldRecord = poMachine->GetRecord(iLocalAcceptorID);

    CertainLogDebug("before update cmd: %s state %u",
            poPaxosCmd->GetTextCmd().c_str(), poMachine->GetEntryState());

    const EntryRecord_t &tSrcRecord = poPaxosCmd->GetSrcRecord();
    const EntryRecord_t &tDestRecord = poPaxosCmd->GetDestRecord();

    if (m_poMemCacheCtrl->IsOverLoad())
    {
        CertainLogError("E(%lu, %lu) TotalSize %lu",
                iEntityID, iEntry, m_poMemCacheCtrl->GetTotalSize());
        return eRetCodeMemCacheLimited;
    }

    int iRet = poMachine->Update(iEntityID, iEntry, iLocalAcceptorID,
            iAcceptorID, tSrcRecord);
    m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
    if (iRet < 0)
    {
        CertainLogFatal("poMachine->Update ret %d cmd: %s state: %s",
                iRet, poPaxosCmd->GetTextCmd().c_str(), poMachine->ToString().c_str());
        return -1;
    }

    CertainLogDebug("after  update cmd: %s state %u",
            poPaxosCmd->GetTextCmd().c_str(), poMachine->GetEntryState());

    if (poMachine->GetEntryState() == kEntryStateMajorityPromise)
    {
        PaxosValue_t tValue;

        if (ptEntityInfo->poClientCmd != NULL
                && ptEntityInfo->poClientCmd->GetEntry() == iEntry)
        {
            tValue.bHasValue = true;
            tValue.iValueID = ptEntityInfo->poClientCmd->GetWriteBatchID();
            tValue.strValue = ptEntityInfo->poClientCmd->GetWriteBatch();
            tValue.vecValueUUID = ptEntityInfo->poClientCmd->GetWBUUID();
        }
        else
        {
            tValue.bHasValue = true;
            tValue.iValueID = clsEntityInfoMng::GenerateValueID(ptEntityInfo,
                    poMachine->GetRecord(iLocalAcceptorID).iPreparedNum);
        }

        bool bAcceptPreparedValue = false;
        iRet = poMachine->AcceptOnMajorityPromise(iLocalAcceptorID, tValue,
                bAcceptPreparedValue);
        if (iRet != 0)
        {
            CertainLogFatal("state: %s", poMachine->ToString().c_str());
            return -2;
        }

        if (!bAcceptPreparedValue && ptEntityInfo->poClientCmd != NULL
                && ptEntityInfo->poClientCmd->GetEntry() == iEntry)
        {
            InvalidClientCmd(ptEntityInfo, eRetCodeMajPromFailed);
        }
    }

    const EntryRecord_t &tNewRecord = poMachine->GetRecord(iLocalAcceptorID);
    const EntryRecord_t &tRemoteRecord = poMachine->GetRecord(iAcceptorID);

    bool bRemoteUpdated = IsEntryRecordUpdated(tDestRecord, tNewRecord);

    bool bLocalUpdated = IsEntryRecordUpdated(tOldRecord, tNewRecord);

    clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID,
            iEntityID, iEntry, &tNewRecord, &tRemoteRecord);
    po->SetUUID(poPaxosCmd->GetUUID());
    po->SetMaxPLogEntry((uint64_t)ptEntityInfo->iMaxPLogEntry);

    AssertEqual(ptInfo->bBroadcast, false);
    po->SetDestAcceptorID(iAcceptorID);

    if (bLocalUpdated)
    {
        ptInfo->bRemoteUpdated = bRemoteUpdated;

        ptInfo->bUncertain = true;

        // Opt for req on the newest entry.
        if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY
                && ptEntityInfo->iMaxChosenEntry <= iEntry)
        {
            po->SetCheckHasMore(true);
        }

        iRet = clsPLogWorker::EnterPLogReqQueue(po);
        if (iRet != 0)
        {
            CertainLogError("EnterPLogReqQueue ret %d", iRet);

            delete po, po = NULL;
            ptInfo->bUncertain = false;
            CleanUpEntry(ptInfo);

            return -3;
        }

        // Only slave has lease, as requests go to master first.
        if (iEntry > ptEntityInfo->iMaxChosenEntry
                && poMachine->IsRemoteCompeting())
        {
            m_poEntityMng->UpdateSuggestedLease(ptEntityInfo);
        }
    }
    else
    {
        CheckIfNeedNotifyDB(ptEntityInfo);
        clsAutoDelete<clsPaxosCmd> oAuto(po);

        if (ptEntityInfo->poClientCmd != NULL
                && ptEntityInfo->poClientCmd->IsReadOnly())
        {
            if (ptEntityInfo->poClientCmd->GetUUID() == poPaxosCmd->GetUUID()
                    && poMachine->IsLocalEmpty())
            {
                poMachine->SetCheckedEmpty(poPaxosCmd->GetSrcAcceptorID());
            }

            if (poMachine->IsReadOK())
            {
                InvalidClientCmd(ptEntityInfo, eRetCodeOK);
                return 0;
            }
            else if (!poMachine->IsLocalEmpty())
            {
                InvalidClientCmd(ptEntityInfo, eRetCodeReadFailed);
            }
        }

        ptInfo->bRemoteUpdated = bRemoteUpdated;
        SyncEntryRecord(ptInfo, po->GetDestAcceptorID(), po->GetUUID());
    }

    return 0;
}

void clsEntityWorker::CleanUpEntry(EntryInfo_t *ptInfo)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    AssertNotEqual(ptEntityInfo, NULL);

    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;

    CertainLogDebug("E(%lu, %lu)", iEntityID, iEntry);

    if (ptEntityInfo->poClientCmd != NULL
            && ptEntityInfo->poClientCmd->GetEntry() == ptInfo->iEntry)
    {
        AssertNotEqual(ptInfo->iEntry, 0);
        InvalidClientCmd(ptEntityInfo, eRetCodeCleanUp);
    }

    m_poEntryMng->DestroyEntryInfo(ptInfo);
}

int clsEntityWorker::DoWithPaxosCmd(clsPaxosCmd *poPaxosCmd)
{
    int iRet;

    uint32_t iRandomDropRatio = m_poConf->GetRandomDropRatio();
    if (iRandomDropRatio > 0 && m_poRandom->Next() % 100 < iRandomDropRatio)
    {
        return eRetCodeRandomDrop;
    }

    uint32_t iIOReqTimeoutUS = m_poConf->GetIOReqTimeoutMS() * 1000;
    if (iIOReqTimeoutUS > 0)
    {
        uint64_t iCurrTimeUS = GetCurrTimeUS();
        if (iCurrTimeUS >= poPaxosCmd->GetTimestampUS() + iIOReqTimeoutUS)
        {
            CertainLogError("reject cmd: %s", poPaxosCmd->GetTextCmd().c_str());
            return eRetCodeReject;
        }
    }

    uint32_t iAcceptorID = poPaxosCmd->GetSrcAcceptorID();
    uint64_t iEntityID = poPaxosCmd->GetEntityID();
    uint64_t iEntry = poPaxosCmd->GetEntry();

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    if (ptEntityInfo == NULL)
    {
        if (!m_poEntityMng->CheckAndEliminate())
        {
            if (!EvictEntity(NULL))
            {
                OSS::ReportEvictFail();
                OSS::ReportEntityLimited();
                return eRetCodeEntityLimited;
            }
            else
            {
                OSS::ReportEvictSucc();
            }
        }

        ptEntityInfo = m_poEntityMng->CreateEntityInfo(iEntityID);
        if (ptEntityInfo == NULL)
        {
            CertainLogFatal("CreateEntityInfo failed cmd: %s",
                    poPaxosCmd->GetTextCmd().c_str());
            return eRetCodeRouteErr;
        }

        RangeLoadFromPLog(ptEntityInfo);
    }

    Assert(m_poEntityMng->Refresh(ptEntityInfo));

    ptEntityInfo->iActiveAcceptorID = iAcceptorID;

    if (poPaxosCmd->GetResult() == eRetCodeNotFound)
    {
        CertainLogError("E(%lu %lu) not found, need get all, "
                "bGetAllPending %d MaxPLogEntry %lu MaxContChosenEntry %lu",
                iEntityID, iEntry, ptEntityInfo->bGetAllPending,
                ptEntityInfo->iMaxPLogEntry, ptEntityInfo->iMaxContChosenEntry);

        if(ptEntityInfo->bGetAllPending)
        {
            return eRetCodeGetAllPending;
        }

        if(ptEntityInfo->iMaxContChosenEntry >= iEntry)
        {
            return 0;
        }

        ptEntityInfo->bGetAllPending = true;
        InvalidClientCmd(ptEntityInfo, eRetCodeGetAllPending);

        iRet = clsGetAllWorker::EnterReqQueue(poPaxosCmd);
        if(iRet == 0)
        {
            return eRetCodePtrReuse;
        }

        ptEntityInfo->bGetAllPending = false;
        return eRetCodeQueueFailed;
    }

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;
    AssertNotEqual(iAcceptorID, iLocalAcceptorID);

    // INVALID_ACCEPTOR_ID was used to get a identical packet.
    if (poPaxosCmd->GetDestAcceptorID() == INVALID_ACCEPTOR_ID)
    {
        poPaxosCmd->SetDestAcceptorID(iLocalAcceptorID);
    }

    if (!ptEntityInfo->bRangeLoaded)
    {
        // RangeLoadFromPLog first, which will GetMaxPLogEntry.
        if (!ptEntityInfo->bRangeLoading)
        {
            RangeLoadFromPLog(ptEntityInfo);

            if (!ptEntityInfo->bRangeLoading)
            {
                CertainLogError("ignore cmd: %s", poPaxosCmd->GetTextCmd().c_str());
                return eRetCodeMsgIngnored;
            }
        }
    }

    // Wait for resolving uncertain status.
    EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
    if (ptInfo != NULL && ptInfo->bUncertain)
    {
        if (m_poMemCacheCtrl->IsOverLoad())
        {
            CertainLogError("E(%lu, %lu) TotalSize %lu",
                    iEntityID, iEntry, m_poMemCacheCtrl->GetTotalSize());
            return eRetCodeMemCacheLimited;
        }

        if (ptInfo->apWaitingMsg[iAcceptorID] != NULL)
        {
            CertainLogInfo("cmd: %s replaced with new cmd: %s",
                    ptInfo->apWaitingMsg[iAcceptorID]->GetTextCmd().c_str(),
                    poPaxosCmd->GetTextCmd().c_str());

            delete ptInfo->apWaitingMsg[iAcceptorID];
            ptInfo->apWaitingMsg[iAcceptorID] = NULL;
        }

        CertainLogInfo("waiting cmd: %s", poPaxosCmd->GetTextCmd().c_str());

        ptInfo->apWaitingMsg[iAcceptorID] = poPaxosCmd;
        m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
        return eRetCodePtrReuse;
    }

    CertainLogDebug("iAcceptorID %u E(%lu, %lu) entrys: %lu %lu",
            iAcceptorID, iEntityID, iEntry, ptEntityInfo->iMaxChosenEntry,
            ptEntityInfo->iMaxContChosenEntry);

    if (ptEntityInfo->bRangeLoading)
    {
        if (m_poMemCacheCtrl->IsOverLoad())
        {
            CertainLogError("E(%lu, %lu) TotalSize %lu",
                    iEntityID, iEntry, m_poMemCacheCtrl->GetTotalSize());
            return eRetCodeMemCacheLimited;
        }

        // The first loading could contain the entry probably.
        if (ptEntityInfo->iMaxChosenEntry == 0)
        {
            if (ptEntityInfo->apWaitingMsg[iAcceptorID] == NULL)
            {
                CertainLogInfo("pending cmd: %s",
                        poPaxosCmd->GetTextCmd().c_str());

                ptEntityInfo->apWaitingMsg[iAcceptorID] = poPaxosCmd;
                m_poMemCacheCtrl->UpdateTotalSize(ptEntityInfo);
                return eRetCodePtrReuse;
            }
            else if (ptEntityInfo->apWaitingMsg[iAcceptorID]->GetEntry() < iEntry)
            {
                CertainLogError("cmd: %s replaced with new cmd: %s",
                        ptEntityInfo->apWaitingMsg[iAcceptorID]->GetTextCmd().c_str(),
                        poPaxosCmd->GetTextCmd().c_str());
                delete ptEntityInfo->apWaitingMsg[iAcceptorID];
                ptEntityInfo->apWaitingMsg[iAcceptorID] = poPaxosCmd;
                m_poMemCacheCtrl->UpdateTotalSize(ptEntityInfo);
                return eRetCodePtrReuse;
            }
            else
            {
                CertainLogError("ignore cmd: %s", poPaxosCmd->GetTextCmd().c_str());
                return eRetCodeMsgIngnored;
            }
        }

        if (!ptEntityInfo->bRangeLoaded)
        {
            // Avoid GetMaxPLogEntry before PutRecord.
            CertainLogError("ignore cmd: %s", poPaxosCmd->GetTextCmd().c_str());
            return eRetCodeMsgIngnored;
        }
    }

    CertainLogDebug("iAcceptorID %u E(%lu, %lu) iMaxChosenEntry %lu",
            iAcceptorID, iEntityID, iEntry, ptEntityInfo->iMaxChosenEntry);

    if (m_poConf->GetEnableLearnOnly() && !poPaxosCmd->GetSrcRecord().bChosen)
    {
        CheckForCatchUp(ptEntityInfo, iAcceptorID, poPaxosCmd->GetMaxChosenEntry());
        CertainLogError("learn only, ignore cmd: %s", poPaxosCmd->GetTextCmd().c_str());
        return eRetCodeMsgIngnored;
    }

    if(ptEntityInfo->iMaxChosenEntry >= iEntry && poPaxosCmd->IsQuickRsp())
    {
        clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID, iEntityID,
                iEntry, NULL, NULL);
        po->SetDestAcceptorID(iAcceptorID);
        po->SetResult(eRetCodeRemoteNewer);
        po->SetUUID(poPaxosCmd->GetUUID());
        po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));

        OSS::ReportFastFail();
        m_poIOWorkerRouter->GoAndDeleteIfFailed(po);

        CheckForCatchUp(ptEntityInfo, iAcceptorID, 0);

        CertainLogError("E(%lu %lu) QuickRsp iMaxChosenEntry %lu",
                iEntityID, iEntry, ptEntityInfo->iMaxChosenEntry);
        return 0;
    }

    if(poPaxosCmd->GetResult() == eRetCodeRemoteNewer)
    {
        if (ptEntityInfo->poClientCmd != NULL)
        {
            if(ptEntityInfo->poClientCmd->GetUUID() == poPaxosCmd->GetUUID()
                    && ptEntityInfo->poClientCmd->GetEntry() == poPaxosCmd->GetEntry())
            {
                assert(ptEntityInfo->poClientCmd->IsReadOnly());
                InvalidClientCmd(ptEntityInfo, eRetCodeRemoteNewer);
            }
        }

        if (ptInfo != NULL && !ptInfo->bUncertain)
        {
            if (ptEntityInfo->poClientCmd != NULL
                    && ptEntityInfo->poClientCmd->GetEntry() == iEntry)
            {
                InvalidClientCmd(ptEntityInfo);
            }
            m_poEntryMng->RemoveTimeout(ptInfo);
            CertainLogError("E(%lu, %lu) Remove For CatchUp", iEntityID, iEntry);
        }
        CheckForCatchUp(ptEntityInfo, iAcceptorID, poPaxosCmd->GetMaxChosenEntry());
        return 0;
    }

    if (ptEntityInfo->iMaxChosenEntry >= iEntry)
    {
        if (ptInfo != NULL
                && ptInfo->poMachine->GetEntryState() == kEntryStateChosen)
        {
            if (poPaxosCmd->GetSrcRecord().bChosen)
            {
                return 0;
            }

            const EntryRecord_t &tLocalRecord = ptInfo->poMachine->GetRecord(
                    iLocalAcceptorID);
            const EntryRecord_t &tRemoteRecord = poPaxosCmd->GetSrcRecord();
            clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID, iEntityID,
                    iEntry, &tLocalRecord, &tRemoteRecord);

            po->SetDestAcceptorID(iAcceptorID);
            po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));
            po->SetUUID(poPaxosCmd->GetUUID());

            m_poIOWorkerRouter->GoAndDeleteIfFailed(po);

            return 0;
        }

        // Entry not more than iMaxContChosenEntry must be chosen.
        if (ptEntityInfo->iMaxContChosenEntry >= iEntry)
        {
            if (poPaxosCmd->GetSrcRecord().bChosen)
            {
                return 0;
            }

            clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID, iEntityID,
                    iEntry, NULL, &poPaxosCmd->GetSrcRecord());

            po->SetDestAcceptorID(iAcceptorID);
            po->SetPLogReturn(true);
            po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));
            po->SetUUID(poPaxosCmd->GetUUID());

            iRet = clsPLogWorker::EnterPLogReqQueue(po);
            if (iRet != 0)
            {
                delete po, po = NULL;
                CertainLogError("EnterPLogReqQueue ret %d", iRet);
                return -1;
            }

            return 0;
        }

        CertainLogInfo("Check if msg delay cmd: %s",
                poPaxosCmd->GetTextCmd().c_str());
    }

    if (ptInfo == NULL)
    {
        if (CheckIfEntryNumLimited(iEntityID))
        {
            return eRetCodeEntryLimited;
        }

        ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, iEntry);
        m_poEntryMng->AddTimeout(ptInfo, 10000);

        if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY
                || iEntry <= ptEntityInfo->iMaxPLogEntry)
        {
            if (LoadFromPLogWorker(ptInfo) != 0)
            {
                m_poEntryMng->DestroyEntryInfo(ptInfo);
                return -2;
            }
            else
            {
                ptInfo->apWaitingMsg[iAcceptorID] = poPaxosCmd;
                m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
                return eRetCodePtrReuse;
            }
        }
    }
    else if(ptInfo->bNotFound)
    {
        AssertNotMore(iEntry, ptEntityInfo->iMaxContChosenEntry);

        clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID, iEntityID,
                iEntry, NULL,  NULL);

        po->SetResult(eRetCodeNotFound);
        po->SetDestAcceptorID(iAcceptorID);
        po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));
        po->SetUUID(poPaxosCmd->GetUUID());

        m_poIOWorkerRouter->GoAndDeleteIfFailed(po);

        CertainLogInfo("E(%lu %lu) not found and MaxContChoseEntry %lu",
                iEntityID, iEntry, ptEntityInfo->iMaxContChosenEntry);
        return 0;

    }
    else if(ptInfo->poMachine->GetEntryState() != kEntryStateChosen
            && ptEntityInfo->iMaxContChosenEntry >= iEntry)
    {
        clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID, iEntityID,
                iEntry, NULL,  NULL);
        po->SetResult(eRetCodeNotFound);
        po->SetDestAcceptorID(iAcceptorID);
        po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));
        po->SetUUID(poPaxosCmd->GetUUID());

        m_poIOWorkerRouter->GoAndDeleteIfFailed(po);

        CertainLogFatal("E(%lu %lu) not found and MaxContChoseEntry %lu",
                iEntityID, iEntry, ptEntityInfo->iMaxContChosenEntry);
        return 0;
    }

    if (ptEntityInfo->poClientCmd == NULL
            || ptEntityInfo->poClientCmd->GetEntry() != iEntry)
    {
        // 1. Avoid multi create for concurrent read.
        // 2. When timeout, cleanup if empty, otherwise to get chosen.
        m_poEntryMng->AddTimeout(ptInfo, 10000);
    }

    AssertNotEqual(ptInfo->poMachine->GetEntryState(), kEntryStateChosen);
    iRet = UpdateRecord(poPaxosCmd);
    if (iRet != 0)
    {
        CertainLogError("UpdateRecord ret %d", iRet);
        return -3;
    }

    CheckForCatchUp(ptEntityInfo, iAcceptorID,
            max(iEntry - 1, poPaxosCmd->GetMaxChosenEntry()));

    return 0;
}

void clsEntityWorker::InvalidClientCmd(EntityInfo_t *ptEntityInfo, int iResult)
{
    clsClientCmd *poClientCmd = ptEntityInfo->poClientCmd;
    ptEntityInfo->poClientCmd = NULL;

    if (poClientCmd != NULL && poClientCmd->GetCmdID() == kRecoverCmd)
    {
        uint64_t iEntityID = ptEntityInfo->iEntityID;
        EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, 0);
        if (ptInfo != NULL)
        {
            m_poEntryMng->DestroyEntryInfo(ptInfo);
        }
    }

    InvalidClientCmd(poClientCmd, iResult);
}

bool clsEntityWorker::InvalidClientCmd(clsClientCmd *poCmd, int iResult)
{
    if (poCmd == NULL)
    {
        return false;
    }

    poCmd->SetResult(iResult);

    if (iResult != eRetCodeOK)
    {
        CertainLogError("InvalidClientCmd cmd %u uuid %lu result: %d",
                poCmd->GetCmdID(), poCmd->GetUUID(), iResult);
    }

    int iRet = m_poIOWorkerRouter->Go(poCmd);
    if (iRet != 0)
    {
        CertainLogError("BUG probably Go ret %d poCmd: %s",
                iRet, poCmd->GetTextCmd().c_str());

        // sum of list size <= MAX_ASYNC_PIPE_NUM
        m_poWaitingGoList.push_back(poCmd);
        return false;
    }

    return true;
}

void clsEntityWorker::CheckIfWaitRecoverCmd(EntityInfo_t *ptEntityInfo,
        clsRecoverCmd *poCmd)
{
    // If the cmd is most likely timeout eventually, do fast failover.
    if (!ptEntityInfo->bRangeLoading)
    {
        // It should range load from plog, but failed as queue full.
        InvalidClientCmd(poCmd, eRetCodeQueueFull);
    }
    else
    {
        ptEntityInfo->poClientCmd = poCmd;
        EntryInfo_t *ptInfo = m_poEntryMng->CreateEntryInfo(ptEntityInfo, 0);
        m_poEntryMng->AddTimeout(ptInfo, m_poConf->GetRecoverTimeoutMS());
    }
}

bool clsEntityWorker::EvictEntity(EntityInfo_t *ptEntityInfo)
{
    if (ptEntityInfo == NULL)
    {
        ptEntityInfo = m_poEntityMng->PeekOldest();
        AssertNotEqual(ptEntityInfo, NULL);
    }

    CertainLogImpt("iEntityID %lu iRefCount %d bRangeLoading %u bGetAllPending %u",
            ptEntityInfo->iEntityID, ptEntityInfo->iRefCount,
            ptEntityInfo->bRangeLoading, ptEntityInfo->bGetAllPending);

    if (ptEntityInfo->bRangeLoading || ptEntityInfo->bGetAllPending)
    {
        Assert(m_poEntityMng->Refresh(ptEntityInfo));

        return false;
    }

    while (ptEntityInfo->iRefCount > 1)
    {
        int iOldRefCount = ptEntityInfo->iRefCount;

        EntryInfo_t *ptInfo = CIRCLEQ_FIRST(&ptEntityInfo->tEntryList);

        AssertNotEqual(ptInfo, NULL);
        if (ptInfo->bUncertain)
        {
            Assert(m_poEntityMng->Refresh(ptEntityInfo));

            return false;
        }
        CleanUpEntry(ptInfo);

        AssertEqual(iOldRefCount, ptEntityInfo->iRefCount + 1);
    }

    AssertEqual(ptEntityInfo->poClientCmd, NULL);

    for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
    {
        if (ptEntityInfo->apWaitingMsg[i] != NULL)
        {
            delete ptEntityInfo->apWaitingMsg[i];
            ptEntityInfo->apWaitingMsg[i] = NULL;
        }
    }

    m_poEntityMng->DestroyEntityInfo(ptEntityInfo);

    return true;
}

int clsEntityWorker::DoWithRecoverCmd(clsRecoverCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    uint64_t iMaxCommitedEntry = poCmd->GetMaxCommitedEntry();
    CertainLogInfo("cmd: %s", poCmd->GetTextCmd().c_str());

    EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, 0);
    AssertEqual(ptInfo, NULL);

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    if (ptEntityInfo != NULL)
    {
        CertainLogError("Failover E(%lu, %lu) Entrys: %lu %lu %lu ref %d %u",
                iEntityID, iMaxCommitedEntry,
                ptEntityInfo->iMaxContChosenEntry,
                ptEntityInfo->iMaxChosenEntry, ptEntityInfo->iMaxPLogEntry,
                ptEntityInfo->iRefCount, ptEntityInfo->bRangeLoading);

        if (poCmd->IsEvictEntity())
        {
            if (EvictEntity(ptEntityInfo))
            {
                OSS::ReportEvictSucc();
                CertainLogImpt("Evict iEntityID %lu succ", iEntityID);
                InvalidClientCmd(poCmd, eRetCodeOK);
            }
            else
            {
                OSS::ReportEvictFail();
                CertainLogError("Evict iEntityID %lu fail", iEntityID);
                InvalidClientCmd(poCmd, eRetCodePLogPending);
            }
            return eRetCodePtrReuse;
        }

        Assert(m_poEntityMng->Refresh(ptEntityInfo));

        if (poCmd->IsCheckGetAll())
        {
            if (ptEntityInfo->bGetAllPending)
            {
                InvalidClientCmd(poCmd, eRetCodeGetAllPending);
                return eRetCodePtrReuse;
            }

            // Use iGetAllFinishTimeMS to avoid starting another GetAll, when the last one has just finished.
            if (ptEntityInfo->iMaxContChosenEntry > iMaxCommitedEntry
                    && GetCurrTimeMS() < ptEntityInfo->iGetAllFinishTimeMS + 1000)
            {
                CertainLogError("iEntityID %lu Check if GetAll has just finished", iEntityID);
                InvalidClientCmd(poCmd, eRetCodeOK);
                return eRetCodePtrReuse;
            }
            else
            {
                if (ptEntityInfo->iMaxContChosenEntry < iMaxCommitedEntry)
                {
                    CertainLogFatal("iEntityID %lu Entrys: %lu %lu Check if busy",
                            iEntityID, iMaxCommitedEntry, ptEntityInfo->iMaxContChosenEntry);
                }

                // GetAll to recover/fix local machine
                CertainLogError("notified to GetAll E(%lu, %lu) Entrys: %lu %lu %lu ref %d %u",
                        iEntityID, iMaxCommitedEntry,
                        ptEntityInfo->iMaxContChosenEntry,
                        ptEntityInfo->iMaxChosenEntry, ptEntityInfo->iMaxPLogEntry,
                        ptEntityInfo->iRefCount, ptEntityInfo->bRangeLoading);

                clsPaxosCmd *po = new clsPaxosCmd(GetPeerAcceptorID(ptEntityInfo), iEntityID, 0, NULL, NULL);

                int iRet = clsGetAllWorker::EnterReqQueue(po);
                if(iRet != 0)
                {
                    delete po, po = NULL;
                    InvalidClientCmd(poCmd, eRetCodeQueueFailed);
                    return eRetCodePtrReuse;
                }

                ptEntityInfo->bGetAllPending = true;
                InvalidClientCmd(poCmd, eRetCodeGetAllPending);
                return eRetCodePtrReuse;
            }
        }

        CheckForCatchUp(ptEntityInfo, INVALID_ACCEPTOR_ID, 0);
        CheckIfNeedNotifyDB(ptEntityInfo);

        poCmd->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));
        poCmd->SetMaxContChosenEntry(uint64_t(ptEntityInfo->iMaxContChosenEntry));

        AssertEqual(ptEntityInfo->poClientCmd, NULL);
        if (ptEntityInfo->iMaxContChosenEntry < ptEntityInfo->iMaxChosenEntry)
        {
            InvalidClientCmd(poCmd, eRetCodeCatchUp);
            return eRetCodePtrReuse;
        }
        else if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY)
        {
            CheckIfWaitRecoverCmd(ptEntityInfo, poCmd);
            return eRetCodePtrReuse;
        }

        InvalidClientCmd(poCmd, eRetCodeOK);
    }
    else
    {
        if (poCmd->IsEvictEntity())
        {
            CertainLogError("not need Evict iEntityID %lu", iEntityID);
            InvalidClientCmd(poCmd, eRetCodeOK);
            return eRetCodePtrReuse;
        }

        if (!m_poEntityMng->CheckAndEliminate())
        {
            if (!EvictEntity(NULL))
            {
                OSS::ReportEvictFail();
                OSS::ReportEntityLimited();
                InvalidClientCmd(poCmd, eRetCodeEntityLimited);
                return eRetCodePtrReuse;
            }
            else
            {
                OSS::ReportEvictSucc();
            }
        }

        ptEntityInfo = m_poEntityMng->CreateEntityInfo(iEntityID);
        if (ptEntityInfo == NULL)
        {
            CertainLogFatal("CreateEntityInfo failed cmd: %s",
                    poCmd->GetTextCmd().c_str());
            InvalidClientCmd(poCmd, eRetCodeRouteErr);
            return eRetCodePtrReuse;
        }

        CheckForCatchUp(ptEntityInfo, INVALID_ACCEPTOR_ID, 0);

        CheckIfWaitRecoverCmd(ptEntityInfo, poCmd);
        return eRetCodePtrReuse;
    }

    return eRetCodePtrReuse;
}

int clsEntityWorker::DoWithIOReq(clsCmdBase *poCmd)
{
    if (clsCertainWrapper::GetInstance()->GetConf()->GetEnableLearnOnly())
    {
        if (poCmd->GetCmdID() != kPaxosCmd)
        {
            clsClientCmd *poClientCmd = dynamic_cast<clsClientCmd *>(poCmd);
            AssertNotEqual(poCmd, NULL);
            InvalidClientCmd(poClientCmd, eRetCodeRejectAll);
            return eRetCodePtrReuse;
        }
        else
        {
            return eRetCodeRejectAll;
        }
    }

    uint64_t iEntityID = poCmd->GetEntityID();
    AssertEqual(Hash(iEntityID) % m_poConf->GetEntityWorkerNum(), m_iWorkerID);
    CertainLogInfo("cmd: %s", poCmd->GetTextCmd().c_str());

    clsPaxosCmd *poPaxosCmd = NULL;
    clsClientCmd *poClientCmd = NULL;
    clsRecoverCmd *poRecoverCmd = NULL;

    switch (poCmd->GetCmdID())
    {
        case kWriteBatchCmd:
            poClientCmd = dynamic_cast<clsClientCmd *>(poCmd);
            return DoWithClientCmd(poClientCmd);

        case kRecoverCmd:
            poRecoverCmd = dynamic_cast<clsRecoverCmd *>(poCmd);
            return DoWithRecoverCmd(poRecoverCmd);

        case kPaxosCmd:
            poPaxosCmd = dynamic_cast<clsPaxosCmd *>(poCmd);
            return DoWithPaxosCmd(poPaxosCmd);

        default:
            CertainLogError("cmd: %s", poCmd->GetTextCmd().c_str());
            Assert(false);
    }

    return 0;
}

void clsEntityWorker::SyncEntryRecord(EntryInfo_t *ptInfo,
        uint32_t iDestAcceptorID, uint64_t iUUID)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    AssertNotEqual(ptEntityInfo, NULL);

    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;

    clsEntryStateMachine *poMachine = ptInfo->poMachine;
    const EntryRecord_t &tSrcRecord = poMachine->GetRecord(iLocalAcceptorID);

    CertainLogDebug("state %d bRemoteUpdated %u bBroadcast %u record %s",
            poMachine->GetEntryState(), ptInfo->bRemoteUpdated,
            ptInfo->bBroadcast, EntryRecordToString(tSrcRecord).c_str());

    if (ptInfo->bBroadcast)
    {
        AssertEqual(iDestAcceptorID, INVALID_ACCEPTOR_ID);

        BroadcastToRemote(ptInfo, poMachine);
        ptInfo->bBroadcast = false;
        ptInfo->bRemoteUpdated = false;
    }

    if (ptInfo->bRemoteUpdated)
    {
        AssertNotEqual(iDestAcceptorID, INVALID_ACCEPTOR_ID);

        const EntryRecord_t &tDestRecord = poMachine->GetRecord(
                iDestAcceptorID);

        // If dest has been chosen, it's no use to send msg.
        if (!tDestRecord.bChosen)
        {
            clsPaxosCmd *po = new clsPaxosCmd(iLocalAcceptorID, iEntityID,
                    iEntry, &tSrcRecord, &tDestRecord);

            po->SetUUID(iUUID);
            po->SetDestAcceptorID(iDestAcceptorID);

            po->SetMaxChosenEntry(uint64_t(ptEntityInfo->iMaxChosenEntry));
            m_poIOWorkerRouter->GoAndDeleteIfFailed(po);
        }
        ptInfo->bRemoteUpdated = false;
    }
}

int clsEntityWorker::DoWithWaitingMsg(
        clsPaxosCmd **apWaitingMsg, uint32_t iCnt)
{
    uint32_t iFailCnt = 0;

    for (uint32_t i = 0; i < iCnt; ++i)
    {
        clsPaxosCmd *poPaxosCmd = apWaitingMsg[i];
        apWaitingMsg[i] = NULL;

        if (poPaxosCmd == NULL)
        {
            continue;
        }

        CertainLogInfo("cmd: %s", poPaxosCmd->GetTextCmd().c_str());

        int iRet = DoWithIOReq(dynamic_cast<clsCmdBase *>(poPaxosCmd));
        if (iRet < 0)
        {
            iFailCnt++;
            CertainLogError("DoWithIOReq ret %d cmd %s",
                    iRet, poPaxosCmd->GetTextCmd().c_str());
        }
        if (iRet != eRetCodePtrReuse)
        {
            delete poPaxosCmd, poPaxosCmd = NULL;
        }
    }

    if (iFailCnt > 0)
    {
        CertainLogError("iFailCnt %u", iFailCnt);
        return -1;
    }

    return 0;
}

int clsEntityWorker::DoWithGetAllRsp(clsPaxosCmd *poPaxosCmd)
{
    OSS::ReportGetAllRsp();

    uint32_t iAcceptorID = poPaxosCmd->GetSrcAcceptorID();
    uint64_t iEntityID = poPaxosCmd->GetEntityID();
    uint64_t iEntry = poPaxosCmd->GetEntry();

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    if (ptEntityInfo == NULL)
    {
        CertainLogError("E(%lu, %lu) AcceptID %u Entity not in mem",
                iEntityID, iEntry, iAcceptorID);
        return eRetCodeFailed;
    }

    AssertEqual(ptEntityInfo->bGetAllPending, true);
    ptEntityInfo->bGetAllPending = false;

    CertainLogImpt("E(%lu, %lu) RemoteAcceptID %u RemoteMaxPos %lu "
            "iMaxContChosenEntry %lu iMaxChosenEntry %lu",
            iEntityID, iEntry, iAcceptorID, iEntry,
            ptEntityInfo->iMaxContChosenEntry, ptEntityInfo->iMaxChosenEntry);

    if (poPaxosCmd->GetResult() != 0)
    {
        CertainLogError("E(%lu, %lu) AcceptID %u getall ret %d",
                iEntityID, iEntry, iAcceptorID, poPaxosCmd->GetResult());
        return eRetCodeFailed;
    }

    if(ptEntityInfo->iMaxContChosenEntry < iEntry)
    {
        ptEntityInfo->iMaxContChosenEntry = iEntry;
    }

    if (ptEntityInfo->iNotifyedEntry < iEntry)
    {
        ptEntityInfo->iNotifyedEntry = iEntry;
    }

    if(ptEntityInfo->iMaxChosenEntry < iEntry)
    {
        ptEntityInfo->iMaxChosenEntry = iEntry;
    }

    if(ptEntityInfo->iCatchUpEntry < ptEntityInfo->iMaxContChosenEntry)
    {
        ptEntityInfo->iCatchUpEntry = ptEntityInfo->iMaxContChosenEntry;
    }

    if(ptEntityInfo->iMaxPLogEntry < iEntry)
    {
        ptEntityInfo->iMaxPLogEntry = iEntry;
    }

    CheckForCatchUp(ptEntityInfo, INVALID_ACCEPTOR_ID, 0);

    ptEntityInfo->iGetAllFinishTimeMS = GetCurrTimeMS();

    return 0;
}

int clsEntityWorker::DoWithPLogRsp(clsCmdBase *poCmd)
{
    if (poCmd->GetCmdID() == kPaxosCmd)
    {
        return DoWithPaxosCmdFromPLog(dynamic_cast<clsPaxosCmd *>(poCmd));
    }
    else
    {
        AssertEqual(poCmd->GetCmdID(), kRecoverCmd);
        return RangeRecoverFromPLog(dynamic_cast<clsRecoverCmd *>(poCmd));
    }
}

int clsEntityWorker::DoWithPaxosCmdFromPLog(clsPaxosCmd *poPaxosCmd)
{
    int iRet;
    uint64_t iEntityID = poPaxosCmd->GetEntityID();
    uint64_t iEntry = poPaxosCmd->GetEntry();
    AssertEqual(Hash(iEntityID) % m_poConf->GetEntityWorkerNum(), m_iWorkerID);

    CertainLogDebug("E(%lu, %lu)", iEntityID, iEntry);

    EntryInfo_t *ptInfo = m_poEntryMng->FindEntryInfo(iEntityID, iEntry);
    AssertNotEqual(ptInfo, NULL);
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    AssertNotEqual(ptEntityInfo, NULL);

    if (!m_poEntryMng->WaitForTimeout(ptInfo))
    {
        m_poEntryMng->AddTimeout(ptInfo, 15000);
    }

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;
    AssertLess(iLocalAcceptorID, m_iAcceptorNum);

    AssertEqual(ptInfo->bUncertain, true);

    if (poPaxosCmd->IsPLogError())
    {
        CertainLogError("plog error cmd: %s", poPaxosCmd->GetTextCmd().c_str());
        ptInfo->bUncertain = false;
        CleanUpEntry(ptInfo);
        return -1;
    }

    if (poPaxosCmd->IsPLogLoad())
    {
        return RecoverFromPLogWorker(poPaxosCmd);
    }

    ptInfo->bUncertain = false;

    // Keep iMaxPLogEntry consistent with data in disk.
    if (poPaxosCmd->IsCheckHasMore() && !poPaxosCmd->IsHasMore())
    {
        if (ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY || ptEntityInfo->iMaxPLogEntry < iEntry)
        {
            ptEntityInfo->iMaxPLogEntry = iEntry;
        }
    }
    else if (ptEntityInfo->iMaxPLogEntry != INVALID_ENTRY
            && ptEntityInfo->iMaxPLogEntry < iEntry)
    {
        ptEntityInfo->iMaxPLogEntry = iEntry;
    }

    clsEntryStateMachine *poMachine = ptInfo->poMachine;

    if(iEntry <= ptEntityInfo->iMaxContChosenEntry 
            && poMachine->GetEntryState() != kEntryStateChosen)
    {
        clsClientCmd * poClientCmd = ptEntityInfo->poClientCmd;

        CertainLogError("E(%lu, %lu) MaxContChosenEntry %lu ClientNull %d",
                iEntityID, iEntry, ptEntityInfo->iMaxContChosenEntry, poClientCmd==NULL);

        if (poClientCmd != NULL && poClientCmd->GetEntry() == iEntry)
        {
            InvalidClientCmd(ptEntityInfo);
        }
        return -2;
    }

    SyncEntryRecord(ptInfo, poPaxosCmd->GetDestAcceptorID(),
            poPaxosCmd->GetUUID());

    const EntryRecord_t &tSrcRecord = poMachine->GetRecord(iLocalAcceptorID);
    if (!(poPaxosCmd->GetSrcRecord() == tSrcRecord))
    {
        AssertEqual(true, false);
    }

    poMachine->SetStoredValueID(iLocalAcceptorID);

    if (poMachine->GetEntryState() == kEntryStateChosen)
    {
        m_poEntryMng->RemoveCatchUpFlag(ptInfo);

        OSS::ReportChosenProposalNum(tSrcRecord.iAcceptedNum);

        if (tSrcRecord.iAcceptedNum > m_iAcceptorNum)
        {
            CertainLogInfo("Not PreAuth record: %s",
                    EntryRecordToString(tSrcRecord).c_str());
        }

        // Update iLocalPreAuthEntry.
        if (clsEntryStateMachine::GetAcceptorID(tSrcRecord.tValue.iValueID)
                == ptEntityInfo->iLocalAcceptorID)
        {
            if (ptEntityInfo->iLocalPreAuthEntry < iEntry + 1)
            {
                ptEntityInfo->iLocalPreAuthEntry = iEntry + 1;
            }
        }

        if (ptEntityInfo->iMaxChosenEntry < iEntry)
        {
            ptEntityInfo->iMaxChosenEntry = iEntry;

            // Remove Lease for the slave when chosen.
            if (ptEntityInfo->iLocalAcceptorID != 0)
            {
                ptEntityInfo->iLeaseExpiredTimeMS = 0;
            }
        }

        clsClientCmd *poClientCmd = ptEntityInfo->poClientCmd;
        AssertNotEqual(tSrcRecord.tValue.iValueID, 0);

        // wb.size == 0 <==> readonly cmd
        if (tSrcRecord.tValue.strValue.size() > 0)
        {
            const vector<uint64_t> &vecUUID = tSrcRecord.tValue.vecValueUUID;
            CertainLogInfo("uuid num %lu cmd: %s",
                    vecUUID.size(), poPaxosCmd->GetTextCmd().c_str());

            for (uint32_t i = 0; i < vecUUID.size(); ++i)
            {
                m_poUUIDMng->AddUUID(iEntityID, vecUUID[i]);
            }
        }

        if (poClientCmd != NULL && poClientCmd->GetEntry() == iEntry)
        {
            CertainLogDebug("cli_cmd: %s chosen: %s",
                    poClientCmd->GetTextCmd().c_str(),
                    EntryRecordToString(tSrcRecord).c_str());
            AssertEqual(poClientCmd->GetEntityID(), iEntityID);

            if (poClientCmd->GetWriteBatchID() == tSrcRecord.tValue.iValueID)
            {
#if CERTAIN_DEBUG
                if (tSrcRecord.tValue.strValue
                        != ptEntityInfo->poClientCmd->GetWriteBatch())
                {
                    CertainLogFatal("BUG record: %s cli_cmd: %s",
                            EntryRecordToString(tSrcRecord).c_str(),
                            ptEntityInfo->poClientCmd->GetTextCmd().c_str());
                    AssertEqual(CRC32(tSrcRecord.tValue.strValue),
                            CRC32(ptEntityInfo->poClientCmd->GetWriteBatch()));
                }
#endif
                InvalidClientCmd(ptEntityInfo, eRetCodeOK);
            }
            else
            {
                CertainLogError("wb_id(%lu, %lu) cmd %s",
                        poClientCmd->GetWriteBatchID(),
                        tSrcRecord.tValue.iValueID,
                        poClientCmd->GetTextCmd().c_str());
                InvalidClientCmd(ptEntityInfo, eRetCodeOtherVChosen);
            }
        }
    }

    uint32_t iWaitingMsgPtrSize = sizeof(clsPaxosCmd*) * m_iAcceptorNum;
    clsPaxosCmd** apWaitingMsg = (clsPaxosCmd**)malloc(iWaitingMsgPtrSize);
    std::unique_ptr<char> oAutoFreeWaitingMsgPtr((char*)apWaitingMsg);

    memcpy(apWaitingMsg, ptInfo->apWaitingMsg, iWaitingMsgPtrSize);
    memset(ptInfo->apWaitingMsg, 0, iWaitingMsgPtrSize);

    iRet = DoWithWaitingMsg(apWaitingMsg, m_iAcceptorNum);
    if (iRet != 0)
    {
        CertainLogError("DoWithWaitingMsg ret %d", iRet);
    }
    else
    {
        m_poMemCacheCtrl->UpdateTotalSize(ptInfo);
    }

    CheckForCatchUp(ptEntityInfo, poPaxosCmd->GetDestAcceptorID(), 0);

    return 0;
}

void clsEntityWorker::PushCmdToDBWorker(EntryInfo_t *ptInfo)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;

    AssertEqual(ptInfo->poMachine->GetEntryState(), kEntryStateChosen);
    AssertEqual(ptEntityInfo->iMaxContChosenEntry + 1, ptInfo->iEntry);

    const EntryRecord_t &tSrcRecord = ptInfo->poMachine->GetRecord(
            ptEntityInfo->iLocalAcceptorID);

    clsClientCmd *poChosenCmd = new clsWriteBatchCmd(tSrcRecord.tValue);

    poChosenCmd->SetEntityID(ptEntityInfo->iEntityID);
    poChosenCmd->SetEntry(ptInfo->iEntry);

    int iRet = clsDBWorker::EnterDBReqQueue(poChosenCmd);
    if (iRet != 0)
    {
        CertainLogError("EnterDBReqQueue ret %d cmd: %s",
                iRet, poChosenCmd->GetTextCmd().c_str());

        // Delete it and kvsvr will help commit it.
        delete poChosenCmd, poChosenCmd = NULL;
    }
    else
    {
        AssertLess(ptEntityInfo->iNotifyedEntry, ptInfo->iEntry);
        ptEntityInfo->iNotifyedEntry = ptInfo->iEntry;
    }

    // Only the newest entry stay for a while.
    if (ptInfo->iEntry >= ptEntityInfo->iMaxChosenEntry)
    {
        m_poEntryMng->AddTimeout(ptInfo, 10000);
    }
    else
    {
        m_poEntryMng->RemoveTimeout(ptInfo);
        m_poEntryMng->IncreaseEliminatePriority(ptInfo);
    }
}

void clsEntityWorker::CheckIfNeedNotifyDB(EntityInfo_t *ptEntityInfo)
{
    if (ptEntityInfo->iNotifyedEntry < ptEntityInfo->iMaxContChosenEntry)
    {
        int iRet = clsDBWorker::NotifyDBWorker(ptEntityInfo->iEntityID);
        if (iRet != 0)
        {
            CertainLogError("NotifyDBWorker iEntityID %lu ret %d",
                    ptEntityInfo->iEntityID, iRet);
        }
        else
        {
            ptEntityInfo->iNotifyedEntry = ptEntityInfo->iMaxContChosenEntry;
        }
    }
}

int clsEntityWorker::DoWithTimeout(EntryInfo_t *ptInfo)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    if (ptEntityInfo == NULL)
    {
        CertainLogFatal("iEntry %lu", ptInfo->iEntry);
    }
    AssertNotEqual(ptEntityInfo, NULL);

    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;
    AssertEqual(Hash(iEntityID) % m_poConf->GetEntityWorkerNum(),
            m_iWorkerID);

    if (ptInfo->iEntry == 0)
    {
        if(ptEntityInfo->poClientCmd != NULL)
        {
            AssertEqual(ptEntityInfo->poClientCmd->GetCmdID(), kRecoverCmd);
        }

        InvalidClientCmd(ptEntityInfo, eRetCodeTimeout);

        return eRetCodePtrReuse;
    }

    if (ptEntityInfo->poClientCmd != NULL
            && ptEntityInfo->poClientCmd->GetEntry() == iEntry)
    {
        InvalidClientCmd(ptEntityInfo, eRetCodeTimeout);
        m_poEntryMng->AddTimeout(ptInfo, 10000);

        return eRetCodePtrReuse;
    }

    if (ptInfo->bUncertain)
    {
        CertainLogError("Check if disk busy E(%lu, %lu) st %d",
                iEntityID, iEntry, ptInfo->poMachine->GetEntryState());

        return eRetCodePtrReuse;
    }

    if(ptEntityInfo->bGetAllPending)
    {
        InvalidClientCmd(ptEntityInfo, eRetCodeTimeout);
        m_poEntryMng->DestroyEntryInfo(ptInfo);

        CertainLogError("E(%lu, %lu) GetAllPending, timeout and destroy entry",
                iEntityID, iEntry);

        return eRetCodePtrReuse;
    }

    ActivateEntry(ptInfo);

    return eRetCodePtrReuse;
}

bool clsEntityWorker::ActivateEntry(EntryInfo_t *ptInfo)
{
    EntityInfo_t *ptEntityInfo = ptInfo->ptEntityInfo;
    uint64_t iEntityID = ptEntityInfo->iEntityID;
    uint64_t iEntry = ptInfo->iEntry;

    uint32_t iLocalAcceptorID = ptEntityInfo->iLocalAcceptorID;
    clsEntryStateMachine *poMachine = ptInfo->poMachine;
    AssertEqual(ptInfo->bUncertain, false);
    AssertEqual(m_poEntryMng->WaitForTimeout(ptInfo), false);

    // The entry is newly open and has no activity.
    if (ptEntityInfo->iMaxChosenEntry < iEntry && poMachine->IsLocalEmpty())
    {
        CertainLogInfo("iEntityID %lu entrys: %lu %lu %lu ref %d",
                iEntityID, ptEntityInfo->iMaxContChosenEntry, ptEntityInfo->iCatchUpEntry,
                ptEntityInfo->iMaxChosenEntry, ptEntityInfo->iRefCount);
        CleanUpEntry(ptInfo);
        return false;
    }

    // The entry has been sent to the DB worker.
    if (ptEntityInfo->iMaxContChosenEntry >= iEntry)
    {
        CleanUpEntry(ptInfo);
        return false;
    }

    // Not need to broadcast, wait for being elimated or commited.
    if (poMachine->GetEntryState() == kEntryStateChosen)
    {
        CertainLogError("E(%lu, %lu) st %d", iEntityID, iEntry, poMachine->GetEntryState());
        m_poEntryMng->IncreaseEliminatePriority(ptInfo);
        return false;
    }

    // For catch up.
    if (iEntry <= ptEntityInfo->iMaxChosenEntry)
    {
        if (m_poEntryMng->CheckIfCatchUpLimited(ptInfo))
        {
            CertainLogError("E(%lu, %lu) catchup limited", iEntityID, iEntry);
            return false;
        }

        uint32_t iDestAcceptorID = INVALID_ACCEPTOR_ID;

        while (1)
        {
            if (ptInfo->iActiveAcceptorID == INVALID_ACCEPTOR_ID)
            {
                ptInfo->iActiveAcceptorID = 0;
            }

            iDestAcceptorID = ptInfo->iActiveAcceptorID % m_iAcceptorNum;

            if (iDestAcceptorID != ptEntityInfo->iLocalAcceptorID)
            {
                break;
            }

            // (TODO)rock: check if online
            ptInfo->iActiveAcceptorID++;
        }

        // Machine A fix the entry only.
        if (ptInfo->iActiveAcceptorID > m_iAcceptorNum && ptEntityInfo->iLocalAcceptorID == 0)
        {
            CertainLogError("May need fix E(%lu, %lu) st %d",
                    iEntityID, iEntry, ptInfo->poMachine->GetEntryState());
            OSS::ReportMayNeedFix();

            if (m_poConf->GetEnableAutoFixEntry())
            {
                CertainLogZero("ProposeNoop for Fix E(%lu, %lu) st %d",
                        iEntityID, iEntry, ptInfo->poMachine->GetEntryState());

                ProposeNoop(ptEntityInfo, ptInfo);
                return true;
            }
        }

        AssertEqual(ptInfo->bRemoteUpdated, false);
        ptInfo->bRemoteUpdated = true;
        SyncEntryRecord(ptInfo, iDestAcceptorID, 0);
        ptInfo->bRemoteUpdated = false;

        OSS::ReportSingleCatchUp();

        m_poEntryMng->AddTimeout(ptInfo, 15000);

        CertainLogError("sync acceptor %u %u E(%lu, %lu) st %d",
                iDestAcceptorID, ptInfo->iActiveAcceptorID, iEntityID, iEntry,
                ptInfo->poMachine->GetEntryState());

        ptInfo->iActiveAcceptorID++;

        return true;
    }

    if (ptInfo->iActiveAcceptorID == INVALID_ACCEPTOR_ID)
    {
        ptInfo->iActiveAcceptorID = 0;
    }

    if (ptInfo->iActiveAcceptorID >= m_iAcceptorNum * 2)
    {
        CleanUpEntry(ptInfo);
        return false;
    }
    else
    {
        ptInfo->iActiveAcceptorID += m_iAcceptorNum;

        const EntryRecord_t &tRecord = poMachine->GetRecord(iLocalAcceptorID);
        CertainLogError("Broadcast E(%lu, %lu) st %u iMaxChosenEntry %lu r[%u] %s",
                iEntityID, iEntry, poMachine->GetEntryState(),
                ptEntityInfo->iMaxChosenEntry, iLocalAcceptorID,
                EntryRecordToString(tRecord).c_str());

        BroadcastToRemote(ptInfo, ptInfo->poMachine);
        OSS::ReportTimeoutBroadcast();

        // Wait longer, as broadcast is heavy, and many candidates to reply.
        m_poEntryMng->AddTimeout(ptInfo, 30000);
    }

    return true;
}

int clsEntityWorker::EnterPLogRspQueue(clsCmdBase *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
    clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

    clsPLogRspQueue *poQueue = poQueueMng->GetPLogRspQueue(Hash(iEntityID)
            % poConf->GetEntityWorkerNum());

    clsSmartSleepCtrl oSleepCtrl(200, 1000);

    while (1)
    {
        int iRet = poQueue->PushByMultiThread(poCmd);
        if (iRet == 0)
        {
            break;
        }

        CertainLogError("PushByMultiThread ret %d cmd: %s",
                iRet, poCmd->GetTextCmd().c_str());
        oSleepCtrl.Sleep();
    }

    return 0;
}

int clsEntityWorker::EnterIOReqQueue(clsClientCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();
    clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
    clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

    clsIOReqQueue *poQueue = poQueueMng->GetIOReqQueue(Hash(iEntityID)
            % poConf->GetEntityWorkerNum());

    int iRet = poQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        OSS::ReportIOQueueErr();
        CertainLogError("PushByMultiThread ret %d cmd: %s",
                iRet, poCmd->GetTextCmd().c_str());
        return -1;
    }

    return 0;
}

int clsEntityWorker::RecoverEntityInfo(uint64_t iEntityID, const EntityMeta_t &tMeta)
{
    if (Hash(iEntityID) % m_poConf->GetEntityWorkerNum() != m_iWorkerID)
    {
        return -1;
    }

    EntityInfo_t *ptEntityInfo = m_poEntityMng->FindEntityInfo(iEntityID);
    assert(ptEntityInfo == NULL);

    ptEntityInfo = m_poEntityMng->CreateEntityInfo(iEntityID);
    if (ptEntityInfo == NULL)
    {
        CertainLogFatal("iEntityID %lu", iEntityID);
        return -2;
    }

    // Fix if data corrupt outside.
    if (tMeta.iMaxCommitedEntry > tMeta.iMaxPLogEntry)
    {
        CertainLogFatal("iEntityID %lu Entrys %lu %lu",
                iEntityID, tMeta.iMaxCommitedEntry, tMeta.iMaxPLogEntry);
        return -3;
    }

    ptEntityInfo->iMaxPLogEntry = tMeta.iMaxPLogEntry;
    assert(tMeta.iMaxPLogEntry > 0);

    ptEntityInfo->iMaxChosenEntry = tMeta.iMaxCommitedEntry;

    if (tMeta.bChosen)
    {
        if (ptEntityInfo->iMaxChosenEntry < tMeta.iMaxPLogEntry)
        {
            ptEntityInfo->iMaxChosenEntry = tMeta.iMaxPLogEntry;
        }

        AssertNotEqual(0, tMeta.iValueID);
        if (clsEntryStateMachine::GetAcceptorID(tMeta.iValueID) == ptEntityInfo->iLocalAcceptorID)
        {
            ptEntityInfo->iLocalPreAuthEntry = tMeta.iMaxPLogEntry + 1;
        }
    }

    ptEntityInfo->iMaxContChosenEntry = tMeta.iMaxCommitedEntry;
    ptEntityInfo->iCatchUpEntry = tMeta.iMaxCommitedEntry;
    ptEntityInfo->iNotifyedEntry = tMeta.iMaxCommitedEntry;

    ptEntityInfo->bRangeLoaded = true;

    CertainLogImpt("iEntityID %lu Entrys %lu %lu %lu",
            iEntityID, ptEntityInfo->iMaxContChosenEntry,
            ptEntityInfo->iMaxChosenEntry, ptEntityInfo->iLocalPreAuthEntry);

    return 0;
}

uint32_t clsEntityWorker::GetPeerAcceptorID(EntityInfo_t *ptEntityInfo)
{
    uint32_t iPeerAcceptorID = ptEntityInfo->iLocalAcceptorID + 1;
    if (iPeerAcceptorID >= m_poConf->GetAcceptorNum())
    {
        iPeerAcceptorID = 0;
    }
    return iPeerAcceptorID;
}

} // namespace Certain
