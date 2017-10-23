#include "certain/Certain.h"
#include "EntityInfoMng.h"
#include "EntryState.h"

namespace Certain
{

void clsMemCacheCtrl::UpdateMaxSize()
{
    if (m_iMaxMemCacheSizeMB != m_poConf->GetMaxMemCacheSizeMB())
    {
        uint64_t iOldMaxSize = m_iMaxSize;
        m_iMaxMemCacheSizeMB = m_poConf->GetMaxMemCacheSizeMB();
        m_iMaxSize = uint64_t(m_iMaxMemCacheSizeMB) * (1 << 20) / m_poConf->GetEntityWorkerNum();
        if (m_iMaxSize < (1 << 20))
        {
            CertainLogFatal("m_iMaxMemCacheSizeMB %u m_iMaxSize %lu too small",
                    m_iMaxMemCacheSizeMB, m_iMaxSize);
            m_iMaxSize = (1 << 20);
        }

        CertainLogImpt("m_iMaxSize %lu -> %lu", iOldMaxSize, m_iMaxSize);
    }
}

void clsMemCacheCtrl::RemoveFromTotalSize(EntryInfo_t *ptInfo)
{
    if (ptInfo == NULL)
    {
        CertainLogFatal("ptInfo == NULL");
        return;
    }

    m_iTotalSize -= ptInfo->iEntrySize;
    ptInfo->iEntrySize = 0;
}

void clsMemCacheCtrl::UpdateTotalSize(EntryInfo_t *ptInfo)
{
    if (ptInfo == NULL)
    {
        CertainLogFatal("ptInfo == NULL");
        return;
    }

    uint32_t iOldSize = ptInfo->iEntrySize;

    uint32_t iNewSize = 0;

    if (ptInfo->poMachine != NULL)
    {
        iNewSize += ptInfo->poMachine->CalcSize();
    }

    for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
    {
        if (ptInfo->apWaitingMsg[i] != NULL)
        {
            iNewSize += ptInfo->apWaitingMsg[i]->CalcSize();
        }
    }

    m_iTotalSize -= iOldSize;
    m_iTotalSize += iNewSize;

    ptInfo->iEntrySize = iNewSize;
}

void clsMemCacheCtrl::RemoveFromTotalSize(EntityInfo_t *ptEntityInfo)
{
    if (ptEntityInfo == NULL)
    {
        CertainLogFatal("ptEntityInfo == NULL");
        return;
    }

    m_iTotalSize -= ptEntityInfo->iWaitingSize;
    ptEntityInfo->iWaitingSize = 0;
}

void clsMemCacheCtrl::UpdateTotalSize(EntityInfo_t *ptEntityInfo)
{
    if (ptEntityInfo == NULL)
    {
        CertainLogFatal("ptEntityInfo == NULL");
        return;
    }

    uint32_t iOldSize = ptEntityInfo->iWaitingSize;

    uint32_t iNewSize = 0;
    for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
    {
        if (ptEntityInfo->apWaitingMsg[i] != NULL)
        {
            iNewSize += ptEntityInfo->apWaitingMsg[i]->CalcSize();
        }
    }

    m_iTotalSize -= iOldSize;
    m_iTotalSize += iNewSize;

    ptEntityInfo->iWaitingSize = iNewSize;
}

bool clsMemCacheCtrl::IsOverLoad(bool bReport)
{
    UpdateMaxSize();
    if (m_iTotalSize >= m_iMaxSize)
    {
        if (bReport)
        {
            OSS::ReportMemLimited();
        }
        return true;
    }
    return false;
}

bool clsMemCacheCtrl::IsAlmostOverLoad()
{
    UpdateMaxSize();

    if (m_iTotalSize >= m_iMaxSize)
    {
        return true;
    }

    uint64_t iRemainSize = m_iMaxSize - m_iTotalSize;

    // check if remain less than 1/32(~3%) mem
    return (iRemainSize << 5) <= m_iMaxSize;
}

clsSmallEntityInfoTable::clsSmallEntityInfoTable(uint32_t iMaxEntityID, uint32_t iAcceptorNum)
{
    m_iMaxEntityID = iMaxEntityID;
    m_ptEntityInfo = (EntityInfo_t *)calloc(iMaxEntityID,
            sizeof(EntityInfo_t));
    Assert(m_ptEntityInfo != NULL);

    m_ptWaitingMsg = (clsPaxosCmd**)calloc(iMaxEntityID * iAcceptorNum, sizeof(clsPaxosCmd*));
    uint32_t iSize = iAcceptorNum * sizeof(clsPaxosCmd*);
    clsPaxosCmd** ptBase = m_ptWaitingMsg;
    for (uint32_t i = 0; i < iMaxEntityID; ++i)
    {
        m_ptEntityInfo[i].apWaitingMsg = ptBase;
        ptBase += iSize;
    }
}

clsSmallEntityInfoTable::~clsSmallEntityInfoTable()
{
    free(m_ptWaitingMsg), m_ptWaitingMsg = NULL;
    free(m_ptEntityInfo), m_ptEntityInfo = NULL;
}

void clsSmallEntityInfoTable::Add(uint64_t iEntityID,
        EntityInfo_t *ptEntityInfo)
{
    CertainLogFatal("It's no need call this function for small.");
    Assert(false);
}

EntityInfo_t *clsSmallEntityInfoTable::Find(uint64_t iEntityID)
{
    AssertLess(iEntityID, m_iMaxEntityID);
    return &m_ptEntityInfo[iEntityID];
}

void clsSmallEntityInfoTable::Remove(uint64_t iEntityID)
{
    CertainLogFatal("It's no need call this function for small.");
    Assert(false);
}

void clsSmallEntityInfoTable::SetMaxSize(uint32_t iMaxSize)
{
    Assert(false);
}

bool clsSmallEntityInfoTable::CheckForEliminate(EntityInfo_t *&ptEntityInfo)
{
    return false;
}

bool clsSmallEntityInfoTable::Refresh(uint64_t iEntityID)
{
    return false;
}

clsLargeEntityInfoTable::clsLargeEntityInfoTable(uint32_t iMaxEntityNum)
{
    m_iMaxEntityNum = iMaxEntityNum;
    bool bAutoEliminate = false;
    m_poLRUTable = new clsLRUTable<uint64_t, EntityInfo_t *>(
            iMaxEntityNum, bAutoEliminate);
}

clsLargeEntityInfoTable::~clsLargeEntityInfoTable()
{
    delete m_poLRUTable, m_poLRUTable = NULL;
}

void clsLargeEntityInfoTable::Add(uint64_t iEntityID,
        EntityInfo_t *ptEntityInfo)
{
    AssertEqual(m_poLRUTable->Add(iEntityID, ptEntityInfo), true);
}

EntityInfo_t *clsLargeEntityInfoTable::Find(uint64_t iEntityID)
{
    EntityInfo_t *ptEntityInfo = NULL;
    m_poLRUTable->Find(iEntityID, ptEntityInfo);
    return ptEntityInfo;
}

void clsLargeEntityInfoTable::Remove(uint64_t iEntityID)
{
    AssertEqual(m_poLRUTable->Remove(iEntityID), true);
}

void clsLargeEntityInfoTable::SetMaxSize(uint32_t iMaxSize)
{
    m_poLRUTable->SetMaxSize(iMaxSize);
}

bool clsLargeEntityInfoTable::CheckForEliminate(EntityInfo_t *&ptEntityInfo)
{
    if (!m_poLRUTable->IsOverLoad())
    {
        return false;
    }

    ptEntityInfo = NULL;
    uint64_t iEntityID = -1;
    if (!m_poLRUTable->PeekOldest(iEntityID, ptEntityInfo))
    {
        return false;
    }

    AssertEqual(iEntityID, ptEntityInfo->iEntityID);

    return true;
}

bool clsLargeEntityInfoTable::Refresh(uint64_t iEntityID)
{
    return m_poLRUTable->Refresh(iEntityID);
}

clsEntityInfoMng::~clsEntityInfoMng()
{
    delete m_poEntityInfoTable, m_poEntityInfoTable = NULL;
}

clsEntityInfoMng::clsEntityInfoMng(clsConfigure *poConf,
        uint32_t iEntityWorkerID)
{
    m_poConf = poConf;
    m_iEntityWorkerID = iEntityWorkerID;

    m_iAcceptorNum = m_poConf->GetAcceptorNum();

    m_iCreateCnt = 0;
    m_iDestroyCnt = 0;

    EntityInfo_t *ptEntityInfo = NULL;

    if (MAX_SMALL_ENTITY_NUM != 0
            && m_poConf->GetMaxEntityNum() <= MAX_SMALL_ENTITY_NUM)
    {
        m_iMaxEntityNum = m_poConf->GetMaxEntityNum();

        m_poEntityInfoTable = new clsSmallEntityInfoTable(m_iMaxEntityNum, m_iAcceptorNum);

        for (uint32_t i = 0; i < m_iMaxEntityNum; ++i)
        {
            ptEntityInfo = m_poEntityInfoTable->Find(i);
            AssertNotEqual(ptEntityInfo, NULL);
            ptEntityInfo->iEntityID = i;
        }
    }
    else
    {
        m_iMaxEntityNum = m_poConf->GetMaxMemEntityNum()
            / m_poConf->GetEntityWorkerNum();
        Assert(m_iMaxEntityNum > 0);

        CertainLogInfo("m_iMaxEntityNum %u", m_iMaxEntityNum);

        m_poEntityInfoTable = new clsLargeEntityInfoTable(m_iMaxEntityNum);
    }

    m_poCertainUser = clsCertainWrapper::GetInstance()->GetCertainUser();
}

uint64_t clsEntityInfoMng::GenerateValueID(EntityInfo_t *ptEntityInfo,
        uint32_t iProposalNum)
{
    // value_id = [time:16][auto_incr:16][proposal_num:32]

    // It's guaranteed that iProposalNum is unique.
    // In case of data loss, iLocalUID is useful.

    uint64_t iLocalUID = ++ptEntityInfo->iValueIDGenerator;

    Assert(iProposalNum > 0);
    uint64_t iValueID = ((iLocalUID << 32) | iProposalNum);

    return iValueID;
}

EntityInfo_t *clsEntityInfoMng::FindEntityInfo(uint64_t iEntityID)
{
    return m_poEntityInfoTable->Find(iEntityID);
}

EntityInfo_t *clsEntityInfoMng::CreateEntityInfo(uint64_t iEntityID)
{
    uint32_t iLocalAcceptorID = INVALID_ACCEPTOR_ID;
    int iRet = m_poCertainUser->GetLocalAcceptorID(iEntityID, iLocalAcceptorID);
    if (iRet != 0)
    {
        // Check if route error.
        CertainLogFatal("iEntityID %lu GetLocalAcceptorID ret %d", iEntityID, iRet);
        return NULL;
    }
    AssertNotEqual(iLocalAcceptorID, INVALID_ACCEPTOR_ID);

    EntityInfo_t *ptEntityInfo = NULL;
    ptEntityInfo = (EntityInfo_t *)calloc(1, sizeof(EntityInfo_t));
    AssertNotEqual(ptEntityInfo, NULL);

    ptEntityInfo->apWaitingMsg = (clsPaxosCmd**)calloc(m_iAcceptorNum, sizeof(clsPaxosCmd*));
    AssertNotEqual(ptEntityInfo->apWaitingMsg, NULL);

    ptEntityInfo->poLeasePolicy = new clsLeasePolicy(
            iLocalAcceptorID, m_poConf->GetLeaseDurationMS());

    ptEntityInfo->iEntityID = iEntityID;
    ptEntityInfo->iMaxPLogEntry = INVALID_ENTRY;

    ptEntityInfo->iValueIDGenerator = GetCurrTime() % (1 << 16);
    ptEntityInfo->iValueIDGenerator <<= 16;

    ptEntityInfo->iRefCount = 1;

    // Update the iMaxChosenEntry before Add into Table,
    // since thread other than EntityWorker may access it.

    ptEntityInfo->iLocalAcceptorID = iLocalAcceptorID;

    if (ptEntityInfo->iLocalAcceptorID == 0)
    {
        ptEntityInfo->iLocalPreAuthEntry = 1;
    }

    CIRCLEQ_INIT(EntryInfo_t, &ptEntityInfo->tEntryList);

    {
        clsThreadWriteLock oWriteLock(&m_oRWLock);
        m_poEntityInfoTable->Add(ptEntityInfo->iEntityID, ptEntityInfo);
    }

    m_iCreateCnt++;

    if (m_iCreateCnt % 100 == 0)
    {
        CertainLogImpt("worker %u create %lu destroy %lu delta %lu",
                m_iEntityWorkerID, m_iCreateCnt, m_iDestroyCnt,
                m_iCreateCnt - m_iDestroyCnt);
    }

    OSS::ReportEntityCreate();

    return ptEntityInfo;
}

void clsEntityInfoMng::DestroyEntityInfo(EntityInfo_t *ptEntityInfo)
{
    CertainLogError("iEntityID %lu iRefCount %d",
            ptEntityInfo->iEntityID, ptEntityInfo->iRefCount);

    ptEntityInfo->iRefCount--;
    AssertEqual(ptEntityInfo->iRefCount, 0);

    AssertEqual(ptEntityInfo->poClientCmd, NULL);
    Assert(!ptEntityInfo->bRangeLoading);

    m_poMemCacheCtrl->RemoveFromTotalSize(ptEntityInfo);

    for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
    {
        if (ptEntityInfo->apWaitingMsg[i] != NULL)
        {
            delete ptEntityInfo->apWaitingMsg[i];
            ptEntityInfo->apWaitingMsg[i] = NULL;
        }
    }

    {
        clsThreadWriteLock oWriteLock(&m_oRWLock);
        m_poEntityInfoTable->Remove(ptEntityInfo->iEntityID);
    }

    delete ptEntityInfo->poLeasePolicy, ptEntityInfo->poLeasePolicy = NULL;

    free(ptEntityInfo->apWaitingMsg), ptEntityInfo->apWaitingMsg = NULL;
    free(ptEntityInfo), ptEntityInfo = NULL;

    m_iDestroyCnt++;

    if (m_iDestroyCnt % 100 == 0 || m_iCreateCnt == m_iDestroyCnt)
    {
        CertainLogImpt("worker %u create %lu destroy %lu delta %lu",
                m_iEntityWorkerID, m_iCreateCnt, m_iDestroyCnt,
                m_iCreateCnt - m_iDestroyCnt);
    }

    OSS::ReportEntityDestroy();
}

void clsEntityInfoMng::RefEntityInfo(EntityInfo_t *ptEntityInfo)
{
    CertainLogInfo("iEntityID %lu ptEntityInfo %p",
            ptEntityInfo->iEntityID, ptEntityInfo);

    ptEntityInfo->iRefCount++;
}

void clsEntityInfoMng::UnRefEntityInfo(EntityInfo_t *ptEntityInfo)
{
    CertainLogInfo("iEntityID %lu ptEntityInfo %p",
            ptEntityInfo->iEntityID, ptEntityInfo);

    ptEntityInfo->iRefCount--;
    AssertNotMore(0, ptEntityInfo->iRefCount);
}

int clsEntityInfoMng::GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo)
{
    EntityInfo_t *ptEntityInfo = NULL;

    {
        clsThreadReadLock oReadLock(&m_oRWLock);
        ptEntityInfo = m_poEntityInfoTable->Find(iEntityID);

        if (ptEntityInfo == NULL)
        {
            return eRetCodeNotFound;
        }
        else
        {
            tEntityInfo = *ptEntityInfo;
        }
    }

    return 0;
}

int clsEntityInfoMng::GetMaxChosenEntry(uint64_t iEntityID,
        uint64_t &iMaxContChosenEntry, uint64_t &iMaxChosenEntry)
{
    EntityInfo_t *ptEntityInfo = NULL;

    {
        clsThreadReadLock oReadLock(&m_oRWLock);
        ptEntityInfo = m_poEntityInfoTable->Find(iEntityID);

        if (ptEntityInfo == NULL)
        {
            return eRetCodeNotFound;
        }
        else
        {
            iMaxContChosenEntry = ptEntityInfo->iMaxContChosenEntry;
            iMaxChosenEntry = ptEntityInfo->iMaxChosenEntry;
        }
    }

    return 0;
}

int clsEntityInfoMng::GetMaxChosenEntry(uint64_t iEntityID,
        uint64_t &iMaxContChosenEntry, uint64_t &iMaxChosenEntry,
        uint64_t &iLeaseTimeoutMS)
{
    int32_t iRefCount = 0;
    EntityInfo_t *ptEntityInfo = NULL;
    iLeaseTimeoutMS = 0;

    {
        clsThreadReadLock oReadLock(&m_oRWLock);
        ptEntityInfo = m_poEntityInfoTable->Find(iEntityID);

        if (ptEntityInfo == NULL
                || ptEntityInfo->iMaxPLogEntry == INVALID_ENTRY)
        {
            return eRetCodeNotFound;
        }
        else
        {
            iRefCount = ptEntityInfo->iRefCount;
            iMaxContChosenEntry = ptEntityInfo->iMaxContChosenEntry;
            iMaxChosenEntry = ptEntityInfo->iMaxChosenEntry;
            iLeaseTimeoutMS = ptEntityInfo->poLeasePolicy->GetLeaseTimeoutMS();
        }
    }

    if (iLeaseTimeoutMS == clsLeasePolicy::kUnlimitedMS)
    {
        return eRetCodeLeaseReject;
    }

    // All entrys of the entity may eliminated, recover it.
    if (iRefCount == 1 && iMaxContChosenEntry < iMaxChosenEntry)
    {
        CertainLogError("iEntityID %lu entrys: %lu %lu",
                iEntityID, iMaxContChosenEntry, iMaxChosenEntry);
        return eRetCodeNotFound;
    }

    return 0;
}

EntityInfo_t *clsEntityInfoMng::PeekOldest()
{
    EntityInfo_t *ptEntityInfo = NULL;
    if (!m_poEntityInfoTable->CheckForEliminate(ptEntityInfo))
    {
        return NULL;
    }
    return ptEntityInfo;
}

bool clsEntityInfoMng::Refresh(EntityInfo_t *ptEntityInfo)
{
    return m_poEntityInfoTable->Refresh(ptEntityInfo->iEntityID);
}

bool clsEntityInfoMng::CheckAndEliminate()
{
    EntityInfo_t *ptEntityInfo = NULL;

    uint32_t iMaxSizePerWorker = m_poConf->GetMaxMemEntityNum()
        / m_poConf->GetEntityWorkerNum();

    if (iMaxSizePerWorker == 0)
    {
        AssertLess(0, iMaxSizePerWorker);
        iMaxSizePerWorker = 1;
    }

    m_poEntityInfoTable->SetMaxSize(iMaxSizePerWorker);

    if (!m_poEntityInfoTable->CheckForEliminate(ptEntityInfo))
    {
        return true;
    }

    AssertNotMore(1, ptEntityInfo->iRefCount);

    if (ptEntityInfo->bRangeLoading || ptEntityInfo->bGetAllPending)
    {
        CertainLogFatal("iEntityID %lu iRefCount %d bRangeLoading %u bGetAllPending %u",
                ptEntityInfo->iEntityID, ptEntityInfo->iRefCount,
                ptEntityInfo->bRangeLoading, ptEntityInfo->bGetAllPending);
        return false;
    }

    // Be closed-loop with CreateEntityInfo.
    if (ptEntityInfo->iRefCount == 1)
    {
        AssertEqual(ptEntityInfo->poClientCmd, NULL);
        DestroyEntityInfo(ptEntityInfo);
        return true;
    }

    CertainLogInfo("ptEntityInfo->iRefCount %d", ptEntityInfo->iRefCount);

    return false;
}

int clsEntityGroupMng::Init(clsConfigure *poConf)
{
    m_poConf = poConf;
    memset(m_apEntityMng, 0, sizeof(m_apEntityMng));
    return 0;
}

void clsEntityGroupMng::AddEntityInfoMng(uint32_t iEntityWorkerID,
        clsEntityInfoMng *poEntityMng)
{
    AssertEqual(m_apEntityMng[iEntityWorkerID], NULL);
    m_apEntityMng[iEntityWorkerID] = poEntityMng;
}

int clsEntityGroupMng::GetMaxChosenEntry(uint64_t iEntityID,
        uint64_t &iMaxContChosenEntry, uint64_t &iMaxChosenEntry)
{
    uint32_t iWorkerID = Hash(iEntityID) % m_poConf->GetEntityWorkerNum();
    return m_apEntityMng[iWorkerID]->GetMaxChosenEntry(iEntityID,
            iMaxContChosenEntry, iMaxChosenEntry);
}

int clsEntityGroupMng::GetMaxChosenEntry(uint64_t iEntityID,
        uint64_t &iMaxContChosenEntry, uint64_t &iMaxChosenEntry,
        uint64_t &iLeaseTimeoutMS)
{
    uint32_t iWorkerID = Hash(iEntityID) % m_poConf->GetEntityWorkerNum();
    return m_apEntityMng[iWorkerID]->GetMaxChosenEntry(iEntityID,
            iMaxContChosenEntry, iMaxChosenEntry, iLeaseTimeoutMS);
}

int clsEntityGroupMng::GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo)
{
    uint32_t iWorkerID = Hash(iEntityID) % m_poConf->GetEntityWorkerNum();
    return m_apEntityMng[iWorkerID]->GetEntityInfo(iEntityID, tEntityInfo);
    return 0;
}

clsAutoEntityLock::clsAutoEntityLock(uint64_t iEntityID)
{
    m_pLockInfo = NULL;
    m_iEntityID = iEntityID;

    clsCertainWrapper *po = clsCertainWrapper::GetInstance();
    po->GetCertainUser()->LockEntity(m_iEntityID, &m_pLockInfo);
}

clsAutoEntityLock::~clsAutoEntityLock()
{
    clsCertainWrapper *po = clsCertainWrapper::GetInstance();
    po->GetCertainUser()->UnLockEntity(m_pLockInfo);
}

clsAutoPLogEntityLock::clsAutoPLogEntityLock(uint64_t iEntityID)
{
    m_pLockInfo = NULL;
    m_iEntityID = iEntityID;

    clsCertainWrapper *po = clsCertainWrapper::GetInstance();
    po->GetCertainUser()->LockPLogEntity(m_iEntityID, &m_pLockInfo);
}

clsAutoPLogEntityLock::~clsAutoPLogEntityLock()
{
    clsCertainWrapper *po = clsCertainWrapper::GetInstance();
    po->GetCertainUser()->UnLockPLogEntity(m_pLockInfo);
}

} // namespace Certain
