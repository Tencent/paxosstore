#ifndef CERTAIN_ENTRYINFOMNG_H_
#define CERTAIN_ENTRYINFOMNG_H_

#include "Common.h"
#include "Command.h"

namespace std
{

template<>
    struct hash<Certain::PLogPos_t>
    {
        size_t operator()(const Certain::PLogPos_t& tPos) const
        {
            return Certain::Hash((const char *)&tPos, sizeof(tPos));
        }
    };

} // namespace std

#define CERTAIN_MAX_ENTRY_TIMEOUT_MS 30000 // 30s

namespace Certain
{

class clsConfigure;
class clsCmdBase;
class clsEntryStateMachine;

struct EntityInfo_t;
struct EntryInfo_t
{
    EntityInfo_t *ptEntityInfo;
    uint64_t iEntry;

    // (TODO)rock: maintain in EntityInfo_t
    uint32_t iActiveAcceptorID;
    uint32_t iEntrySize;

    bool bCatchUpFlag;
    bool bUncertain;
    bool bRemoteUpdated;
    bool bBroadcast;
    bool bNotFound;

    clsPaxosCmd **apWaitingMsg;

    clsEntryStateMachine *poMachine;

    // For clsArrayTimer<EntryInfo_t>.
    clsArrayTimer<EntryInfo_t>::TimeoutEntry_t tTimeoutEntry;

    // For tEntryList in EntityInfo
    CIRCLEQ_ENTRY(EntryInfo_t) tListElt;
};

class clsEntityInfoMng;
class clsEntryInfoMng
{
private:
    clsConfigure *m_poConf;
    clsEntityInfoMng *m_poEntityMng;

    typedef clsLRUTable<PLogPos_t, EntryInfo_t *> EntryLRUTable_t;

    EntryLRUTable_t *m_poEntryTable;
    clsArrayTimer<EntryInfo_t> *m_poEntryTimer;

    clsFixSizePool *m_poFixSizePool;

    uint32_t m_iAcceptorNum;

    uint32_t m_iCatchUpFlagCnt;

    // For print log.
    uint64_t m_iCreateCnt;
    uint64_t m_iDestroyCnt;
    uint32_t m_iEntityWorkerID;

public:
    clsEntryInfoMng(clsConfigure *poConf, uint32_t iEntityWorkerID)
        : m_poConf(poConf),
        m_poEntityMng(NULL),
        m_poFixSizePool(NULL),
        m_iAcceptorNum(poConf->GetAcceptorNum()),
        m_iCatchUpFlagCnt(0),
        m_iCreateCnt(0),
        m_iDestroyCnt(0),
        m_iEntityWorkerID(iEntityWorkerID)
    {
        uint32_t iMaxEntryNum = m_poConf->GetMaxMemEntryNum()
            / m_poConf->GetEntityWorkerNum();
        assert(iMaxEntryNum > 0);

        bool bAutoEliminate = false;
        m_poEntryTable = new EntryLRUTable_t(iMaxEntryNum, bAutoEliminate);

        m_poEntryTimer = new clsArrayTimer<EntryInfo_t>(CERTAIN_MAX_ENTRY_TIMEOUT_MS);

        m_poFixSizePool = new clsFixSizePool(iMaxEntryNum, sizeof(EntryInfo_t));

        CertainLogImpt("iMaxEntryNum %u CERTAIN_MAX_ENTRY_TIMEOUT_MS %d",
                iMaxEntryNum, CERTAIN_MAX_ENTRY_TIMEOUT_MS);
    }

    ~clsEntryInfoMng()
    {
        delete m_poEntryTimer, m_poEntryTimer = NULL;
        delete m_poEntryTable, m_poEntryTable = NULL;
        delete m_poFixSizePool, m_poFixSizePool = NULL;
    }

    EntryInfo_t *TakeTimeout();
    void AddTimeout(EntryInfo_t *ptInfo, uint32_t iTimeoutMS);
    void RemoveTimeout(EntryInfo_t *ptInfo);

    bool WaitForTimeout(EntryInfo_t *ptInfo);

    EntryInfo_t *CreateEntryInfo(EntityInfo_t *ptEntityInfo, uint64_t iEntry);
    void DestroyEntryInfo(EntryInfo_t *ptInfo);

    EntryInfo_t *FindEntryInfo(uint64_t iEntityID, uint64_t iEntry);

    void IncreaseEliminatePriority(EntryInfo_t *ptInfo);
    void ReduceEliminatePriority(EntryInfo_t *ptInfo);

    void RemoveCatchUpFlag(EntryInfo_t *ptInfo);
    bool CheckIfCatchUpLimited(EntryInfo_t *ptInfo);

    uint32_t GetEntrySize()
    {
        return m_poEntryTable->Size();
    }

    void SetEntityInfoMng(clsEntityInfoMng *poEntityMng)
    {
        m_poEntityMng = poEntityMng;
    }

    bool IsOverLoad()
    {
        return m_poEntryTable->IsOverLoad();
    }

    bool PeekOldest(PLogPos_t &tPos, EntryInfo_t *&ptInfo)
    {
        return m_poEntryTable->PeekOldest(tPos, ptInfo);
    }
};

} // namespace Certain

#endif
