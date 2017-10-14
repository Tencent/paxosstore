#ifndef CERTAIN_ENTRYSTATE_H_
#define CERTAIN_ENTRYSTATE_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

enum enumEntryState
{
    kEntryStateNormal = 0,
    kEntryStatePromiseLocal,
    kEntryStatePromiseRemote,
    kEntryStateMajorityPromise,
    kEntryStateAcceptRemote,
    kEntryStateAcceptLocal,
    kEntryStateChosen
};

class clsEntryStateMachine
{
public:
    static uint32_t s_iAcceptorNum;
    static uint32_t s_iMajorityNum;
    static uint32_t GetAcceptorID(uint64_t iValueID);

private:
    int m_iEntryState;

    uint32_t m_iMaxPreparedNum;

    uint32_t m_iMostAcceptedNum;
    uint32_t m_iMostAcceptedNumCnt;

    std::vector<EntryRecord_t> m_atRecord;

    uint32_t CountAcceptedNum(uint32_t iAcceptedNum);
    uint32_t CountPromisedNum(uint32_t iPromisedNum);

    int CalcEntryState(uint32_t iLocalAcceptorID);

    int MakeRealRecord(EntryRecord_t &tRecord);

    void UpdateMostAcceptedNum(const EntryRecord_t &tRecord);
    bool GetValueByAcceptedNum(uint32_t iAcceptedNum,
            PaxosValue_t &tValue);

public:
    static int Init(clsConfigure *poConf);

    clsEntryStateMachine()
    {
        m_iEntryState = kEntryStateNormal;

        m_iMaxPreparedNum = 0;

        m_iMostAcceptedNum = 0;
        m_iMostAcceptedNumCnt = 0;

        m_atRecord.resize(s_iAcceptorNum);
        for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
        {
            InitEntryRecord(&m_atRecord[i]);
        }
    }

    ~clsEntryStateMachine() { }

    int GetEntryState() { return m_iEntryState; }

    uint32_t GetNextPreparedNum(uint32_t iLocalAcceptorID);

    const EntryRecord_t &GetRecord(uint32_t iAcceptorID);

    int Update(uint64_t iEntityID, uint64_t iEntry,
            uint32_t iLocalAcceptorID, uint32_t iAcceptorID,
            const EntryRecord_t &tRecordMayWithValueIDOnly);

    int AcceptOnMajorityPromise(uint32_t iLocalAcceptorID,
            const PaxosValue_t &tValue, bool &bAcceptPreparedValue);

    void SetStoredValueID(uint32_t iLocalAcceptorID);

    // For readonly cmd.
    void ResetAllCheckedEmpty();
    void SetCheckedEmpty(uint32_t iAcceptorID);
    bool IsLocalEmpty();
    bool IsReadOK();

    bool IsRemoteCompeting();

    string ToString();

    uint32_t CalcSize();
};

} // namespace Certain

#endif
