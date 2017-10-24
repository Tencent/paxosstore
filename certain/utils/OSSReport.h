#ifndef CERTAIN_OSSREPORT_H_
#define CERTAIN_OSSREPORT_H_

#include "utils/Header.h"
//#include "iOssAttr.h"

inline void OssAttrInc(int iKey, int iID, int iCnt)
{
    // For report.
}

#define CERTAIN_OSS_DEFAULT_ID_KEY      29878

// 0 ~ 5
#define CERTAIN_OSS_RUN_PAXOS           0
#define CERTAIN_OSS_CHECK_EMPTY         6
#define CERTAIN_OSS_PAXOS_FOR_READ      7
#define CERTAIN_OSS_PAXOS_FOR_WRITE     8

#define CERTAIN_OSS_PLOG_PUT            10
#define CERTAIN_OSS_GET_META_KEY        20
#define CERTAIN_OSS_PLOG_GET            30
#define CERTAIN_OSS_PLOG_RANGE_LOAD     40
#define CERTAIN_OSS_PLOG_GET_VALUE      50

#define CERTAIN_OSS_EXTRA_PLOG_GET      60

#define CERTAIN_OSS_MEM_LIMITED         67
#define CERTAIN_OSS_CHECK_GET_ALL       68
#define CERTAIN_OSS_GET_ALL_FAIL        69
#define CERTAIN_OSS_DB_COMMIT           70
#define CERTAIN_OSS_GET_ALL_REQ         76
#define CERTAIN_OSS_GET_ALL_RSP         77
#define CERTAIN_OSS_LEASE_WAIT          78
#define CERTAIN_OSS_LEASE_REJECT        79

// '1 ~ 10' --> '80 ~ 89', '>= 10' --> 90
#define CERTAIN_OSS_BATCH_CATCH_UP      80
#define CERTAIN_OSS_SINGLE_CATCH_UP     91
#define CERTAIN_OSS_ENTITY_LIMITED      92
#define CERTAIN_OSS_MID_STATE_ELIM      93
#define CERTAIN_OSS_CHOSEN_ELIM         94
#define CERTAIN_OSS_EMPTY_ELIM          95
#define CERTAIN_OSS_ELIM_FAILED         96
#define CERTAIN_OSS_MID_ELIM_FAILED     97
#define CERTAIN_OSS_TIMEOUT_BROADCAST   98
#define CERTAIN_OSS_FAST_FAIL           99

#define CERTAIN_OSS_CHOSEN_N            100

#define CERTAIN_OSS_DB_QUEUE_ERR        110
#define CERTAIN_OSS_PLOG_QUEUE_ERR      111
#define CERTAIN_OSS_IO_QUEUE_ERR        112
#define CERTAIN_OSS_GET_ALL_QUEUE_ERR   113
#define CERTAIN_OSS_CATCH_UP_QUEUE_ERR  114
#define CERTAIN_OSS_PLOG_WR_QUEUE_ERR   115

#define CERTAIN_OSS_ENTITY_CREATE       116
#define CERTAIN_OSS_ENTITY_DESTROY      117
#define CERTAIN_OSS_MULTI_CMD           118
#define CERTAIN_OSS_MULTI_CMD_INNER     119
#define CERTAIN_OSS_POLL_TIMEOUT        120
#define CERTAIN_OSS_NEW_PAXOS_CMD       121
#define CERTAIN_OSS_FREE_PAXOS_CMD      122
#define CERTAIN_OSS_MAY_NEED_FIX        123
#define CERTAIN_OSS_GET_AND_COMMIT      124
#define CERTAIN_OSS_EVICT_SUCC          125
#define CERTAIN_OSS_EVICT_FAIL          126

#define CERTAIN_OSS_FATAL_ERR           127

extern uint32_t s_iCertainOSSIDKey;

namespace Certain
{

namespace OSS
{

void SetCertainOSSIDKey(uint32_t iOSSIDKey);

inline void ReportDBQueueErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_DB_QUEUE_ERR, 1);
}

inline void ReportPLogQueueErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_PLOG_QUEUE_ERR, 1);
}

inline void ReportIOQueueErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_IO_QUEUE_ERR, 1);
}

inline void ReportGetAllQueueErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_GET_ALL_QUEUE_ERR, 1);
}

inline void ReportCatchUpQueueErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_CATCH_UP_QUEUE_ERR, 1);
}

inline void ReportPLogWrQueueErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_PLOG_WR_QUEUE_ERR, 1);
}

inline void ReportEntityCreate()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_ENTITY_CREATE, 1);
}

inline void ReportEntityDestroy()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_ENTITY_DESTROY, 1);
}

inline void ReportMultiCmd()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_MULTI_CMD, 1);
}

inline void ReportMultiCmdInner(uint32_t iInnerCnt)
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_MULTI_CMD_INNER, iInnerCnt);
}

inline void ReportPollTimeout()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_POLL_TIMEOUT, 1);
}

inline void ReportNewPaxosCmd()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_NEW_PAXOS_CMD, 1);
}

inline void ReportFreePaxosCmd()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_FREE_PAXOS_CMD, 1);
}

inline void ReportMayNeedFix()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_MAY_NEED_FIX, 1);
}

inline void ReportGetAndCommit()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_GET_AND_COMMIT, 1);
}

inline void ReportEvictSucc()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EVICT_SUCC, 1);
}

inline void ReportEvictFail()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EVICT_FAIL, 1);
}

inline void ReportFatalErr()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_FATAL_ERR, 1);
}

inline void ReportUseTimeMS(int iID, int iRet, uint64_t iUseTimeMS)
{

    if (iRet != 0)
    {
        OssAttrInc(s_iCertainOSSIDKey, iID + 5, 1);
    }
    else if (iUseTimeMS <= 10)
    {
        OssAttrInc(s_iCertainOSSIDKey, iID + 0, 1);
    }
    else if (iUseTimeMS <= 30)
    {
        OssAttrInc(s_iCertainOSSIDKey, iID + 1, 1);
    }
    else if (iUseTimeMS <= 100)
    {
        OssAttrInc(s_iCertainOSSIDKey, iID + 2, 1);
    }
    else if (iUseTimeMS <= 300)
    {
        OssAttrInc(s_iCertainOSSIDKey, iID + 3, 1);
    }
    else
    {
        OssAttrInc(s_iCertainOSSIDKey, iID + 4, 1);
    }
}

inline void ReportRunPaxosTimeMS(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_RUN_PAXOS, iRet, iUseTimeMS);
}

inline void ReportCheckEmpty()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_CHECK_EMPTY, 1);
}

inline void ReportPaxosForRead()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_PAXOS_FOR_READ, 1);
}

inline void ReportPaxosForWrite()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_PAXOS_FOR_WRITE, 1);
}

inline void ReportPLogPutTimeMS(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_PLOG_PUT, iRet, iUseTimeMS);
}

inline void ReportPLogGetMetaKeyTimeMS(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_GET_META_KEY, iRet, iUseTimeMS);
}

inline void ReportPLogGetTimeMS(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_PLOG_GET, iRet, iUseTimeMS);
}

inline void ReportPLogRangeLoadTimeMS(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_PLOG_RANGE_LOAD, iRet, iUseTimeMS);
}

inline void ReportPLogGetValueTimeMS(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_PLOG_GET_VALUE, iRet, iUseTimeMS);
}

inline void ReportBatchCatchUp(uint32_t iNum)
{
    if (iNum == 0)
    {
        return;
    }

    if (iNum <= 10)
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_BATCH_CATCH_UP + iNum - 1, 1);
    }
    else
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_BATCH_CATCH_UP + 10, 1);
    }
}

inline void ReportSingleCatchUp()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_SINGLE_CATCH_UP, 1);
}

inline void ReportMidStateElim()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_MID_STATE_ELIM, 1);
}

inline void ReportEntityLimited()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_ENTITY_LIMITED, 1);
}

inline void ReportMemLimited()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_MEM_LIMITED, 1);
}

inline void ReportCheckGetAll()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_CHECK_GET_ALL, 1);
}

inline void ReportGetAllFail()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_GET_ALL_FAIL, 1);
}

inline void ReportGetAllReq()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_GET_ALL_REQ, 1);
}

inline void ReportGetAllRsp()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_GET_ALL_RSP, 1);
}

inline void ReportLeaseWait()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_LEASE_WAIT, 1);
}

inline void ReportLeaseReject()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_LEASE_REJECT, 1);
}

inline void ReportChosenElim()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_CHOSEN_ELIM, 1);
}

inline void ReportEmptyElim()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EMPTY_ELIM, 1);
}

inline void ReportElimFailed()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_ELIM_FAILED, 1);
}

inline void ReportMidElimFailed()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_MID_ELIM_FAILED, 1);
}

inline void ReportTimeoutBroadcast()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_TIMEOUT_BROADCAST, 1);
}

inline void ReportFastFail()
{
    OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_FAST_FAIL, 1);
}

inline void ReportChosenProposalNum(uint64_t iProposalNum)
{
    assert(iProposalNum != 0);

    if (iProposalNum <= 9)
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_CHOSEN_N + iProposalNum - 1, 1);
    }
    else
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_CHOSEN_N + 9, 1);
    }
}

inline void ReportExtraPLogGet(uint32_t iExtraPLogGetCnt)
{
    if (iExtraPLogGetCnt < 3)
    {
        // 0, 1, 2
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EXTRA_PLOG_GET + iExtraPLogGetCnt, 1);
    }
    else if (iExtraPLogGetCnt <= 10)
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EXTRA_PLOG_GET + 3, 1);
    }
    else if (iExtraPLogGetCnt <= 50)
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EXTRA_PLOG_GET + 4, 1);
    }
    else
    {
        OssAttrInc(s_iCertainOSSIDKey, CERTAIN_OSS_EXTRA_PLOG_GET + 5, 1);
    }
}

inline void ReportDBCommit(int iRet, uint64_t iUseTimeMS)
{
    ReportUseTimeMS(CERTAIN_OSS_DB_COMMIT, iRet, iUseTimeMS);
}

}; // namespace OSS

}; // namespace Certain

#endif
