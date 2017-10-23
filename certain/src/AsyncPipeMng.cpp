#include "Command.h"
#include "AsyncPipeMng.h"
#include "certain/Certain.h"

namespace Certain
{

int clsAsyncPipeMng::Init(clsConfigure *poConf)
{
    int iRet;
    m_tIdleIdxList.clear();

    memset(m_aiGroupCnt, 0, sizeof(m_aiGroupCnt));

    for (uint32_t i = 0; i < MAX_ASYNC_PIPE_NUM; ++i)
    {
        iRet = MakeNonBlockPipe(m_aaAsyncPipe[i][0],
                m_aaAsyncPipe[i][1]);
        AssertEqual(iRet, 0);

#if 0
        iRet = SetNonBlock(m_aaAsyncPipe[i][0], false);
        AssertEqual(iRet, 0);
#endif

        m_tIdleIdxList.push_back(i);

        m_aiEntityIDMap[i] = INVALID_ENTITY_ID;
    }

    m_poCertain = clsCertainWrapper::GetInstance();
    m_iMaxGroupLimit = m_poCertain->GetCertainUser()->GetControlGroupLimit();
    CertainLogZero("MAX_ASYNC_PIPE_NUM %u m_iMaxGroupLimit %u",
            MAX_ASYNC_PIPE_NUM, m_iMaxGroupLimit);

    return 0;
}

void clsAsyncPipeMng::Destroy()
{
    // (TODO)rock:
}

int clsAsyncPipeMng::GetIdlePipeIdx(uint32_t &iIdx, uint64_t iEntityID)
{
    clsThreadLock oLock(&m_oMutex);

    int iGroupID = m_poCertain->GetCertainUser()->GetControlGroupID(iEntityID);
    if (iGroupID != -1 && m_aiGroupCnt[iGroupID] >= m_iMaxGroupLimit)
    {
        return eRetCodeNoGroupIdlePipe;
    }

    if (m_tIdleIdxList.empty())
    {
        return eRetCodeNoIdlePipe;
    }

    iIdx = m_tIdleIdxList.front();
    m_tIdleIdxList.pop_front();

    if (iGroupID != -1)
    {
        assert(m_aiEntityIDMap[iIdx] == INVALID_ENTITY_ID);
        m_aiEntityIDMap[iIdx] = iEntityID;
        m_aiGroupCnt[iGroupID]++;
    }

    return 0;
}

void clsAsyncPipeMng::PutIdlePipeIdx(uint32_t iIdx)
{
    clsThreadLock oLock(&m_oMutex);
    m_tIdleIdxList.push_back(iIdx);

    uint64_t iEntityID = m_aiEntityIDMap[iIdx];
    if (iEntityID != INVALID_ENTITY_ID)
    {
        m_aiEntityIDMap[iIdx] = INVALID_ENTITY_ID;
        int iGroupID = m_poCertain->GetCertainUser()->GetControlGroupID(iEntityID);
        assert(iGroupID != -1);
        assert(m_aiGroupCnt[iGroupID] > 0);
        m_aiGroupCnt[iGroupID]--;
    }
}

// iPtr is for check only.
int clsAsyncPipeMng::SyncWriteByPipeIdx(uint32_t iIdx, uintptr_t iPtr)
{
    CertainLogDebug("iIdx %u iPtr %lu", iIdx, iPtr);
    AssertLess(iIdx, MAX_ASYNC_PIPE_NUM);

    int iOutFD = m_aaAsyncPipe[iIdx][1];
    int iRet = write(iOutFD, &iPtr, sizeof(iPtr));
    AssertEqual(iRet, sizeof(iPtr));
    return 0;
}

int clsAsyncPipeMng::SyncWaitByPipeIdx(uint32_t iIdx, uintptr_t iPtr)
{
    CertainLogDebug("iIdx %u iPtr %lu", iIdx, iPtr);
    int iInFD = m_aaAsyncPipe[iIdx][0];

    struct pollfd PollFd; 
    PollFd.events = POLLIN | POLLERR;
    PollFd.fd = iInFD;

    int iEventCnt = 0;
    int iTimeout = 1000;
    int iTimeoutCnt = 0;

    while (true)
    {
        errno = 0;
        iEventCnt = poll(&PollFd, 1, iTimeout);
        if (errno == EINTR)
        {
            continue;
        }
        else if (iEventCnt > 0)
        {
            break;
        }
        else
        {
            iTimeoutCnt++;
            OSS::ReportPollTimeout();

            // It won't be timeout forever.
            CertainLogError("iPtr %lu PipeIdx %u iTimeoutCnt %d",
                    iPtr, iIdx, iTimeoutCnt);
        }
    }

    if (iEventCnt <= 0)
    {
        return -1;
    }

    uintptr_t iRetPtr;
    int iRet = read(iInFD, &iRetPtr, sizeof(iRetPtr));
    AssertEqual(iRet, sizeof(iRetPtr));

    if (iRetPtr != iPtr)
    {
        CertainLogFatal("BUG %lu %lu", iRetPtr, iPtr);
        return eRetCodePipePtrErr;
    }

    return 0;
}

} // namespace Certain
