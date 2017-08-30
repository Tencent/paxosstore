
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <poll.h>
#include <cassert>
#include <cstring>
#include "cutils/mem_utils.h"
#include "cutils/async_worker.h"
#include "cutils/log_utils.h"
#include "comm/kvsvrcomm.h"
#include "universal_batch.h"
#include "db_comm.h"

namespace
{
using namespace dbcomm;

inline void AssertEmptyPipePair(const clsPipeAllocator::PipePair& tPipePair)
{
    assert(-1 == tPipePair.iPipe[0]);
    assert(-1 == tPipePair.iPipe[1]);
}

inline void AssertValidPipePair(const clsPipeAllocator::PipePair& tPipePair)
{
    assert(0 <= tPipePair.iPipe[0]);
    assert(0 <= tPipePair.iPipe[1]);
}

clsUniversalBatch::Node_t* GetFirstNode(clsUniversalBatch::NodeList_t* pList)
{
    return pList->pHead;
}

clsUniversalBatch::Node_t* TakeFirstNotEmptyNode(clsUniversalBatch::NodeList_t* pList)
{
    clsUniversalBatch::Node_t* lpNode = pList->pHead;
    if (!lpNode || !lpNode->iTaskCnt)
    {
        return NULL;
    }
    if (pList->pHead == pList->pTail)
    {
        pList->pHead = pList->pTail = NULL;
    }
    else
    {
        pList->pHead = pList->pHead->pNext;
    }

    lpNode->pNext = NULL;
    return lpNode;
}

clsUniversalBatch::Node_t* TakeFirstNotEmptyNodeLock(clsUniversalBatch::NodeList_t* pList)
{
    std::lock_guard<std::mutex> lock(pList->tMutex);
    clsUniversalBatch::Node_t* lp = TakeFirstNotEmptyNode(pList);
    return lp;
}

void AddToTail(clsUniversalBatch::NodeList_t* pList, clsUniversalBatch::Node_t* pNode)
{
    pNode->pNext = NULL;
    if (!pList->pHead)
    {
        pList->pHead = pList->pTail = pNode;
    }
    else
    {
        pList->pTail->pNext = pNode;
        pList->pTail        = pNode;
    }
}

void FreeList(clsUniversalBatch::NodeList_t* pList)
{
    for (int i = 0; i < pList->iItemCnt; i++)
    {
        clsUniversalBatch::Node_t* lpNode = pList->pItems + i;
        delete[] lpNode->pTask;
    }

    if (pList->pItems)
    {
        free(pList->pItems);
        pList->pItems   = NULL;
        pList->iItemCnt = 0;
    }
}

void InitListWithAssert(clsUniversalBatch::NodeList_t* pList, int iListSize)
{
    memset(pList, 0, sizeof(clsUniversalBatch::NodeList_t));

    pList->pItems =
        (clsUniversalBatch::Node_t*)calloc(1, sizeof(clsUniversalBatch::Node_t) * iListSize);
    pList->iItemCnt = iListSize;

    for (int i = 0; i < pList->iItemCnt; i++)
    {
        clsUniversalBatch::Node_t* lpNode = pList->pItems + i;
        lpNode->pTask = new clsUniversalBatch::Task_t[clsUniversalBatch::MAX_BATCH_SIZE];
        for (int j = 0; j < clsUniversalBatch::MAX_BATCH_SIZE; j++)
        {
            lpNode->pTask[j].iPipe[0] = -1;
            lpNode->pTask[j].iPipe[1] = -1;
            lpNode->pTask[j].ptReq    = NULL;
            lpNode->pTask[j].ptRsp    = NULL;
            lpNode->pTask[j].iRet     = -1;
        }
        AddToTail(pList, lpNode);
    }
}

void MonitorBatchJob(clsUniversalBatch& oBatchHandle, bool& bStop)
{
    // SetThreadTitle("kv_ubat_mon");

    // Deamon::BindCPUForWorker();
    BindWorkerCpu();
    int iLogCnt = 0;
    while (false == bStop)
    {
        oBatchHandle.DoMonitor();
        usleep(1000);  // 1ms timeout
        ++iLogCnt;
        if (0 == (iLogCnt % 10000))
        {
            logerr("INFO: (%s) iWaitTime %d PIPE STAT %zu %zu",
                       oBatchHandle.GetMonitorName().c_str(),
                       oBatchHandle.GetWaitTime(),
                       oBatchHandle.PeekPipeAlloc().GetPipeUsed(),
                       oBatchHandle.PeekPipeAlloc().GetPipeSize());
        }
    }
}

}  // namespace

namespace dbcomm
{
clsUniversalBatch::clsUniversalBatch(
        int iWaitTime,
        std::function<void(clsUniversalBatch::Node_t*)> pfnHandle,
        const std::string& sMonitorName)
    : m_sMonitorName(sMonitorName)
    , m_pfnHandle(pfnHandle)
    , m_tPipeAlloc(500, 200 * MAX_BATCH_SIZE)
    , m_iWaitTime(iWaitTime)
    , m_iBatchCnt(MAX_BATCH_SIZE)
{
    assert(NULL != m_pfnHandle);
    InitListWithAssert(&m_tList, 200);
    printf("%s batch req wait time %d\n", __func__, m_iWaitTime);
}

clsUniversalBatch::~clsUniversalBatch()
{
    FreeList(&m_tList);
}

void clsUniversalBatch::Release(Task_t* pTask)
{
    pTask->ptReq = NULL;
    pTask->ptRsp = NULL;
    pTask->iRet  = -1;
}

void clsUniversalBatch::Release(Node_t* pNode)
{
    std::vector<clsPipeAllocator::PipePair> vecPipePair(pNode->iTaskCnt);
    for (int i = 0; i < pNode->iTaskCnt; i++)
    {
        Release(pNode->pTask + i);
        vecPipePair[i].iPipe[0]  = pNode->pTask[i].iPipe[0];
        vecPipePair[i].iPipe[1]  = pNode->pTask[i].iPipe[1];
        pNode->pTask[i].iPipe[0] = -1;
        pNode->pTask[i].iPipe[1] = -1;
        AssertValidPipePair(vecPipePair[i]);
    }
    pNode->iTaskCnt   = 0;
    pNode->iBatchSize = 0;
    assert(0 == pNode->iRef);

    std::unique_lock<std::mutex> lock(m_tList.tMutex);
    if (false == vecPipePair.empty())
    {
        m_tPipeAlloc.FreePipe(vecPipePair.data(), vecPipePair.size());
    }

    AddToTail(&m_tList, pNode);
    lock.unlock();
}

void clsUniversalBatch::SetBatchCnt(int iBatchCnt)
{
    if (iBatchCnt >= MAX_BATCH_SIZE)
    {
        m_iBatchCnt = MAX_BATCH_SIZE;
    }
    else
    {
        m_iBatchCnt = iBatchCnt;
    }
    printf("-----------------BatchCnt %d\n", m_iBatchCnt);
}

void clsUniversalBatch::SetWaitTime(int iWaitTime)
{
    int iPrevWaitTime = m_iWaitTime;
    while (!__sync_bool_compare_and_swap(&m_iWaitTime, iPrevWaitTime, iWaitTime))
    {
        iPrevWaitTime = m_iWaitTime;
    }
}

int clsUniversalBatch::AddReq(void* ptReq, void* ptRsp, int& iRet)
{
    return AddReqImpl(ptReq, ptRsp, iRet, 1);
}

int clsUniversalBatch::AddReq(void* ptReq, void* ptRsp, int& iRet, int iRealCnt)
{
    return AddReqImpl(ptReq, ptRsp, iRet, iRealCnt);
}

int clsUniversalBatch::AddReqImpl(void* ptReq, void* ptRsp, int& iRet, int iRealCnt)
{
    assert(NULL != ptReq);
    assert(NULL != ptRsp);

    std::unique_lock<std::mutex> lock(m_tList.tMutex);
    clsPipeAllocator::PipePair tPipePair = {{-1, -1}};
    if (false == m_tPipeAlloc.AllocPipe(tPipePair))
    {
        return ALLOC_PIPE_FAILED;
    }

    AssertValidPipePair(tPipePair);
    Node_t* lpNode = GetFirstNode(&m_tList);
    if (NULL == lpNode)
    {
        m_tPipeAlloc.FreePipe(&tPipePair, 1);
        return NO_FREE_NODE;
    }

    Task_t* pTask = lpNode->pTask + (lpNode->iTaskCnt++);
    assert(NULL != pTask);
    assert(-1 == pTask->iPipe[0]);
    assert(-1 == pTask->iPipe[1]);
    assert(-1 == pTask->iRet);
    assert(NULL == pTask->ptRsp);
    assert(NULL == pTask->ptReq);

    pTask->iPipe[0] = tPipePair.iPipe[0];
    pTask->iPipe[1] = tPipePair.iPipe[1];
    pTask->ptReq    = ptReq;
    pTask->ptRsp    = ptRsp;
    pTask->iRet     = -1;
    lpNode->iBatchSize += iRealCnt;

    uint64_t llNow = GetTickMS();
    uint64_t llDiff = 0;
    if (1 == lpNode->iTaskCnt)
    {
        lpNode->tFirstTaskTime = llNow;
    }
    else
    {
        if (lpNode->tFirstTaskTime <= llNow) {
            llDiff = llNow = lpNode->tFirstTaskTime;
        }
    }

    assert(0 <= m_iWaitTime);
    if (m_iBatchCnt <= lpNode->iBatchSize || llDiff >= (uint64_t)m_iWaitTime)
    // llDiff >= static_cast<uint64_t>(m_iWaitTime))
    {
        Node_t* pNode = TakeFirstNotEmptyNode(&m_tList);
        lock.unlock();

        assert(pNode == lpNode);
        lpNode->iRef = lpNode->iTaskCnt;
        assert(NULL != m_pfnHandle);
        m_pfnHandle(pNode);

        Notify(lpNode, true);
    }
    else
    {
        lock.unlock();
        GetNotify(pTask->iPipe[0]);
    }

    // no lock
    iRet = pTask->iRet;
    assert(lpNode->iRef >= 1);

    int iOldVal = __sync_fetch_and_sub(&lpNode->iRef, 1);
    if (1 == iOldVal)
    {
        Release(lpNode);
    }

    return 0;
}

void clsUniversalBatch::Notify(Node_t* lpNode, bool bExceptLast)
{
    char ch            = 'z';
    const int iTaskCnt = lpNode->iTaskCnt;
    if (!bExceptLast)
    {
        for (int i = 0; i < iTaskCnt; ++i)
        {
            assert(1 == write(lpNode->pTask[i].iPipe[1], &ch, 1));
        }
    }
    else
    {
        for (int i = 0; i < iTaskCnt - 1; ++i)
        {
            assert(1 == write(lpNode->pTask[i].iPipe[1], &ch, 1));
        }
    }
}

void clsUniversalBatch::GetNotify(int iFd)
{
    struct pollfd PollFd;
    PollFd.events = POLLIN | POLLERR;
    PollFd.fd     = iFd;

    while (true)
    {
        int iRet = poll(&PollFd, 1, 1000);
        if (iRet > 0)
        {
            break;
        }
        else
        {
            logerr("ERROR: %s:%s:%d poll time out", __FILE__, __func__, __LINE__);
        }
    }

    char ch;
    int iRet = read(iFd, &ch, 1);
    assert(iRet == 1);
}

void clsUniversalBatch::DoMonitor()
{
    Node_t* lpNode = TakeFirstNotEmptyNodeLock(&m_tList);
    if (lpNode)
    {
        lpNode->iRef = lpNode->iTaskCnt;
        assert(NULL != m_pfnHandle);
        m_pfnHandle(lpNode);
        logerr(
            "DEBUG: monitor send cnt %d", lpNode->iTaskCnt);

        Notify(lpNode, false);
    }
}

void clsUniversalBatch::StartMonitor()
{
    if (nullptr != m_oMonitor)
    {
        return;
    }

    m_oMonitor = cutils::make_unique<
        cutils::AsyncWorker>(MonitorBatchJob, std::ref(*this));
    assert(nullptr != m_oMonitor);
}
}  // namespace dbcomm


