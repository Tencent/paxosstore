
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <pthread.h>
#include <functional>
#include <memory>
#include <string>
#include <mutex>
#include "pipe_alloc.h"

namespace cutils
{
class AsyncWorker;
}

namespace dbcomm
{
class clsUniversalBatch
{
public:
    struct Node_t;
    clsUniversalBatch(int iWaitTime,
                      std::function<void(
                          clsUniversalBatch::Node_t*)> pfnHandle,
                      const std::string& sMonitorName);

    ~clsUniversalBatch();

    int AddReq(void* tReq, void* tRsp, int& iRet);
    int AddReq(void* tReq, void* tRsp, int& iRet, int iRealCnt);

    void StartMonitor();
    void SetBatchCnt(int iBatchCnt);

    void SetWaitTime(int iWaitTime);
    int GetWaitTime() const
    {
        return m_iWaitTime;
    }

    void DoMonitor();

    const dbcomm::clsPipeAllocator& PeekPipeAlloc() const
    {
        return m_tPipeAlloc;
    }

    const std::string& GetMonitorName() const
    {
        return m_sMonitorName;
    }

private:
    int AddReqImpl(void* tReq, void* tRsp, int& iRet, int iRealCnt);

public:
    enum
    {
        MAX_BATCH_SIZE = 50
    };
    enum
    {
        NO_FREE_NODE      = 1,
        NEED_BATCH        = 2,
        ALLOC_PIPE_FAILED = 3,
    };

    struct Task_t
    {
        void* ptReq;
        void* ptRsp;
        int iRet;
        int iPipe[2];
    };

    struct Node_t
    {
        Node_t* pNext;
        Task_t* pTask;
        int iTaskCnt;
        int iBatchSize;
        volatile int iRef;
        uint64_t tFirstTaskTime;
    };

    struct NodeList_t
    {
        std::mutex tMutex;
        Node_t* pHead;
        Node_t* pTail;

        Node_t* pItems;
        int iItemCnt;
    };

private:
    void Release(Node_t* pNode);
    void Release(Task_t* pTask);
    void Notify(Node_t* pNode, bool bExceptLast);
    void GetNotify(int iFd);

private:
    const std::string m_sMonitorName;
    std::function<void(clsUniversalBatch::Node_t*)> m_pfnHandle;

    NodeList_t m_tList;
    clsPipeAllocator m_tPipeAlloc;

    int m_iWaitTime;
    int m_iBatchCnt;

    std::unique_ptr<cutils::AsyncWorker> m_oMonitor;
};

}  // namespace dbcomm;
