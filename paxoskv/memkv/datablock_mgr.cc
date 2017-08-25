
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <pthread.h>
#include <cstring>
#include <cstdio>
#include <cassert>
#include "dbcomm/hashlock.h"
#include "cutils/log_utils.h"
#include "datablock_mgr.h"
#include "datablock.h"
#include "memsize_mng.h"
#include "memcomm.h"
#include "co_routine.h"

namespace memkv {

clsDataBlockMgr::clsDataBlockMgr(uint32_t iBaseShmKey) {
    m_iBaseShmKey = iBaseShmKey;
    m_ppDataBlock = NULL;
    m_pGlobalMeta = NULL;
    m_pHashBaseLock = NULL;
    m_iStartBlockID = 0;

    m_iMaxAppendBlockNum = 2;
    memset(m_arrAppendBlock, -1, sizeof(m_arrAppendBlock));
}

clsDataBlockMgr::~clsDataBlockMgr() {
    if (m_pGlobalMeta != NULL) {
        for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
            if (m_ppDataBlock[i]) {
                delete m_ppDataBlock[i];
                m_ppDataBlock[i] = NULL;
            }
        }
    }

    free(m_ppDataBlock);
    m_ppDataBlock = NULL;

    if (m_pHashBaseLock != NULL) {
        delete m_pHashBaseLock;
    }

    Detach();
}

int clsDataBlockMgr::Init(const char* sLockPath, uint32_t iMaxAppendBlockNum) {
    printf("clsDataBlockMgr::Init sLockPath %s iMaxAppendBlockNum %u\n",
           sLockPath, iMaxAppendBlockNum);
    assert(2 <= iMaxAppendBlockNum);
    assert(MAX_APPEND_BLOCK_NUM >= iMaxAppendBlockNum);
    m_iMaxAppendBlockNum = iMaxAppendBlockNum;

    char* p = NULL;

    int iRet = -1;

    // init GlobalMeta
    int iGlobalMetaSize = sizeof(GlobalMeta_t);

    if (KVGetShm2((void**)&p, m_iBaseShmKey, iGlobalMetaSize, 0666) < 0) {
        int iRet = KVGetShm2((void**)&p, m_iBaseShmKey, iGlobalMetaSize,
                             0666 | IPC_CREAT);
        if (iRet < 0) {
            logerr("KVGetShm2: iRet %d ShmKey %u size %u", iRet, m_iBaseShmKey,
                   iGlobalMetaSize);
            return -1;
        }
        memset(p, 0, iGlobalMetaSize);
    }

    m_pGlobalMeta = (GlobalMeta_t*)p;

    if (m_pGlobalMeta->iBlockNum > MAX_BLOCK_NUM) {
        logerr("BlockNum %d > MAX_BLOCK_NUM %d", m_pGlobalMeta->iBlockNum,
               MAX_BLOCK_NUM);
        return -2;
    }

    // init lock
    //	char sLockPath[256]={0};
    //	snprintf(sLockPath, sizeof(sLockPath),
    //"/home/qspace/data/kvsvr/memkv/datablockmgr.lock");

    m_pHashBaseLock = new dbcomm::HashBaseLock();
    iRet = m_pHashBaseLock->Init(sLockPath, MAX_BLOCK_NUM + 1);
    if (iRet != 0) {
        logerr("InitLock iRet %d", iRet);
        return -3;
    }

    // Init DataBlock
    m_ppDataBlock =
        (clsDataBlock**)calloc(sizeof(clsDataBlock*), MAX_BLOCK_NUM);
    assert(m_ppDataBlock != NULL);

    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
        uint32_t iShmKey = m_iBaseShmKey + i + 1;

        clsDataBlock* pDataBlock = 
            new clsDataBlock(iShmKey, m_pHashBaseLock);
        iRet = pDataBlock->Attach(i);
        if (iRet != 0) {
            logerr("Init DataBlock iRet %d iBlockID %d ShmKey %u BlockNum %u",
                   iRet, i, iShmKey, m_pGlobalMeta->iBlockNum);
            delete pDataBlock;
            return -4;
        }
        m_ppDataBlock[i] = pDataBlock;
    }

    KVDelShm(m_iBaseShmKey + m_pGlobalMeta->iBlockNum + 1);

    memset(m_arrAppendBlock, -1, sizeof(m_arrAppendBlock));
    return 0;
}

bool clsDataBlockMgr::GetLoadFlag() {
    return (m_pGlobalMeta->iLoadFlag == 0) ? false : true;
}

void clsDataBlockMgr::SetLoadFlag() { m_pGlobalMeta->iLoadFlag = 1; }

int clsDataBlockMgr::InitReadOnly() {
    char* p = NULL;

    int iRet = -1;

    // init GlobalMeta
    int iGlobalMetaSize = sizeof(GlobalMeta_t);

    if (KVGetShm2((void**)&p, m_iBaseShmKey, iGlobalMetaSize, 0444) < 0) {
        return -1;
    }

    m_pGlobalMeta = (GlobalMeta_t*)p;

    if (m_pGlobalMeta->iBlockNum > MAX_BLOCK_NUM) {
        logerr("BlockNum %d > MAX_BLOCK_NUM %d", m_pGlobalMeta->iBlockNum,
               MAX_BLOCK_NUM);
        return -2;
    }

    // Init DataBlock
    m_ppDataBlock =
        (clsDataBlock**)calloc(sizeof(clsDataBlock*), MAX_BLOCK_NUM);
    assert(m_ppDataBlock != NULL);

    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
        uint32_t iShmKey = m_iBaseShmKey + i + 1;

        clsDataBlock* pDataBlock = new clsDataBlock(iShmKey, m_pHashBaseLock);

        iRet = pDataBlock->InitReadOnly(i);

        if (iRet != 0) {
            logerr("Attach DataBlock iRet %d iBlockID %d ShmKey %u BlockNum %u",
                   iRet, i, iShmKey, m_pGlobalMeta->iBlockNum);
            delete pDataBlock;
            return -4;
        }
        m_ppDataBlock[i] = pDataBlock;
    }

    return 0;
}

int clsDataBlockMgr::UpdateReadOnly() {
    if (NULL == m_pGlobalMeta || NULL == m_ppDataBlock) {
        return -1;
    }

    assert(NULL != m_pGlobalMeta);
    assert(NULL != m_ppDataBlock);

    for (uint32_t idx = 0; idx < m_pGlobalMeta->iBlockNum; ++idx) {
        if (NULL != m_ppDataBlock[idx]) {
            continue;
        }

        uint32_t iShmKey = m_iBaseShmKey + idx + 1;
        clsDataBlock* pDataBlock = new clsDataBlock(iShmKey, m_pHashBaseLock);
        int iRet = pDataBlock->InitReadOnly(idx);
        if (0 != iRet) {
            logerr("Attach DataBlock iRet %d iBlockID %d ShmKey %u BlockNum %u",
                   iRet, idx, iShmKey, m_pGlobalMeta->iBlockNum);
            delete pDataBlock;
            return -4;
        }

        m_ppDataBlock[idx] = pDataBlock;
    }

    return 0;
}

int clsDataBlockMgr::Detach() {
    if (m_pGlobalMeta) {
        shmdt((void*)m_pGlobalMeta);
    }
    return 0;
}

clsDataBlock* clsDataBlockMgr::GetByStatus(uint8_t cStatus,
                                           uint32_t iStartBlockID) {
    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
        uint32_t iIdx = (iStartBlockID + i) % m_pGlobalMeta->iBlockNum;
        if (m_ppDataBlock[iIdx]->GetStatus() == cStatus) {
            return m_ppDataBlock[iIdx];
        }
    }

    return NULL;
}

clsDataBlock* clsDataBlockMgr::GetByIdx(uint32_t iBlockID) {
    if (iBlockID >= m_pGlobalMeta->iBlockNum) {
        logerr("iBlockID %d BlockNum %d", iBlockID, m_pGlobalMeta->iBlockNum);
        return NULL;
    }

    return m_ppDataBlock[iBlockID];
}

clsDataBlock* clsDataBlockMgr::Alloc() {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    return AllocNotLock();
}

clsDataBlock* clsDataBlockMgr::AllocNotLock(bool bCheckMemSpace) {
    clsDataBlock* pDataBlock = GetByStatus(clsDataBlock::EMPTY, 0);

    if (pDataBlock != NULL) {
        // printf("BlockID %u Status %u\n", pDataBlock->GetBlockID(),
        //		pDataBlock->GetStatus());
        return pDataBlock;
    }

    if (m_pGlobalMeta->iBlockNum >= MAX_BLOCK_NUM) {
        return NULL;
    }

    bool bMemEnough = clsMemSizeMng::GetDefault()->IsMemEnough(BLOCK_SIZE);
    if (bCheckMemSpace && !bMemEnough) {
        logerr("Not Enough Mem Left");
        return NULL;
    }

    uint32_t iShmKey = m_iBaseShmKey + m_pGlobalMeta->iBlockNum + 1;

    uint32_t iBlockID = m_pGlobalMeta->iBlockNum;

    pDataBlock = new clsDataBlock(iShmKey, m_pHashBaseLock);

    int iRet = pDataBlock->Init(iBlockID);
    if (iRet != 0) {
        logerr("DataBlock Init iRet %d iShmKey %u iBlockID %u", 
                iRet, iShmKey, iBlockID);
        delete pDataBlock;
        return NULL;
    }

    m_ppDataBlock[m_pGlobalMeta->iBlockNum] = pDataBlock;
    m_pGlobalMeta->iBlockNum++;
    return pDataBlock;
}

void clsDataBlockMgr::Free(clsDataBlock* pDataBlock) {
    if (pDataBlock != NULL) {
        dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
        hashlock.WriteLock();

        pDataBlock->SetStatus(clsDataBlock::EMPTY);
        pDataBlock->SetUseSize(0);
        pDataBlock->SetUseKeyCnt(0);
        pDataBlock->SetTotalKeyCnt(0);
    }
}

clsDataBlock* clsDataBlockMgr::GetNotWritingAppendBlock() {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
        clsDataBlock* pDataBlock = m_ppDataBlock[i];

        if (pDataBlock->GetStatus() == clsDataBlock::APPEND &&
            !pDataBlock->IsWriting()) {
            pDataBlock->SetWritingFlag();
            return pDataBlock;
        }
    }

    clsDataBlock* pDataBlock = AllocNotLock();
    if (pDataBlock != NULL) {
        pDataBlock->SetStatus(clsDataBlock::APPEND);
        pDataBlock->SetWritingFlag();
        return pDataBlock;
    }
    return NULL;
}

clsDataBlock* clsDataBlockMgr::GetAppendBlock() {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    clsDataBlock* pDataBlock =
        GetByStatus(clsDataBlock::APPEND, m_iStartBlockID);
    if (pDataBlock == NULL) {
        pDataBlock = AllocNotLock();
    }

    if (pDataBlock != NULL) {
        pDataBlock->SetStatus(clsDataBlock::APPEND);
        m_iStartBlockID = pDataBlock->GetBlockID();
    }

    return pDataBlock;
}

clsDataBlock* clsDataBlockMgr::GetAppendBlockNew() {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    assert(2 <= m_iMaxAppendBlockNum);
    assert(MAX_BLOCK_NUM >= m_iMaxAppendBlockNum);
    if (co_is_enable_sys_hook()) {
        uint64_t tid = 0;
        pthread_threadid_np(nullptr, &tid);
        size_t idx = tid % (m_iMaxAppendBlockNum - 1);
        return GetAppendBlockNoLock(idx);
    }

    return GetAppendBlockNoLock(m_iMaxAppendBlockNum - 1);
}

clsDataBlock* clsDataBlockMgr::GetAppendBlockNoLock(const size_t idx) {
    assert(idx < m_iMaxAppendBlockNum);
    clsDataBlock* pDataBlock = NULL;
    if (0 <= m_arrAppendBlock[idx]) {
        pDataBlock = GetByIdx(m_arrAppendBlock[idx]);
        assert(NULL != pDataBlock);
        if (clsDataBlock::APPEND != pDataBlock->GetStatus()) {
            m_arrAppendBlock[idx] = -1;
            pDataBlock = NULL;
        }
    }

    if (NULL != pDataBlock) {
        assert(0 <= m_arrAppendBlock[idx]);
        // may not be the case;
        // assert(clsDataBlock::APPEND == pDataBlock->GetStatus());
        return pDataBlock;
    }

    assert(NULL == pDataBlock);
    std::set<int> setAppendBlock;
    for (uint32_t i = 0; i < m_iMaxAppendBlockNum; ++i) {
        setAppendBlock.insert(m_arrAppendBlock[i]);
    }

    uint32_t iStartBlockID = m_iStartBlockID;
    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; ++i) {
        uint32_t j = (iStartBlockID + i) % m_pGlobalMeta->iBlockNum;
        if (clsDataBlock::APPEND == m_ppDataBlock[j]->GetStatus() &&
            setAppendBlock.end() == setAppendBlock.find(j)) {
            pDataBlock = m_ppDataBlock[j];
            assert(NULL != pDataBlock);
            assert(j == pDataBlock->GetBlockID());
            m_arrAppendBlock[idx] = j;
            // assert(clsDataBlock::APPEND == pDataBlock->GetStatus());
            return pDataBlock;
        }
    }

    assert(NULL == pDataBlock);
    pDataBlock = AllocNotLock();
    if (NULL != pDataBlock) {
        pDataBlock->SetStatus(clsDataBlock::APPEND);
        m_iStartBlockID = pDataBlock->GetBlockID();
        m_arrAppendBlock[idx] = pDataBlock->GetBlockID();
        assert(0 <= m_arrAppendBlock[idx]);
    }

    assert(NULL == pDataBlock ||
           clsDataBlock::APPEND == pDataBlock->GetStatus());
    return pDataBlock;
}

clsDataBlock* clsDataBlockMgr::GetLeastFullBlock() {
    uint32_t iUseRatio = 100;
    int iLeastUseIdx = -1;

    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
        uint32_t iIdx = i % m_pGlobalMeta->iBlockNum;
        clsDataBlock* pDataBlock = m_ppDataBlock[iIdx];
        if (pDataBlock->GetStatus() == clsDataBlock::FULL) {
            if (pDataBlock->GetUseRatio() <= iUseRatio) {
                iUseRatio = pDataBlock->GetUseRatio();
                iLeastUseIdx = iIdx;
            }
        }
    }

    if (iLeastUseIdx < 0) {
        return NULL;
    }

    return m_ppDataBlock[iLeastUseIdx];
}

clsDataBlock* 
clsDataBlockMgr::GetMergeFrom(uint32_t iStartBlockID) {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    clsDataBlock* pDataBlock = GetByStatus(clsDataBlock::MERGEFROM, 0);
    if (pDataBlock != NULL) {
        return pDataBlock;
    }

    pDataBlock = GetLeastFullBlock();

    if (pDataBlock != NULL && pDataBlock->GetUseRatio() < 99) {
        pDataBlock->SetStatus(clsDataBlock::MERGEFROM);
        return pDataBlock;
    }

    return NULL;
}

clsDataBlock* clsDataBlockMgr::GetMergeTo() {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    clsDataBlock* pDataBlock = GetByStatus(clsDataBlock::MERGETO, 0);

    if (pDataBlock != NULL) {
        return pDataBlock;
    }

    pDataBlock = AllocNotLock(false);

    if (pDataBlock != NULL) {
        pDataBlock->SetStatus(clsDataBlock::MERGETO);
        return pDataBlock;
    }

    return NULL;
}

uint32_t clsDataBlockMgr::GetBlockNum() { 
    return m_pGlobalMeta->iBlockNum; 
}

void clsDataBlockMgr::CleanWritingFlag() {
    dbcomm::HashLock hashlock(m_pHashBaseLock, uint32_t{0});
    hashlock.WriteLock();

    for (uint32_t i = 0; i < m_pGlobalMeta->iBlockNum; i++) {
        clsDataBlock* pDataBlock = m_ppDataBlock[i];
        pDataBlock->SetNotWritingFlag();
    }
}

}  // namespace memkv
