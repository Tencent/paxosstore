
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cstring>

#include "dbcomm/hashlock.h"
#include "cutils/log_utils.h"
#include "cutils/hash_utils.h"
#include "memidx.h"
#include "memcomm.h"
#include "memsize_mng.h" 


namespace {

    
} // namespace 

namespace memkv {

KeyNodePtr &KeyNodePtr::Next(KeyBlock_t *astKeyBlock[]) {
    return astKeyBlock[iBlockID]->aKey[iOffset].stNext;
}

KeyNode_t &KeyNodePtr::Node(KeyBlock_t *astKeyBlock[]) {
    return astKeyBlock[iBlockID]->aKey[iOffset];
}

clsMemIdx::clsMemIdx()
    : m_iShmKey(0),
      m_pstMemIdxInfo(NULL),
      m_pstHead(NULL),
      m_poAllocLock(NULL),
      m_poSpinLock(NULL) {
    memset(m_aKeyBlock, 0, sizeof(KeyBlock_t *) * MAX_BLOCK);
}

clsMemIdx::~clsMemIdx() {
    if (m_pstMemIdxInfo) {
        for (uint32_t i = 0; i < m_pstMemIdxInfo->iUseBlock; i++) {
            if (m_aKeyBlock[i]) {
                shmdt((void *)m_aKeyBlock[i]);
            }
        }

        shmdt((void *)m_pstMemIdxInfo);
    }

    delete m_poSpinLock;
    m_poSpinLock = NULL;

    delete m_poAllocLock;
    m_poAllocLock = NULL;
}

int clsMemIdx::DelShm(uint32_t iShmKey) {
    // MemIdxInfoShmKey
    KVDelShm(iShmKey);

    // HashHead
    iShmKey++;
    KVDelShm(iShmKey);

    // 1 - 256 KeyBlock
    for (int i = 1; i <= MAX_BLOCK; i++) {
        iShmKey++;
        KVDelShm(iShmKey);
    }
    return 0;
}

int clsMemIdx::Init(uint32_t iShmKey, uint32_t iHeadSize, uint32_t iMaxBlock,
                    const char *sAllocLockPath) {
    if (iMaxBlock > MAX_BLOCK) {
        logerr("iMaxBlock %u > MAX_BLOCK %u", iMaxBlock, MAX_BLOCK);
        return Too_Large_Max_Block;
    }

    m_iShmKey = iShmKey;
    DelShm(m_iShmKey);

    const int iMemIdxInfoShmKey = m_iShmKey;
    m_iShmKey++;
    const int iHashHeadShmKey = m_iShmKey;
    if (KVGetShm2((void **)&m_pstMemIdxInfo, iMemIdxInfoShmKey,
                  sizeof(MemIdxInfo_t), 0666 | IPC_CREAT) < 0) {
        logerr("KVGetShm2 %x error ", iMemIdxInfoShmKey);
        return -1;
    }

    m_pstMemIdxInfo->iMaxBlock = iMaxBlock;

    if (KVGetShm2((void **)&m_pstHead, iHashHeadShmKey,
                  sizeof(uint32_t) + iHeadSize * sizeof(KeyNodePtr),
                  0666 | IPC_CREAT) < 0) {
        logerr("KVGetShm2 %x error ", iHashHeadShmKey);
        return -2;
    }
    m_pstHead->iSize = iHeadSize;

    clsMemSizeMng::GetDefault()->AddUseSize(
            iHeadSize * sizeof(KeyNodePtr));

    m_poAllocLock = new dbcomm::HashBaseLock();
    assert(m_poAllocLock != NULL);
    int ret = m_poAllocLock->Init(sAllocLockPath, 1);
    if (ret < 0) {
        logerr("clsMemIdx::AllocLock ret %d", ret);
        return -3;
    }

    m_poSpinLock = new AllocSpinLock();
    m_poSpinLock->Init(iMaxBlock);

    return 0;
}

int clsMemIdx::InitReadOnly(uint32_t iShmKey, uint32_t iHeadSize) {
    m_iShmKey = iShmKey;

    const int iMemIdxInfoShmKey = m_iShmKey;
    m_iShmKey++;
    const int iHashHeadShmKey = m_iShmKey;
    if (KVGetShm2((void **)&m_pstMemIdxInfo, iMemIdxInfoShmKey,
                  sizeof(MemIdxInfo_t), 0444) < 0) {
        logerr("KVGetShm2 %x error ", iMemIdxInfoShmKey);
        return -1;
    }

    if (KVGetShm2((void **)&m_pstHead, iHashHeadShmKey,
                  sizeof(uint32_t) + iHeadSize * sizeof(KeyNodePtr),
                  0666) < 0) {
        logerr("KVGetShm2 %x error ", iHashHeadShmKey);
        return -2;
    }

    for (uint32_t i = 0; i < m_pstMemIdxInfo->iUseBlock; i++) {
        int ret = InitReadOnlyBlock(i);
        assert(ret >= 0);
    }

    return 0;
}

int clsMemIdx::UpdateReadOnly() {
    if (NULL == m_pstMemIdxInfo || NULL == m_pstHead || NULL == m_aKeyBlock) {
        return -1;
    }

    assert(NULL != m_pstMemIdxInfo);
    assert(NULL != m_pstHead);
    assert(NULL != m_aKeyBlock);

    for (uint32_t idx = 0; idx < m_pstMemIdxInfo->iUseBlock; ++idx) {
        if (NULL != m_aKeyBlock[idx]) {
            continue;
        }

        assert(0 == InitReadOnlyBlock(idx));
        assert(NULL != m_aKeyBlock[idx]);
    }

    return 0;
}

void clsMemIdx::Detach() {
    if (NULL != m_pstMemIdxInfo) {
        for (uint32_t i = 0; i < m_pstMemIdxInfo->iUseBlock; i++) {
            if (NULL != m_aKeyBlock[i]) {
                shmdt((void *)m_aKeyBlock[i]);
                m_aKeyBlock[i] = NULL;
            }
        }

        shmdt((void *)m_pstHead);
        m_pstHead = NULL;
        shmdt((void *)m_pstMemIdxInfo);
        m_pstMemIdxInfo = NULL;
    }

    return;
}

int clsMemIdx::InitReadOnlyBlock(int iBlockID) {
    assert(m_aKeyBlock[iBlockID] == NULL);
    m_iShmKey++;
    if (KVGetShm2((void **)&m_aKeyBlock[iBlockID], m_iShmKey,
                  sizeof(KeyBlock_t) + MAX_KEY_SIZE, 0444) < 0) {
        logerr("KVGetShm2 %x error ", m_iShmKey);
        return -1;
    }

    return 0;
}

int clsMemIdx::InitBlock(int iBlockID) {
    assert(m_aKeyBlock[iBlockID] == NULL);
    m_iShmKey++;

    if (KVGetShm_NoMemSet((void **)&m_aKeyBlock[iBlockID], m_iShmKey,
                          sizeof(KeyBlock_t) + MAX_KEY_SIZE,
                          0666 | IPC_CREAT) < 0) {
        logerr("KVGetShm2 %x error ", m_iShmKey);
        return -1;
    }

    m_aKeyBlock[iBlockID]->iMaxKeyCnt = MAX_OFFSET;
    m_aKeyBlock[iBlockID]->iUseCnt = 0;
    m_aKeyBlock[iBlockID]->stFree.Set(iBlockID, 1);

    m_aKeyBlock[iBlockID]->aKey[0].stNext.SetNULL();
    m_aKeyBlock[iBlockID]->aKey[0].cKeyLen = 0;
    for (uint32_t i = 1; i < m_aKeyBlock[iBlockID]->iMaxKeyCnt - 1; i++) {
        m_aKeyBlock[iBlockID]->aKey[i].cKeyLen = 0;
        m_aKeyBlock[iBlockID]->aKey[i].stNext.Set(iBlockID, i + 1);
    }

    m_aKeyBlock[iBlockID]
        ->aKey[m_aKeyBlock[iBlockID]->iMaxKeyCnt - 1]
        .stNext.SetNULL();

    return 0;
}

KeyNodePtr clsMemIdx::BlockAlloc(int iBlockID) {
    SpinLock spinlock(m_poSpinLock, iBlockID);

    KeyBlock_t *pstBlock = m_aKeyBlock[iBlockID];
    KeyNodePtr stFree = pstBlock->stFree;
    if (stFree.IsNULL()) return stFree;

    pstBlock->stFree = stFree.Next(m_aKeyBlock);
    pstBlock->iUseCnt++;
    return stFree;
}

int clsMemIdx::AllocBlock() {
    dbcomm::HashLock hashlock(m_poAllocLock, uint32_t{1});
    hashlock.WriteLock(__FILE__, __LINE__);

    if (m_pstMemIdxInfo->iUseBlock + 1 > m_pstMemIdxInfo->iMaxBlock) {
        return -1;
    }

    InitBlock(m_pstMemIdxInfo->iUseBlock);
    int iBlockID = m_pstMemIdxInfo->iUseBlock;
    m_pstMemIdxInfo->iUseBlock++;

    clsMemSizeMng::GetDefault()->AddUseSize(MAX_KEY_SIZE);
    // printf("iUseBlock %u\n", m_pstMemIdxInfo->iUseBlock);
    return iBlockID;
}

KeyNodePtr clsMemIdx::Alloc() {
    while (1) {
        uint32_t iBlockID = m_pstMemIdxInfo->iCurBlock;
        uint32_t iUseBlock = m_pstMemIdxInfo->iUseBlock;
        if (iUseBlock > 0) {
            for (unsigned int i = 0; i < m_pstMemIdxInfo->iUseBlock;
                 i++, iBlockID++) {
                if (iBlockID >= m_pstMemIdxInfo->iUseBlock) iBlockID = 0;

                KeyNodePtr stPtr = BlockAlloc(iBlockID);
                if (!stPtr.IsNULL()) {
                    m_pstMemIdxInfo->iCurBlock = iBlockID;
                    return stPtr;
                }
            }
        }

        dbcomm::HashLock hashlock(m_poAllocLock, uint32_t{1});
        hashlock.WriteLock(__FILE__, __LINE__);
        if (iUseBlock < m_pstMemIdxInfo->iUseBlock) continue;

        assert(iUseBlock <= m_pstMemIdxInfo->iUseBlock);

        if (m_pstMemIdxInfo->iUseBlock + 1 > m_pstMemIdxInfo->iMaxBlock) {
            KeyNodePtr NULLPtr;
            return NULLPtr;
        }

        InitBlock(m_pstMemIdxInfo->iUseBlock);
        m_pstMemIdxInfo->iCurBlock = m_pstMemIdxInfo->iUseBlock;
        m_pstMemIdxInfo->iUseBlock++;

        clsMemSizeMng::GetDefault()->AddUseSize(MAX_KEY_SIZE);
        // printf("iUseBlock %u\n", m_pstMemIdxInfo->iUseBlock);
    }

    KeyNodePtr NULLPtr;
    return NULLPtr;
}

void clsMemIdx::Free(KeyNodePtr &stFree) {
    SpinLock spinlock(m_poSpinLock, stFree.iBlockID);

    KeyBlock_t *pstBlock = m_aKeyBlock[stFree.iBlockID];
    stFree.Node(m_aKeyBlock).cKeyLen = 0;
    stFree.Next(m_aKeyBlock) = pstBlock->stFree;
    pstBlock->stFree = stFree;
    pstBlock->iUseCnt--;
}

void clsMemIdx::MemCpy(KeyNodePtr &stPtr, MemKey_t &stKey) {
    KeyNode_t &node = stPtr.Node(m_aKeyBlock);
    memcpy(node.sKey, stKey.sKey, stKey.cKeyLen);
    node.cKeyLen = stKey.cKeyLen;
    node.iBlockID = stKey.iBlockID;
    node.iBlockOffset = stKey.iBlockOffset;
}
void clsMemIdx::MemCpy(MemKey_t &stKey, KeyNodePtr &stPtr) {
    KeyNode_t &node = stPtr.Node(m_aKeyBlock);
    memcpy(stKey.sKey, node.sKey, node.cKeyLen);
    stKey.cKeyLen = node.cKeyLen;
    stKey.iBlockID = node.iBlockID;
    stKey.iBlockOffset = node.iBlockOffset;
}

bool clsMemIdx::KeyNodeCmp(const char *sKey, uint8_t cKeyLen,
                           KeyNodePtr stPtr) {
    KeyNode_t &node = stPtr.Node(m_aKeyBlock);
    return node.cKeyLen == cKeyLen && !memcmp(node.sKey, sKey, cKeyLen);
}

KeyNodePtr clsMemIdx::ListFind(const char *sKey, uint8_t cKeyLen,
                               KeyNodePtr stHead) {
    KeyNodePtr stPtr = stHead;
    while (!stPtr.IsNULL()) {
        if (KeyNodeCmp(sKey, cKeyLen, stPtr)) return stPtr;
        stPtr = stPtr.Next(m_aKeyBlock);
    }

    KeyNodePtr NULLPtr;
    return NULLPtr;
}

KeyNodePtr clsMemIdx::ListDel(const char *sKey, uint8_t cKeyLen,
                              KeyNodePtr stHead) {
    KeyNodePtr stPtr = stHead;
    KeyNodePtr stPrev = stHead;
    while (!stPtr.IsNULL()) {
        if (KeyNodeCmp(sKey, cKeyLen, stPtr)) {
            stPrev.Next(m_aKeyBlock) = stPtr.Next(m_aKeyBlock);
            return stPtr;
        }
        stPrev = stPtr;
        stPtr = stPtr.Next(m_aKeyBlock);
    }

    KeyNodePtr NULLPtr;
    return NULLPtr;
}

int clsMemIdx::Set(MemKey_t &stKey) { return Set(-1, stKey); }

int clsMemIdx::Set(int iBlockID, MemKey_t &stKey) {
    uint32_t iIdx = HashFunc(stKey.sKey, stKey.cKeyLen);
    KeyNodePtr *astArray = m_pstHead->astArray;

    if (astArray[iIdx].IsNULL()) {
        KeyNodePtr stPtr;
        if (iBlockID < 0)
            stPtr = Alloc();
        else
            stPtr = BlockAlloc(iBlockID);
        if (stPtr.IsNULL()) {
            logerr("stPtr.IsNULL()");
            return Empty;
        }
        MemCpy(stPtr, stKey);
        astArray[iIdx] = stPtr;
        stPtr.Next(m_aKeyBlock).SetNULL();
        return 1;
    }

    KeyNodePtr stPtr = ListFind(stKey.sKey, stKey.cKeyLen, astArray[iIdx]);
    if (!stPtr.IsNULL()) {
        MemCpy(stPtr, stKey);
        return 0;
    }

    if (iBlockID < 0)
        stPtr = Alloc();
    else
        stPtr = BlockAlloc(iBlockID);
    if (stPtr.IsNULL()) {
        logerr("stPtr.IsNULL()");
        return Empty;
    }

    MemCpy(stPtr, stKey);
    stPtr.Next(m_aKeyBlock) = astArray[iIdx];
    astArray[iIdx] = stPtr;
    return 1;
}

int clsMemIdx::Get(MemKey_t &stKey) {
    uint32_t iIdx = HashFunc(stKey.sKey, stKey.cKeyLen);
    KeyNodePtr *astArray = m_pstHead->astArray;
    assert(NULL != astArray);

    if (astArray[iIdx].IsNULL()) {
        return 0;
    }

    KeyNodePtr stPtr = ListFind(stKey.sKey, stKey.cKeyLen, astArray[iIdx]);
    if (stPtr.IsNULL()) {
        return 0;
    }

    assert(false == stPtr.IsNULL());
    KeyNode_t &node = stPtr.Node(m_aKeyBlock);
    stKey.iBlockID = node.iBlockID;
    stKey.iBlockOffset = node.iBlockOffset;
    // check
    assert(stKey.cKeyLen == node.cKeyLen);
    assert(0 == memcmp(stKey.sKey, node.sKey, stKey.cKeyLen));
    return 1;
}

int clsMemIdx::Get(const char* sKey, uint8_t cKeyLen, MemKey_t &stKey) {
    uint32_t iIdx = HashFunc(sKey, cKeyLen);

    KeyNodePtr *astArray = m_pstHead->astArray;

    if (astArray[iIdx].IsNULL()) {
        memset(&stKey, 0, sizeof(stKey));
        return 0;
    }

    KeyNodePtr stPtr = ListFind(sKey, cKeyLen, astArray[iIdx]);
    if (stPtr.IsNULL()) {
        memset(&stKey, 0, sizeof(stKey));
        return 0;
    }

    MemCpy(stKey, stPtr);
    return 1;
}

int clsMemIdx::Del(const char* sKey, uint8_t cKeyLen) {
    uint32_t iIdx = HashFunc(sKey, cKeyLen); 
    KeyNodePtr *astArray = m_pstHead->astArray;

    if (astArray[iIdx].IsNULL()) return 0;

    if (KeyNodeCmp(sKey, cKeyLen, astArray[iIdx])) {
        KeyNodePtr stPtr = astArray[iIdx];
        astArray[iIdx] = astArray[iIdx].Next(m_aKeyBlock);
        Free(stPtr);
        return 1;
    }

    KeyNodePtr stPtr = ListDel(sKey, cKeyLen, astArray[iIdx]);
    if (stPtr.IsNULL()) {
        return 0;
    }

    Free(stPtr);
    return 1;
}

int clsMemIdx::Visit(
        clsMemBaseVisitor* pVisitor, unsigned long long idx) {
    if (idx >= m_pstHead->iSize) {
        return -1;
    }
    KeyNodePtr stPtr = m_pstHead->astArray[idx];
    if (stPtr.IsNULL()) return 0;

    MemKey_t tKey;
    while (!stPtr.IsNULL()) {
        MemCpy(tKey, stPtr);
        pVisitor->OnMemKey(tKey);
        stPtr = stPtr.Next(m_aKeyBlock);
    }

    return 0;
}

uint32_t clsMemIdx::HashFunc(const char* sKey, uint8_t cKeyLen)
{
    assert(sizeof(uint64_t) == cKeyLen);
    uint64_t llLogID = 0;
    memcpy(&llLogID, sKey, cKeyLen);
    return cutils::dict_int_hash_func(llLogID) % (m_pstHead->iSize);
}

}  // namespace memkv
