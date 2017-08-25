
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <errno.h>
#include <stdint.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <vector>
#include <cassert>
#include "pthread.h"
#include "memcomm.h"

namespace dbcomm {

class HashBaseLock;
}

namespace memkv {

class clsMemBaseVisitor;

enum { Empty = -1, Too_Large_Max_Block = -2 };

#define UBYTEMAX(n) ((1ull << ((unsigned long long)n)) - 1ull)

#define MAX_BLOCK 1024
#define MAX_OFFSET (UBYTEMAX(22))
#define MAX_KEY_SIZE MAX_OFFSET * sizeof(KeyNode_t)

//#define MEM_IDX_SHM_KEY 0x2457235

typedef struct s_KeyNode KeyNode_t;
typedef struct s_KeyBlock KeyBlock_t;
class KeyNodePtr {
   public:
    KeyNodePtr(uint32_t iBlockID1, uint32_t iOffset1) {
        iBlockID = iBlockID1;
        iOffset = iOffset1;
    }
    KeyNodePtr() {
        iBlockID = 0;
        iOffset = 0;
    }
    ~KeyNodePtr() {}

    bool IsNULL() { return (iBlockID == 0 && iOffset == 0); }

    void Set(uint32_t iBlockID1, uint32_t iOffset1) {
        iBlockID = iBlockID1;
        iOffset = iOffset1;
    }

    void SetNULL() {
        iBlockID = 0;
        iOffset = 0;
    }

    KeyNodePtr &Next(KeyBlock_t *astKeyBlock[]);
    KeyNode_t &Node(KeyBlock_t *astKeyBlock[]);

    uint32_t iBlockID : 10;
    uint32_t iOffset : 22;
} __attribute__((packed));

typedef struct s_HashHead {
    uint32_t iSize;
    KeyNodePtr astArray[0];
} __attribute__((packed)) HashHead_t;

typedef struct s_MemIdxInfo {
    uint32_t iCurBlock;
    uint32_t iUseBlock;
    uint32_t iMaxBlock;
} __attribute__((packed)) MemIdxInfo_t;

typedef struct s_KeyNode {
    char sKey[8];
    uint8_t cKeyLen;
    uint16_t iBlockID;
    uint32_t iBlockOffset;
    // stNext must be the last
    KeyNodePtr stNext;
} __attribute__((packed)) KeyNode_t;

typedef struct s_KeyBlock {
    uint32_t iMaxKeyCnt;
    uint32_t iUseCnt;
    KeyNodePtr stFree;
    KeyNode_t aKey[0];
} __attribute__((packed)) KeyBlock_t;

#if !defined(__APPLE__)
class AllocSpinLock {
   public:
    AllocSpinLock() {}

    void Init(int iLocks) {
        m_iLocks = iLocks;
        m_pSpinLock =
            (pthread_spinlock_t *)calloc(
                    sizeof(pthread_spinlock_t), iLocks);
        assert(m_pSpinLock != NULL);
        for (int i = 0; i < iLocks; i++) {
            assert(pthread_spin_init(
                        &m_pSpinLock[i], PTHREAD_PROCESS_PRIVATE) == 0);
        }
    }

    ~AllocSpinLock() {
        for (int i = 0; i < m_iLocks; i++) {
            pthread_spin_destroy(&m_pSpinLock[i]);
        }

        free(const_cast<int *>(m_pSpinLock));
        m_pSpinLock = NULL;
    }

    void SpinLock(int iIdx) {
        assert(iIdx <= m_iLocks - 1);
        while (1) {
            int ret = pthread_spin_trylock(&m_pSpinLock[iIdx]);
            if (ret == EBUSY) continue;
            if (ret == 0) break;
            assert(0);
        }
    }
    void SpinUnLock(int iIdx) {
        assert(iIdx <= m_iLocks - 1);
        pthread_spin_unlock(&m_pSpinLock[iIdx]);
    }

    pthread_spinlock_t *m_pSpinLock;
    int m_iLocks;
};

#else

// fake spin lock on mac
class AllocSpinLock {

public:

    AllocSpinLock() = default;

    ~AllocSpinLock() {
        for (auto& mutex : m_vecMutex) {
            pthread_mutex_destroy(&mutex);
        }
    }

    void Init(int iLocks) {
        assert(m_vecMutex.empty());
        assert(0 < iLocks);
        m_vecMutex.resize(iLocks);
        for (auto& mutex : m_vecMutex) {
            assert(0 == pthread_mutex_init(&mutex, nullptr));
        }
    }

    void SpinLock(int idx) {
        assert(static_cast<size_t>(idx) < m_vecMutex.size());
        while (true) {
            int ret = pthread_mutex_trylock(&m_vecMutex[idx]);
            if (EBUSY == ret) {
                continue;
            }

            if (0 == ret) {
                break;
            }

            assert(false);
        }
    }

    void SpinUnLock(int idx) {
        assert(static_cast<size_t>(idx) < m_vecMutex.size());
        pthread_mutex_unlock(&m_vecMutex[idx]);
    }

    std::vector<pthread_mutex_t> m_vecMutex;
};

#endif

class SpinLock {
   public:
    SpinLock(AllocSpinLock *poSpinLock, int iIdx) {
        m_iIdx = iIdx;
        m_poSpinLock = poSpinLock;
        poSpinLock->SpinLock(iIdx);
    }
    ~SpinLock() { m_poSpinLock->SpinUnLock(m_iIdx); }
    int m_iIdx;
    AllocSpinLock *m_poSpinLock;
};

class clsMemIdxIter;
class clsMemIdx {
   public:
    friend class clsMemIdxIter;
    clsMemIdx();
    ~clsMemIdx();
    int Init(uint32_t iShmKey, uint32_t iHeadSize, uint32_t iMaxBlock,
             const char *sAllocLockPath = "//clsMemIdx::AllocLock");
    int Set(MemKey_t &stKey);
    int Get(MemKey_t &stKey);
    int Get(const char* sKey, uint8_t cKeyLen, MemKey_t &stKey);
    int Del(const char* sKey, uint8_t cKeyLen);
    // for memkv init
    int AllocBlock();
    int Set(int BlockID, MemKey_t &stKey);

    // for readonly
    int InitReadOnly(uint32_t iShmKey, uint32_t iHeadSize);
    int InitReadOnlyBlock(int iBlockID);

    int UpdateReadOnly();

    void Detach();
    // for iterator
    uint32_t GetMaxIdxSize() { 
        return m_pstMemIdxInfo->iUseBlock * MAX_OFFSET; 
    }

    MemKey_t *GetByIdx(uint32_t iIdx) {
        uint32_t iBlockID = iIdx / MAX_OFFSET;
        uint32_t iOffset = iIdx % MAX_OFFSET;

        return (MemKey_t *)&m_aKeyBlock[iBlockID]->aKey[iOffset];
    }

    MemKey_t *SafeGetByIdx(uint32_t iIdx) {
        uint32_t iBlockID = iIdx / MAX_OFFSET;
        uint32_t iOffset = iIdx % MAX_OFFSET;
        if (NULL == m_aKeyBlock[iBlockID]) {
            return NULL;
        }

        assert(NULL != m_aKeyBlock[iBlockID]);
        return reinterpret_cast<MemKey_t *>(
            &(m_aKeyBlock[iBlockID]->aKey[iOffset]));
    }

    uint32_t GetKeyCnt() {
        uint32_t iTotalKey = 0;
        for (uint32_t i = 0; i < m_pstMemIdxInfo->iUseBlock; i++)
            iTotalKey += m_aKeyBlock[i]->iUseCnt;
        return iTotalKey;
    }

    int Visit(clsMemBaseVisitor *pVisitor, unsigned long long idx);

    //    private:
    int DelShm(uint32_t iShmKey);
    int InitBlock(int iBlockID);
    KeyNodePtr BlockAlloc(int iBlockID);
    KeyNodePtr Alloc();
    void Free(KeyNodePtr &stFree);
    void MemCpy(KeyNodePtr &stPtr, MemKey_t &stKey);
    void MemCpy(MemKey_t &stKey, KeyNodePtr &stPtr);
    bool KeyNodeCmp(const char *sKey, uint8_t cKeyLen, KeyNodePtr stPtr);
    KeyNodePtr ListFind(
            const char *sKey, uint8_t cKeyLen, KeyNodePtr stHead);
    KeyNodePtr ListDel(
            const char *sKey, uint8_t cKeyLen, KeyNodePtr stHead);

    uint32_t HashFunc(const char* sKey, uint8_t cKeyLen);

    uint32_t m_iShmKey;
    MemIdxInfo_t *m_pstMemIdxInfo;
    HashHead_t *m_pstHead;
    KeyBlock_t *m_aKeyBlock[MAX_BLOCK];
    dbcomm::HashBaseLock *m_poAllocLock;
    AllocSpinLock *m_poSpinLock;
};

}  // namespace memkv
