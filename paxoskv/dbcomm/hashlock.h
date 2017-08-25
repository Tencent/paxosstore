
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <pthread.h>
#include <stdint.h>
#include <vector>
#include <set>

namespace dbcomm {

class HashBaseLock {
public:
    HashBaseLock();
    ~HashBaseLock();

    int Init(const char *sLockPath, uint32_t iLockCount);
    int Attach(const char *sPath, uint32_t iLockCount);
    int Init(uint32_t iLockCount);

    int ReadLock(uint32_t iHash);
    int WriteLock(uint32_t iHash);
    int TryWriteLock(uint32_t iHash);

    int WriteLock(uint32_t iHash, const char *sFile, uint32_t iLine);
    int UnLock(uint32_t iHash);

    int BatchReadLock(std::vector<uint32_t> &tHashVec);
    int BatchWriteLock(std::vector<uint32_t> &tHashVec);
    int BatchTryWriteLock(
            std::vector<uint32_t> &tHashVec,
            std::set<uint32_t> &tFailSet);

    int BatchUnLock(std::vector<uint32_t> &tHashVec);

    uint32_t GetLockCount();

private:
    uint32_t m_iLockCount = 0;
    char m_sLockPath[256] = {0};
    int m_iShareMem = -1;
    int m_shm_id = -1;

    pthread_rwlock_t *m_pstRWLock = nullptr;
    // pthread_mutex_t* m_pMutex = nullptr;
};


class HashLock {
   public:
    HashLock(HashBaseLock *poHashBaseLock, uint32_t iHash);
    HashLock(HashBaseLock *poHashBaseLock, 
            const std::set<uint32_t> &iHashSet);
    HashLock(HashBaseLock *poHashBaseLock, uint64_t llLogID);
    ~HashLock();

    int ReadLock();
    int WriteLock();
    int WriteLock(const char *sFile, uint32_t iLine);
    int TryWriteLock();

    int BatchReadLock();
    int BatchWriteLock();
    int BatchTryWriteLock(std::set<uint32_t> &iFailSet);

    bool HasLock() const { return m_bLock; }

   private:
    HashBaseLock *m_poHashBaseLock;
    uint32_t m_iHash;
    std::vector<uint32_t> m_iHashVec;
    volatile bool m_bLock;
    bool m_bBatchLock;
};




}  // namespace dbcomm
