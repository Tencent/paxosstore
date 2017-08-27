
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <assert.h>
#include <errno.h>
#include <fcntl.h> /*  For O_* constants */
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include "hashlock.h"
#include "cutils/log_utils.h"
#include "cutils/hash_utils.h"

namespace dbcomm {


HashBaseLock::HashBaseLock() = default;

HashBaseLock::~HashBaseLock() {
    if (m_iShareMem) {
        munmap(m_pstRWLock, sizeof(pthread_rwlock_t) * m_iLockCount);
    } else {
        free(m_pstRWLock);
    }

    if (m_shm_id > 0) {
        close(m_shm_id);
        m_shm_id = -1;
    }
}

int HashBaseLock::Init(const char *sPath, uint32_t iLockCount) {
    uint32_t i = 0;
    int ret = 0;
    uint32_t iLockNum = 0;

    pthread_rwlockattr_t attr;

    iLockNum = cutils::bkdr_hash(sPath);
    snprintf(m_sLockPath, sizeof(m_sLockPath), "/%u.kvlock", iLockNum);
    logerr("kvlock: %s %u", sPath, iLockNum);

    ret = pthread_rwlockattr_init(&attr);
    if (ret != 0) return -1;
    ret = pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    if (ret != 0) return -2;

    m_iLockCount = iLockCount;

    m_shm_id = shm_open(m_sLockPath, O_RDWR | O_CREAT, 0664);
    if (m_shm_id < 0) {
        logerr("ERROR: shm_open(%s) %s", sPath, strerror(errno));
        return -3;
    }
    ftruncate(m_shm_id, sizeof(pthread_rwlock_t) * iLockCount);

    m_pstRWLock = (pthread_rwlock_t *)mmap(
        NULL, sizeof(pthread_rwlock_t) * iLockCount, PROT_READ | PROT_WRITE,
        MAP_SHARED, m_shm_id, 0);

    if (m_pstRWLock == MAP_FAILED) {
        logerr("mmap failed %s", strerror(errno));
        return -4;
    }

    for (i = 0; i < iLockCount; i++) {
        ret = pthread_rwlock_init(&m_pstRWLock[i], &attr);
        if (ret != 0) return -5;
    }

    // pthread_mutex_init(m_pMutex, NULL);
    return 0;
}

int HashBaseLock::Attach(const char *sPath, uint32_t iLockCount) {
    uint32_t i = 0;
    int shm_id = 0;
    int ret = 0;
    uint32_t iLockNum = 0;
    pthread_rwlockattr_t attr;

    iLockNum = cutils::bkdr_hash(sPath);
    snprintf(m_sLockPath, sizeof(m_sLockPath), "/%u.rkvlock", iLockNum);
    logerr("kvlock: %s %u", sPath, iLockNum);

    ret = pthread_rwlockattr_init(&attr);
    if (ret != 0) return -1;

#if !defined(__APPLE__)
    ret = pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    if (ret != 0) return -2;
#endif

    m_iLockCount = iLockCount;

    shm_id = shm_open(m_sLockPath, O_RDWR | O_CREAT, 0664);
    if (shm_id < 0) {
        logerr("ERROR: shm_open(%s %u ) %s", sPath, iLockNum,
                     strerror(errno));
        return -3;
    }

    ftruncate(shm_id, sizeof(pthread_rwlock_t) * iLockCount);

    m_pstRWLock =
        (pthread_rwlock_t *)mmap(
                NULL, sizeof(pthread_rwlock_t) * iLockCount, 
                PROT_READ | PROT_WRITE, MAP_SHARED, shm_id, 0);
    if (m_pstRWLock == MAP_FAILED) {
        logerr("mmap failed %s", strerror(errno));
        return -4;
    }

    for (i = 0; i < iLockCount; i++) {
        ret = pthread_rwlock_init(&m_pstRWLock[i], &attr);
        if (ret != 0) return -5;
    }

    return 0;
}

int HashBaseLock::Init(uint32_t iLockCount) {
    uint32_t i = 0;
    int ret = 0;

    m_iShareMem = 0;

    pthread_rwlockattr_t attr;

    ret = pthread_rwlockattr_init(&attr);
    if (ret != 0) return -1;

    m_iLockCount = iLockCount;

    m_pstRWLock =
        (pthread_rwlock_t *)malloc(sizeof(pthread_rwlock_t) * iLockCount);
    if (m_pstRWLock == NULL) {
        logerr("mmap failed %s", strerror(errno));
        return -2;
    }

    for (i = 0; i < iLockCount; i++) {
        ret = pthread_rwlock_init(&m_pstRWLock[i], &attr);
        if (ret != 0) return -3;
    }
    return 0;
}

int HashBaseLock::ReadLock(uint32_t iHash) {
    int ret = 0;
    ret = pthread_rwlock_rdlock(&m_pstRWLock[iHash % m_iLockCount]);
    if (ret != 0) {
        logerr("pthread_rwlock_rdlock( iHash %u m_iLockCount %u) ret %d %s ",
               iHash, m_iLockCount, ret, strerror(errno));
        assert(0);
    }
    return ret;
}

int HashBaseLock::WriteLock(uint32_t iHash) {
    int ret = 0;
    ret = pthread_rwlock_wrlock(&m_pstRWLock[iHash % m_iLockCount]);
    if (ret != 0) {
        logerr("pthread_rwlock_wrlock( iHash %u m_iLockCount %u) ret %d %s ",
               iHash, m_iLockCount, ret, strerror(errno));
        assert(0);
    }
    return 0;
}

int HashBaseLock::TryWriteLock(uint32_t iHash) {
    int ret = pthread_rwlock_trywrlock(&m_pstRWLock[iHash % m_iLockCount]);
    return ret;
}

int HashBaseLock::UnLock(uint32_t iHash) {
    assert(pthread_rwlock_unlock(&m_pstRWLock[iHash % m_iLockCount]) == 0);
    return 0;
}

int HashBaseLock::BatchReadLock(std::vector<uint32_t> &tHashVec) {
    int ret = 0;
    int i = 0;
    for (i = 0; i < (int)tHashVec.size(); i++) {
        ret = pthread_rwlock_rdlock(&m_pstRWLock[tHashVec[i]]);
        if (ret != 0) {
            logerr(
                "BatchReadLock pthread_rwlock_rdlock( iHash %u m_iLockCount "
                "%u) ret %d",
                tHashVec[i], m_iLockCount, ret);
            assert(0);
        }
    }
    return ret;
}

int HashBaseLock::BatchWriteLock(std::vector<uint32_t> &tHashVec) {
    int ret = 0;
    int i = 0;
    for (i = 0; i < (int)tHashVec.size(); i++) {
        ret = pthread_rwlock_wrlock(&m_pstRWLock[tHashVec[i]]);
        if (ret != 0) {
            logerr(
                "BatchWriteLock pthread_rwlock_wrlock( iHash %u m_iLockCount "
                "%u) ret %d",
                tHashVec[i], m_iLockCount, ret);
            assert(0);
        }
    }
    return ret;
}

int HashBaseLock::BatchTryWriteLock(std::vector<uint32_t> &tHashVec,
                                    std::set<uint32_t> &tFailSet) {
    assert(tFailSet.empty());

    for (size_t i = 0; i < tHashVec.size(); i++) {
        int ret = pthread_rwlock_trywrlock(&m_pstRWLock[tHashVec[i]]);
        if (ret == EBUSY || ret == EDEADLK) {
            tFailSet.insert(tHashVec[i]);
        } else if (ret != 0) {
            logerr(
                "BatchTryWriteLock pthread_rwlock_trywrlock( iHash %u "
                "m_iLockCount %u) ret %d",
                tHashVec[i], m_iLockCount, ret);
            assert(0);
        }
    }

    if (!tFailSet.empty()) {
        if (tFailSet.size() == tHashVec.size()) {
            tHashVec.clear();
        } else {
            std::vector<uint32_t> tmp;
            tmp.reserve(tHashVec.size() - tFailSet.size());
            for (size_t i = 0; i < tHashVec.size(); i++) {
                if (tFailSet.count(tHashVec[i]) == 0) {
                    tmp.push_back(tHashVec[i]);
                }
            }
            tHashVec.swap(tmp);
        }
    }

    return 0;
}

int HashBaseLock::BatchUnLock(std::vector<uint32_t> &tHashVec) {
    for (int i = (int)tHashVec.size() - 1; i >= 0; i--) {
        assert(pthread_rwlock_unlock(&m_pstRWLock[tHashVec[i]]) == 0);
    }
    return 0;
}

uint32_t HashBaseLock::GetLockCount() { return m_iLockCount; }


HashLock::HashLock(HashBaseLock *poHashBaseLock, uint32_t iHash) {
    m_poHashBaseLock = poHashBaseLock;
    m_iHash = iHash;
    m_bLock = false;
    m_bBatchLock = false;
}

HashLock::HashLock(
        HashBaseLock *poHashBaseLock, 
        const std::set<uint32_t> &iHashSet) 
{
    m_poHashBaseLock = poHashBaseLock;
    if (!iHashSet.empty()) {
        std::set<uint32_t>::iterator it = iHashSet.begin();
        for (; it != iHashSet.end(); ++it)
            m_iHashVec.push_back(*it % m_poHashBaseLock->GetLockCount());
        std::sort(m_iHashVec.begin(), m_iHashVec.end());
        m_iHashVec.erase(
                std::unique(m_iHashVec.begin(), m_iHashVec.end()), 
                m_iHashVec.end());
    }
    m_bLock = false;
    m_bBatchLock = false;
}

HashLock::HashLock(
        HashBaseLock *poHashBaseLock, 
        uint64_t llLogID)
{
    m_poHashBaseLock = poHashBaseLock;
    m_iHash = cutils::dict_int_hash_func(llLogID);
    m_bLock = false;
    m_bBatchLock = false;
}

HashLock::~HashLock() {
    if (!m_bLock) {
        return;
    }
    m_bLock = false;
    if (m_bBatchLock) {
        m_poHashBaseLock->BatchUnLock(m_iHashVec);
    } else {
        m_poHashBaseLock->UnLock(m_iHash);
    }
}

int HashLock::ReadLock() {
    if (m_bLock) {
        assert(false);
    }
    m_bLock = true;

    return m_poHashBaseLock->ReadLock(m_iHash);
}

int HashLock::WriteLock(const char *sFile, uint32_t iLine) {
    return WriteLock();
}

int HashLock::WriteLock() {
    if (m_bLock) {
        assert(false);
    }
    m_bLock = true;

    return m_poHashBaseLock->WriteLock(m_iHash);
}

int HashLock::TryWriteLock() {
    if (m_bLock) {
        assert(0);
    }

    int iRet = m_poHashBaseLock->TryWriteLock(m_iHash);
    m_bLock = (iRet == 0);

    return iRet;
}

int HashLock::BatchWriteLock() {
    if (m_bLock) {
        assert(0);
    }
    m_bLock = true;
    m_bBatchLock = true;

    return m_poHashBaseLock->BatchWriteLock(m_iHashVec);
}

int HashLock::BatchTryWriteLock(std::set<uint32_t> &iFailSet) {
    if (m_bLock) {
        assert(0);
    }

    m_bLock = true;
    m_bBatchLock = true;

    int iRet = -1;
    iRet = m_poHashBaseLock->BatchTryWriteLock(m_iHashVec, iFailSet);

    return iRet;
}

int HashLock::BatchReadLock() {
    if (m_bLock) {
        assert(false);
    }
    m_bLock = true;
    m_bBatchLock = true;

    return m_poHashBaseLock->BatchReadLock(m_iHashVec);
}

} // namespace dbcomm


