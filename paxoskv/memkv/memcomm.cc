
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "memcomm.h"
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/shm.h>
#include <unistd.h>
#include <cassert>
#include "core/paxos.pb.h"
#include "core/plog_helper.h"
#include "cutils/log_utils.h"
#include "dbcomm/db_comm.h"
#include "mem_compressor.h"

namespace memkv {

void* KVGetShm(int key, size_t size, int flag) {
    int shmid = 0;
    void* sshm = NULL;

    shmid = shmget(key, size, flag);

    logerr("KVGetShm shmget key 0x%x %u size %lu flag %o ret %d", 
            key, key, size, flag, shmid);
    if (shmid < 0) {
        logerr("shmget %s", strerror(errno));
        printf("shmget %s\n", strerror(errno));
        return NULL;
    }
    sshm = shmat(shmid, (const void*)NULL, 0);
    logerr("KVGetShm shmget key 0x%x %u size %lu flag %o "
            "shmat id %u ret %p\n", key, key, size, flag, shmid, sshm);

    if (sshm == (void*)-1) {
        logerr("shmat %s", strerror(errno));
        printf("shmat %s\n", strerror(errno));
        return NULL;
    }
    return sshm;
}

int KVGetShm2(void** pshm, int shmkey, size_t size, int flag) {
    void* sshm = NULL;
    if (!(sshm = KVGetShm(shmkey, size, flag & (~IPC_CREAT)))) {
        if (!(flag & IPC_CREAT)) return -1;
        if (!(sshm = KVGetShm(shmkey, size, flag))) {
            logerr("KVGetShm2 IPC_CREAT Error");
            usleep(10);
            if (!(sshm = KVGetShm(shmkey, size, flag & (~IPC_CREAT))))
                return -1;
        } else
            memset(sshm, 0, size);
    }
    *pshm = sshm;
    return 0;
}

int KVGetShm_NoMemSet(void** pshm, int shmkey, size_t size, int flag) {
    void* sshm = NULL;
    if (!(sshm = KVGetShm(shmkey, size, flag & (~IPC_CREAT)))) {
        if (!(flag & IPC_CREAT)) return -1;
        if (!(sshm = KVGetShm(shmkey, size, flag))) {
            logerr("KVGetShm2 IPC_CREAT Error");
            usleep(10);
            if (!(sshm = KVGetShm(shmkey, size, flag & (~IPC_CREAT))))
                return -1;
        }
    }
    *pshm = sshm;
    return 0;
}

int KVDelShm(int iKey, size_t iSize, void* pShm) {
    int ret = 0;
    int iShmId = 0;
    if ((iShmId = shmget(iKey, iSize, 0666)) < 0) {
        logerr("shmget %d %lu %s", iKey, iSize, strerror(errno));
        return -1;
    }
    ret = shmdt(pShm);
    if (ret < 0) {
        logerr("shmdt %s", strerror(errno));
        return -1;
    }

    ret = shmctl(iShmId, IPC_RMID, NULL);
    if (ret < 0) {
        logerr("shmctl %s", strerror(errno));
        return -1;
    }
    return 0;
}

int KVDelShm(int iKey) {
    int ret = 0;
    int iShmId = 0;
    if ((iShmId = shmget(iKey, 1, 0666)) < 0) {
        logerr("shmget %d %u %s", iKey, 1, strerror(errno));
        return -1;
    }
    ret = shmctl(iShmId, IPC_RMID, NULL);
    if (ret < 0) {
        logerr("shmctl %s", strerror(errno));
        return -1;
    }
    return 0;
}

HeadWrapper::HeadWrapper(NewHead_t* pHead) : m_pHead(NULL), m_bLiteHead(false) {
    assert(pHead != NULL);

    pLogID = &(pHead->llLogID);
    pFlag = &(pHead->cFlag);
    pFileNo = &(pHead->tBasicInfo.iFileNo);
    pDataLen = &(pHead->iDataLen);
    pData = pHead->pData;

    m_pHead = (void*)pHead;
    m_bLiteHead = false;
}

HeadWrapper::HeadWrapper(NewHeadLite_t* pHead)
    : m_pHead(NULL), m_bLiteHead(false) {
    assert(pHead != NULL);

    pLogID = &(pHead->llLogID);
    pFlag = &(pHead->cFlag);
    pFileNo = &(pHead->iFileNo);
    pDataLen = &(pHead->iDataLen);
    pData = pHead->pData;

    m_pHead = (void*)pHead;
    m_bLiteHead = true;
}

HeadWrapper::HeadWrapper() : m_pHead(NULL), m_bLiteHead(false) {}

const HeadWrapper HeadWrapper::Null = HeadWrapper();

void HeadWrapper::SetBasicInfo(const NewBasic_t& tBasicInfo) {
    if (IsNull()) {
        return;
    } else if (m_bLiteHead == false) {
        NewHead_t* pHead = (NewHead_t*)m_pHead;
        pHead->tBasicInfo = tBasicInfo;
    } else if (m_bLiteHead == true) {
        *pFileNo = tBasicInfo.iFileNo;
    }
}

int HeadWrapper::GetBasicInfo(NewBasic_t& tBasicInfo) const {
    memset(&tBasicInfo, 0, sizeof(tBasicInfo));
    if (IsNull()) {
        return 0;
    }

    if (m_bLiteHead == false) {
        NewHead_t* pHead = (NewHead_t*)m_pHead;
        tBasicInfo = pHead->tBasicInfo;
        return 0;
    }

    paxos::PaxosLog oPLog;
    auto ret = memkv::NewHeadToPlog(*this, oPLog);
    if (0 != ret) {
        logerr("NewHeadToPlog ret %d", ret);
        assert(0 > ret);
        return ret;
    }

    assert(0 == ret);
    paxos::DBData oData;
    auto chosen_ins = paxos::get_chosen_ins(oPLog);
    if (nullptr != chosen_ins) {
        assert(chosen_ins->has_accepted_value());
        if (false ==
            oData.ParseFromString(chosen_ins->accepted_value().data())) {
            auto& str = chosen_ins->accepted_value().data();
            printf("LiteHead parse DBData failed. size: %zu, data: %s\n",
                   str.size(), str.data());
            logerr("LiteHead parse DBData failed. size: %zu, data: %s",
                   str.size(), str.data());
            return -1;
        }

        tBasicInfo.llMaxIndex = chosen_ins->index();
        tBasicInfo.llChosenIndex = chosen_ins->index();
        tBasicInfo.llReqID = chosen_ins->accepted_value().reqid();
        tBasicInfo.iVersion = oData.version();
        tBasicInfo.cFlag = oData.flag();
    }

    tBasicInfo.iFileNo = *pFileNo;

    // max index
    auto max_ins = paxos::get_max_ins(oPLog);
    if (nullptr != max_ins) {
        tBasicInfo.llMaxIndex = max_ins->index();
        tBasicInfo.cState = PENDING;
    }

    assert(tBasicInfo.llMaxIndex >= tBasicInfo.llChosenIndex);
    return 0;
}

bool HeadWrapper::IsNull() const { return m_pHead == NULL; }

size_t HeadWrapper::HeadSize() const {
    if (m_bLiteHead) {
        return sizeof(NewHeadLite_t);
    }
    return sizeof(NewHead_t);
}

size_t HeadWrapper::RecordSize() const {
    if (m_bLiteHead) {
        return sizeof(NewHeadLite_t) + (*pDataLen) + 2;
    }
    return sizeof(NewHead_t) + (*pDataLen) + 2;
}

bool HeadWrapper::IsLiteHead() const { return m_bLiteHead; }

const void* HeadWrapper::Ptr() const { return m_pHead; }

void* HeadWrapper::Ptr() { return m_pHead; }


}  // namespace memkv
