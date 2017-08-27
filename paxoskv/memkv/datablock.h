
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <string>
#include <vector>
#include "memcomm.h"

namespace paxos {

class PaxosLog;
}  // namespace paxos

namespace dbcomm {

class HashBaseLock;

}  // namespace dbcomm

namespace memkv {

class clsDataBlock {
   public:
    clsDataBlock(uint32_t iShmKey, dbcomm::HashBaseLock* pHashBaseLock);
    ~clsDataBlock();

    int Init(uint32_t iBlockID);

    int Attach(uint32_t iBlockID);

    int InitReadOnly(uint32_t iBlockID);

    void SetUseSize(uint32_t iUseSize);


    uint8_t GetStatus();

    uint32_t GetUseSize();

    void SetStatus(uint8_t cStatus);

    uint32_t GetBlockID();

    void SetWritingFlag();
    void SetNotWritingFlag();

    bool IsWriting();

    void SetUseKeyCnt(uint32_t iUseKeyCnt);
    void SetTotalKeyCnt(uint32_t iTotalKeyCnt);
    uint32_t GetUseRatio();

    // add for paxos
    int AppendSet(uint64_t llLogID, 
            const NewBasic_t& tBasicInfo, 
            const paxos::PaxosLog& oPLog, uint32_t& iNewOffset);

    int BatchAppendSet(
            const std::vector<uint64_t>& vecLogID, 
            const std::vector<NewBasic_t*>& vecBasicInfo,
            const std::vector<paxos::PaxosLog*>& vecPLog,
            std::vector<uint32_t>& vecNewOffset);

    HeadWrapper GetHead(uint32_t iOffset, uint64_t llLogID);

    int GetRecordSkipErr(
            uint32_t iOffset, HeadWrapper& oHead, uint32_t& iSkipLen);

    int GetRecord(uint32_t iOffset, HeadWrapper& oHead);

    // TODO: for merge
    int AppendSetRecord(const HeadWrapper& oHead, uint32_t& iNewOffset);

    void ReportDelOneKey();

    // end add for paxos

    std::string GetBlockInfo() const;

   private:

    int Detach();

   public:
    enum { EMPTY = 0, FULL = 1, APPEND = 2, MERGEFROM = 3, MERGETO = 4 };

    enum {
        No_Space = 1,
        Too_Large = 2,
        Invalid_Offset = -100,
        Invalid_Record_Len = -101,
        Key_Not_Match = -102,
        Datalen_Not_Match = -103,
        Invalid_Input = -104,
        Invalid_KeyLen = -105,
        Value_Delete = -106,
        Invalid_ReachEnd = -107,
        Invalid_Status = -108,
    };

   private:
    typedef struct s_BlockMeta {
        uint32_t iUsedSize;
        uint32_t iBlockID;
        uint32_t iUseKeyCnt;
        uint32_t iTotalKeyCnt;
        uint8_t cStatus;
        char sReserve[128 - 4 * sizeof(uint32_t) - sizeof(uint8_t)];
    } __attribute__((packed)) BlockMeta_t;

    BlockMeta_t* m_pBlockMeta;
    dbcomm::HashBaseLock* m_pHashBaseLock;
    char* m_pBlock;
    uint32_t m_iShmKey;
    uint32_t m_iMaxBlockSize;
    bool m_bWriting;
};

}  // namespace memkv
