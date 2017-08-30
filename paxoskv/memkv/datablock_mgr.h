
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>


#define MAX_APPEND_BLOCK_NUM 10


namespace dbcomm {

class HashBaseLock;

} // dbcomm

namespace memkv {

class clsDataBlock;

class clsDataBlockMgr {
   public:
    clsDataBlockMgr(uint32_t iBaseShmKey);
    ~clsDataBlockMgr();

    int Init(const char* sLockPath =
            "/home/qspace/data/kvsvr/memkv/datablockmgr.lock",
             uint32_t iMaxAppendBlockNum = 2);

    int InitReadOnly();

    int UpdateReadOnly();

    clsDataBlock* GetByIdx(uint32_t iBlockID);

    clsDataBlock* GetMergeFrom(uint32_t iStartBlockID);

    clsDataBlock* GetMergeTo();

    clsDataBlock* Alloc();

    void Free(clsDataBlock* pDataBlock);

    uint32_t GetBlockNum();

    clsDataBlock* GetAppendBlock();

    // new
    clsDataBlock* GetAppendBlockNew();
    // end of new

    clsDataBlock* GetNotWritingAppendBlock();

    void CleanWritingFlag();

    bool GetLoadFlag();

    void SetLoadFlag();

#if defined(SMALL_MEM)
    enum { MAX_BLOCK_NUM = 50 };
#else
    enum { MAX_BLOCK_NUM = 256 }; 
#endif

   private:
    int Detach();

    clsDataBlock* GetByStatus(uint8_t cStatus, uint32_t iStartBlockID);

    clsDataBlock* AllocNotLock(bool bCheckMemSpace = true);

    clsDataBlock* GetLeastFullBlock();

    clsDataBlock* GetAppendBlockNoLock(size_t idx);

   private:
    typedef struct s_GlobalMeta {
        uint32_t iBlockNum;
        uint32_t iLoadFlag;
        char sReserve[256 - sizeof(uint32_t) - sizeof(uint32_t)];
    } __attribute__((packed)) GlobalMeta_t;

    uint32_t m_iBaseShmKey;

    clsDataBlock** m_ppDataBlock;
    GlobalMeta_t* m_pGlobalMeta;
    dbcomm::HashBaseLock* m_pHashBaseLock;

    uint32_t m_iStartBlockID;

    uint32_t m_iMaxAppendBlockNum;
    int m_arrAppendBlock[MAX_APPEND_BLOCK_NUM];
};

}  // namespace memkv
