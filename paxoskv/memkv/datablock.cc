
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sstream>
#include "datablock.h"
#include "core/err_code.h"
#include "core/paxos.pb.h"
#include "core/plog_helper.h"
#include "cutils/log_utils.h"
#include "cutils/hassert.h"
#include "dbcomm/db_comm.h"
#include "dbcomm/hashlock.h"
#include "mem_compressor.h"
#include "memsize_mng.h"
#include "mem_assert.h"

static const uint8_t g_cStartFlag = 0x12;
static const uint8_t g_cLiteStartFlag = 0x13;
static const uint8_t g_cEndFlag = 0x34;

static uint32_t g_iNewMinRecordSize =
    sizeof(memkv::NewHead_t) + sizeof(g_cStartFlag) + sizeof(g_cEndFlag);

static uint32_t g_iLiteMinRecordSize =
    sizeof(memkv::NewHeadLite_t) + sizeof(g_cLiteStartFlag) + sizeof(g_cEndFlag);

#define RECORD_SIZE(pHead) \
    (g_iMinRecordSize + (pHead)->iDataLen + (pHead)->iReserveLen)

namespace {

using namespace memkv;

inline uint32_t 
NewRecordSize(const memkv::NewHead_t& tHead) {
    return g_iNewMinRecordSize + tHead.iDataLen;
}

inline uint32_t 
NewRecordSize(int iNewDataLen) {
    assert(0 <= iNewDataLen);
    return g_iNewMinRecordSize + iNewDataLen;
}

inline memkv::NewHead_t* 
NewGetHeadImpl(char* pBlock, uint32_t iOffset) {
    return reinterpret_cast<memkv::NewHead_t*>(
            pBlock + iOffset + sizeof(g_cStartFlag));
}

inline const memkv::NewHead_t* 
NewGetHeadImpl(const char* pBlock, uint32_t iOffset) {
    return NewGetHeadImpl(const_cast<char*>(pBlock), iOffset);
}

inline bool IsStartFlag(uint8_t flag) {
    return flag == g_cStartFlag || flag == g_cLiteStartFlag;
}

inline memkv::HeadWrapper 
GetHeadWrapper(char* pBlock, uint32_t iOffset) {
    uint8_t startFlag = (uint8_t)pBlock[iOffset];
    if (startFlag == g_cStartFlag) {
        auto pHead = reinterpret_cast<
            memkv::NewHead_t*>(pBlock + iOffset + sizeof(g_cStartFlag));
        return memkv::HeadWrapper(pHead);
    }
    
    if (startFlag == g_cLiteStartFlag) {
        auto pHead = reinterpret_cast<
            memkv::NewHeadLite_t*>(
                    pBlock + iOffset + sizeof(g_cLiteStartFlag));
        return memkv::HeadWrapper(pHead);
    } 
    
    return memkv::HeadWrapper::Null;
}

inline const memkv::HeadWrapper 
GetHeadWrapper(const char* pBlock, uint32_t iOffset) {
    return GetHeadWrapper(const_cast<char*>(pBlock), iOffset);
}

int NewIsRecordOK(const char* pBlock, uint32_t iUsedSize, uint32_t iOffset,
                  const char* sFunc) {
    assert(NULL != pBlock);
    if (iOffset + g_iLiteMinRecordSize > iUsedSize) {
        return clsDataBlock::Invalid_ReachEnd;
    }

    const memkv::HeadWrapper oHead = GetHeadWrapper(pBlock, iOffset);
    if (oHead.IsNull()) {
        return clsDataBlock::Invalid_Offset;
    }

    uint32_t iRecordLen = oHead.RecordSize();
    if (iOffset + iRecordLen > iUsedSize) {
        logerr("iOffset %u iRecordLen %u iUsedSize %u %s", iOffset, iRecordLen,
               iUsedSize, sFunc);
        return clsDataBlock::Invalid_Record_Len;
    }

    uint8_t cEndFlag = pBlock[iOffset + iRecordLen - sizeof(g_cEndFlag)];
    if (g_cEndFlag != cEndFlag) {
        logerr("iOffset %u iRecordLen %u cEndFlag %x %s", iOffset, iRecordLen,
               cEndFlag, sFunc);
        return clsDataBlock::Invalid_Offset;
    }

    return 0;
}

int NewValidCheck(const char* pBlock, uint32_t iUsedSize, uint32_t iOffset,
                  uint64_t llLogID, const char* sFunc) {
    int ret = NewIsRecordOK(pBlock, iUsedSize, iOffset, sFunc);
    if (0 != ret) {
        logerr("NewIsRecordOK pBlock %p iUsedSize %u iOffset %u ret %d %s",
               pBlock, iUsedSize, iOffset, ret, sFunc);
        return ret;
    }

    const HeadWrapper oHead = GetHeadWrapper(pBlock, iOffset);
    assert(false == oHead.IsNull());

    if (llLogID != *oHead.pLogID) {
        logerr(
            "Key Not Match pBlock %p iOffset %u "
            "pHead->llLogID %lu llLogID %lu",
            pBlock, iOffset, *oHead.pLogID, llLogID);
        return clsDataBlock::Key_Not_Match;
    }

    if (FLAG_DELETE & (*oHead.pFlag)) {
        logerr("pBlock %p iOffset %u llLogID %lu sFunc DeleteFlag %s", pBlock,
               iOffset, llLogID, sFunc);
        return clsDataBlock::Value_Delete;
    }

    /*
    if (pHead->tBasicInfo.llMaxIndex != pHead->tBasicInfo.llChosenIndex)
    {
            assert(dbcomm::TestFlag(pHead->tBasicInfo.cState, PENDING));
    }
    else
    {
            assert(false == dbcomm::TestFlag(pHead->tBasicInfo.cState,
    PENDING));
    }
    */

    return 0;
}

int AppendDumpRecord(char* pBlock, uint32_t iNewOffset, uint64_t llLogID,
                     const NewBasic_t& tBasicInfo,
                     const paxos::PaxosLog& oPLog) {
    assert(false == dbcomm::TestFlag(
                tBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
    assert(0 <= oPLog.ByteSize());
    AssertCheck(tBasicInfo, oPLog);

    if (enable_mem_compresse()) {
        pBlock[iNewOffset] = g_cLiteStartFlag;
    } else {
        pBlock[iNewOffset] = g_cStartFlag;
    }

    HeadWrapper oHead = GetHeadWrapper(pBlock, iNewOffset);
    assert(oHead.IsNull() == false);

    memset(oHead.Ptr(), 0, oHead.HeadSize());
    (*oHead.pLogID) = llLogID;
    oHead.SetBasicInfo(tBasicInfo);

    int ret = memkv::PLogToNewHead(oPLog, oHead);
    if (ret != 0) {
        logerr("Error! memkv::PLogToNewHead return %d.", ret);
        return -1;
    }

    pBlock[iNewOffset + oHead.RecordSize() - sizeof(g_cEndFlag)] = g_cEndFlag;

    return (int)oHead.RecordSize();
}

}  // namespace


namespace memkv {

clsDataBlock::clsDataBlock(
        uint32_t iShmKey, dbcomm::HashBaseLock* pHashBaseLock) {
    m_pBlock = NULL;
    m_pBlockMeta = NULL;
    m_iShmKey = iShmKey;
    m_iMaxBlockSize = 0;
    m_pHashBaseLock = pHashBaseLock;
    m_bWriting = false;
}

clsDataBlock::~clsDataBlock() { Detach(); }

void clsDataBlock::SetWritingFlag() { m_bWriting = true; }

void clsDataBlock::SetNotWritingFlag() { m_bWriting = false; }

bool clsDataBlock::IsWriting() { return m_bWriting; }

int clsDataBlock::Attach(uint32_t iBlockID) {
    char* p = NULL;

    int iRet = KVGetShm2(
            (void**)&p, m_iShmKey, BLOCK_SIZE, 0666);
    if (iRet < 0) {
        logerr("KVGetShm2: iRet %d ShmKey %u size %u iBlockID %u", iRet,
               m_iShmKey, BLOCK_SIZE, iBlockID);
        return -1;
    }

    m_pBlockMeta = (BlockMeta_t*)p;
    if (m_pBlockMeta->iBlockID != iBlockID) {
        logerr("BlockID not match [%u %u]", 
                m_pBlockMeta->iBlockID, iBlockID);
        return -2;
    }

    m_pBlock = p + sizeof(BlockMeta_t);
    m_iMaxBlockSize = BLOCK_SIZE - sizeof(BlockMeta_t);

    clsMemSizeMng::GetDefault()->AddUseSize(BLOCK_SIZE);
    return 0;
}

int clsDataBlock::Init(uint32_t iBlockID) {
    char* p = NULL;
    if (KVGetShm2(
                (void**)&p, m_iShmKey, BLOCK_SIZE, 0666) < 0) {
        int iRet = KVGetShm_NoMemSet(
                (void**)&p, m_iShmKey, BLOCK_SIZE, 0666 | IPC_CREAT);
        if (iRet < 0) {
            logerr("KVGetShm2: iRet %d ShmKey %u size %u iBlockID %u", 
                    iRet, m_iShmKey, BLOCK_SIZE, iBlockID);
            return -1;
        }

        m_pBlockMeta = (BlockMeta_t*)p;
        memset(m_pBlockMeta, 0, sizeof(BlockMeta_t));
        m_pBlockMeta->iBlockID = iBlockID;
    } else {
        logerr("Block Exist BlockID %u Key %u", iBlockID, m_iShmKey);
        return -2;
    }

    m_pBlock = p + sizeof(BlockMeta_t);
    m_iMaxBlockSize = BLOCK_SIZE - sizeof(BlockMeta_t);

    clsMemSizeMng::GetDefault()->AddUseSize(BLOCK_SIZE);
    return 0;
}

int clsDataBlock::InitReadOnly(uint32_t iBlockID) {
    char* p = NULL;

    if (KVGetShm2(
                (void**)&p, m_iShmKey, BLOCK_SIZE, 0444) < 0) {
        return -1;
    } 

    m_pBlockMeta = (BlockMeta_t*)p;
    if (m_pBlockMeta->iBlockID != iBlockID) {
        logerr("BlockID not match [%u %u]", m_pBlockMeta->iBlockID,
               iBlockID);
        return -2;
    }
    
    m_pBlock = p + sizeof(BlockMeta_t);
    m_iMaxBlockSize = BLOCK_SIZE - sizeof(BlockMeta_t);
    return 0;
}

int clsDataBlock::Detach() {
    if (m_pBlockMeta) {
        shmdt((void*)m_pBlockMeta);
    }
    return 0;
}

uint8_t clsDataBlock::GetStatus() { return m_pBlockMeta->cStatus; }

uint32_t clsDataBlock::GetUseSize() { return m_pBlockMeta->iUsedSize; }

void clsDataBlock::SetUseSize(uint32_t iUseSize) {
    m_pBlockMeta->iUsedSize = iUseSize;
}

void clsDataBlock::SetUseKeyCnt(uint32_t iUseKeyCnt) {
    m_pBlockMeta->iUseKeyCnt = iUseKeyCnt;
}

void clsDataBlock::SetTotalKeyCnt(uint32_t iTotalKeyCnt) {
    m_pBlockMeta->iTotalKeyCnt = iTotalKeyCnt;
}

uint32_t clsDataBlock::GetUseRatio() {
    if (m_pBlockMeta->iTotalKeyCnt == 0) {
        return 0;
    }
    return m_pBlockMeta->iUseKeyCnt * 100 / m_pBlockMeta->iTotalKeyCnt;
}

uint32_t clsDataBlock::GetBlockID() { return m_pBlockMeta->iBlockID; }

void clsDataBlock::SetStatus(uint8_t cStatus) {
    m_pBlockMeta->cStatus = cStatus;
}

int clsDataBlock::AppendSet(
        uint64_t llLogID, 
        const NewBasic_t& tBasicInfo, 
        const paxos::PaxosLog& oPLog, 
        uint32_t& iNewOffset) {
    assert(false == dbcomm::TestFlag(
                tBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
    const int iNewDataMaxLen = memkv::MaxCompressedLength(oPLog);
    assert(0 <= iNewDataMaxLen);

    uint32_t iNewRecordMaxLen = NewRecordSize(iNewDataMaxLen);
    if (MAX_MEM_VALUE_LEN < iNewRecordMaxLen) {
        logerr(
            "key %lu iNewDataLen %d iNewRecordMaxLen %u "
            "MAX_MEM_VALUE_LEN %u",
            llLogID, iNewDataMaxLen, iNewRecordMaxLen, MAX_MEM_VALUE_LEN);
        return Too_Large;
    }

    uint32_t iLockIdx = m_pBlockMeta->iBlockID + 1;
    dbcomm::HashLock hashlock(m_pHashBaseLock, iLockIdx);
    hashlock.WriteLock();
    if (APPEND != m_pBlockMeta->cStatus) {
        logerr("iBlockID %u BlockStatus %d not APPEND", 
                m_pBlockMeta->iBlockID,
               static_cast<int>(m_pBlockMeta->cStatus));
        return No_Space;
    }

    if (m_pBlockMeta->iUsedSize + iNewRecordMaxLen > m_iMaxBlockSize) {
        logerr(
            "iBlockID %u cStatus %d Used %u AddLen %u"
            " MaxBlockSize %u Left %u SET STATUS FULL",
            m_pBlockMeta->iBlockID, 
            static_cast<int>(m_pBlockMeta->cStatus),
            m_pBlockMeta->iUsedSize, 
            iNewRecordMaxLen, m_iMaxBlockSize,
            m_iMaxBlockSize - m_pBlockMeta->iUsedSize);
        m_pBlockMeta->cStatus = FULL;
        return No_Space;
    }

    int iNewRecordLen = AppendDumpRecord(
            m_pBlock, m_pBlockMeta->iUsedSize, 
            llLogID, tBasicInfo, oPLog);
    if (iNewRecordLen < 0) {
        return iNewRecordLen;
    }

    iNewOffset = m_pBlockMeta->iUsedSize;
    m_pBlockMeta->iUsedSize += iNewRecordLen;
    ++(m_pBlockMeta->iTotalKeyCnt);
    ++(m_pBlockMeta->iUseKeyCnt);
    return 0;
}

int clsDataBlock::BatchAppendSet(
        const std::vector<uint64_t>& vecLogID, 
        const std::vector<NewBasic_t*>& vecBasicInfo, 
        const std::vector<paxos::PaxosLog*>& vecPLog, 
        std::vector<uint32_t>& vecNewOffset) {
    assert(false == vecLogID.empty());
    assert(vecLogID.size() == vecBasicInfo.size());
    assert(vecLogID.size() == vecPLog.size());
    assert(vecLogID.size() == vecNewOffset.size());

    uint32_t iTotalRecordMaxLen = 0;
    std::vector<uint32_t> vecRecordMaxLen(vecLogID.size(), 0);
    for (size_t idx = 0; idx < vecLogID.size(); ++idx) {
        assert(NULL != vecBasicInfo[idx]);
        assert(NULL != vecPLog[idx]);
        vecRecordMaxLen[idx] =
            NewRecordSize(MaxCompressedLength(*vecPLog[idx]));
        assert(0 < vecRecordMaxLen[idx]);
        iTotalRecordMaxLen += vecRecordMaxLen[idx];
        assert(false ==
               dbcomm::TestFlag(
                   vecBasicInfo[idx]->cFlag, dbcomm::RECORD_COMPRESSE));
    }

    assert(0 < iTotalRecordMaxLen);
    if (MAX_MEM_VALUE_LEN < iTotalRecordMaxLen) {
        logerr("vecLogID.size %zu iTotalRecordMaxLen %u", 
                vecLogID.size(), iTotalRecordMaxLen);
        return Too_Large;
    }

    uint32_t iLockIdx = m_pBlockMeta->iBlockID + 1;
    dbcomm::HashLock hashlock(m_pHashBaseLock, iLockIdx);
    hashlock.WriteLock();

    if (APPEND != m_pBlockMeta->cStatus) {
        logerr("iBlockID %u BlockStatus %d not APPEND", 
                m_pBlockMeta->iBlockID,
               static_cast<int>(m_pBlockMeta->cStatus));
        return No_Space;
    }

    if (m_pBlockMeta->iUsedSize + iTotalRecordMaxLen > m_iMaxBlockSize) {
        logerr(
            "iBlockID %u cStatus %d Used %u AddMaxLen %u"
            " MaxBlockSize %u Left %u SET STATUS FULL",
            m_pBlockMeta->iBlockID, 
            static_cast<int>(m_pBlockMeta->cStatus),
            m_pBlockMeta->iUsedSize, iTotalRecordMaxLen, m_iMaxBlockSize,
            m_iMaxBlockSize - m_pBlockMeta->iUsedSize);
        m_pBlockMeta->cStatus = FULL;
        return No_Space;
    }

    for (size_t idx = 0; idx < vecLogID.size(); ++idx) {
        int iNewRecordLen =
            AppendDumpRecord(
                    m_pBlock, m_pBlockMeta->iUsedSize, vecLogID[idx], 
                    *(vecBasicInfo[idx]), *(vecPLog[idx]));
        if (iNewRecordLen < 0) {
            logerr("AppendDumpRecord key %lu ret %d", 
                    vecLogID[idx], iNewRecordLen);
            return iNewRecordLen;
        }

        vecNewOffset[idx] = m_pBlockMeta->iUsedSize;
        m_pBlockMeta->iUsedSize += iNewRecordLen;
        ++(m_pBlockMeta->iTotalKeyCnt);
        ++(m_pBlockMeta->iUseKeyCnt);
    }

    return 0;
}

HeadWrapper clsDataBlock::GetHead(
        uint32_t iOffset, uint64_t llLogID) {
    int ret = NewValidCheck(
            m_pBlock, m_pBlockMeta->iUsedSize, 
            iOffset, llLogID, __func__);
    if (0 != ret) {
        logerr("NewValidCheck key %lu iOffset %u ret %d", 
                llLogID, iOffset, ret);
        return HeadWrapper::Null;
    }

    assert(0 == ret);
    return GetHeadWrapper(m_pBlock, iOffset);
}

int clsDataBlock::GetRecordSkipErr(
        uint32_t iOffset, HeadWrapper& oHead, uint32_t& iSkipLen) {
    iSkipLen = 0;
    int ret =
        NewIsRecordOK(m_pBlock, 
                m_pBlockMeta->iUsedSize, iOffset, __func__);
    while (0 != ret && Invalid_ReachEnd != ret) {
        ++iSkipLen;
        ret = NewIsRecordOK(
                m_pBlock, m_pBlockMeta->iUsedSize, 
                iOffset + iSkipLen, __func__);
    }

    assert(0 == ret || Invalid_ReachEnd == ret);
    if (Invalid_ReachEnd == ret) {
        return 0;
    }

    assert(0 == ret);
    assert(0 <= iSkipLen);
    oHead = GetHeadWrapper(m_pBlock, iOffset + iSkipLen);
    assert(oHead.IsNull() == false);
    if (0 != iSkipLen) {
        logerr("m_pBlock %p iOffset %u iSkipLen %d", 
                m_pBlock, iOffset, iSkipLen);
    }

    assert(0 < oHead.RecordSize());
    hassert(0 < static_cast<int>((oHead.RecordSize() + iSkipLen)),
            "BlockID %u offset %u UsedSize %u iMaxBlockSize %u ret %d "
            "RecordSize %zu iSkipLen %u",
            m_pBlockMeta->iBlockID, iOffset, m_pBlockMeta->iUsedSize,
            m_iMaxBlockSize, ret, oHead.RecordSize(), iSkipLen);
    return oHead.RecordSize() + iSkipLen;
}

int clsDataBlock::GetRecord(uint32_t iOffset, HeadWrapper& oHead) {
    int ret =
        NewIsRecordOK(m_pBlock, 
                m_pBlockMeta->iUsedSize, iOffset, __func__);
    if (0 != ret) {
        return 0 > ret ? ret : -1;
    }

    oHead = GetHeadWrapper(m_pBlock, iOffset);
    assert(oHead.IsNull() == false);
    return oHead.RecordSize();
}

// only used for merge
int clsDataBlock::AppendSetRecord(
        const HeadWrapper& oHead, uint32_t& iNewOffset) {
    assert(oHead.IsNull() == false);

    uint32_t iNewRecordLen = oHead.RecordSize();
    if (MAX_MEM_VALUE_LEN < iNewRecordLen) {
        logerr("key %lu iNewRecordLen %d MAX_MEM_VALUE_LEN %u", 
                *oHead.pLogID, iNewRecordLen, MAX_MEM_VALUE_LEN);
        return Too_Large;
    }

    if (MERGETO != m_pBlockMeta->cStatus) {
        logerr("iBlockID %u BlockStatus %d not MERGETO", 
                m_pBlockMeta->iBlockID, 
                static_cast<int>(m_pBlockMeta->cStatus));
        return Invalid_Status;
    }

    if (m_pBlockMeta->iUsedSize + iNewRecordLen > m_iMaxBlockSize) {
        logerr(
            "iBlockID %u MERGETO Used %u AddLen %u"
            " MaxBlockSize %u Left %u SET STATUS FULL",
            m_pBlockMeta->iBlockID, 
            m_pBlockMeta->iUsedSize, iNewRecordLen,
            m_iMaxBlockSize, m_iMaxBlockSize - m_pBlockMeta->iUsedSize);
        m_pBlockMeta->cStatus = FULL;
        return No_Space;
    }

    memcpy(m_pBlock + m_pBlockMeta->iUsedSize, 
            (const char*)oHead.Ptr() - 1, iNewRecordLen);

    iNewOffset = m_pBlockMeta->iUsedSize;
    m_pBlockMeta->iUsedSize += iNewRecordLen;
    ++(m_pBlockMeta->iTotalKeyCnt);
    ++(m_pBlockMeta->iUseKeyCnt);
    return 0;
}

void clsDataBlock::ReportDelOneKey() {
    if (0 == m_pBlockMeta->iUseKeyCnt) {
        return;
    }

    assert(0 < m_pBlockMeta->iUseKeyCnt);
    --(m_pBlockMeta->iUseKeyCnt);
}

std::string clsDataBlock::GetBlockInfo() const {
    std::stringstream ss;

    ss << "META: " << m_pBlockMeta->iBlockID 
       << " " << m_pBlockMeta->iUsedSize
       << " " << m_pBlockMeta->iUseKeyCnt 
       << " " << m_pBlockMeta->iTotalKeyCnt
       << " " << static_cast<int>(m_pBlockMeta->cStatus);

    uint32_t iOffset = 0;

    uint32_t iPendingCnt = 0;
    uint32_t iRealKeyCnt = 0;
    uint32_t iLiteHeadCnt = 0;
    uint32_t iPendingUseSize = 0;
    uint32_t iRealUseSize = 0;
    while (iOffset < m_pBlockMeta->iUsedSize) {
        assert(0 == NewIsRecordOK(
                    m_pBlock, 
                    m_pBlockMeta->iUsedSize, iOffset, __func__));

        HeadWrapper oHead = GetHeadWrapper(m_pBlock, iOffset);
        assert(oHead.IsNull() == false);

        iOffset += oHead.RecordSize();
        if (FLAG_DELETE & (*oHead.pFlag)) {
            continue;
        }

        paxos::PaxosLog oPLog;
        assert(0 == memkv::NewHeadToPlog(oHead, oPLog));

        auto max_ins = paxos::get_max_ins(oPLog);
        if (nullptr != max_ins && false == max_ins->chosen()) {
            ++iPendingCnt;
            iPendingUseSize += max_ins->ByteSize();
        }

        ++iRealKeyCnt;
        iRealUseSize += oHead.RecordSize();

        if (oHead.IsLiteHead()) {
            ++iLiteHeadCnt;
        }
    }

    ss << "  Usage: " << iPendingCnt << " " << iPendingUseSize << " "
       << iRealKeyCnt << " " << iRealUseSize << " " << iLiteHeadCnt;
    return ss.str();
}


} // namespace memkv
