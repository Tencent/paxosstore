
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <sys/time.h>
#include <vector>
#include "memkv.h"
#include "datablock_mgr.h"
#include "datablock.h"
#include "memidx.h"
#include "memcomm.h"
#include "memsize_mng.h"
#include "mem_compressor.h"
#include "mem_assert.h"
#include "core/paxos.pb.h"
#include "core/err_code.h"
#include "core/plog_helper.h"
#include "cutils/log_utils.h"
#include "cutils/hash_utils.h"
#include "cutils/hassert.h"
#include "dbcomm/hashlock.h"
#include "dbcomm/db_comm.h"
#include "comm/kvsvrcomm.h"


//#define NEW_IDX_SHM_KEY 0x20160405
//#define NEW_DATA_BLOCK_SHM_KEY 0x30160405


namespace {

using namespace memkv;

uint32_t HashFunc(const char* sKey, uint8_t cKeyLen) 
{
    assert(sizeof(uint64_t) == static_cast<size_t>(cKeyLen));
    uint64_t llLogID = 0;
    memcpy(&llLogID, sKey, cKeyLen);
    return cutils::dict_int_hash_func(llLogID);
}


bool CheckRecordNew(
        const HeadWrapper& oNewHead, const NewBasic_t& tNewBasicInfo,
		const HeadWrapper& oOldHead, const NewBasic_t& tOldBasicInfo)
{
	assert(oNewHead.Ptr() != oOldHead.Ptr());
	assert(false == dbcomm::TestFlag(*oNewHead.pFlag, FLAG_DELETE));
	if (dbcomm::TestFlag(*oOldHead.pFlag, FLAG_DELETE))
	{
		return true;
	}

	assert(*oNewHead.pLogID == *oOldHead.pLogID);
	assert(false == dbcomm::TestFlag(
                tNewBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
	assert(false == dbcomm::TestFlag(
                tOldBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
	if (tNewBasicInfo.llMaxIndex != tOldBasicInfo.llMaxIndex)
	{
		return tNewBasicInfo.llMaxIndex > tOldBasicInfo.llMaxIndex;
	}

	assert(tNewBasicInfo.llMaxIndex == tOldBasicInfo.llMaxIndex);
	if (tNewBasicInfo.llMaxIndex == tNewBasicInfo.llChosenIndex)
	{
		assert(tOldBasicInfo.llMaxIndex == tOldBasicInfo.llMaxIndex);
		assert(*oNewHead.pDataLen == *oOldHead.pDataLen);
		assert(false == dbcomm::TestFlag(tNewBasicInfo.cState, PENDING));
		assert(false == dbcomm::TestFlag(tOldBasicInfo.cState, PENDING));
		return false;
	}

    // TODO

	assert(tNewBasicInfo.llMaxIndex > tNewBasicInfo.llChosenIndex);
	assert(dbcomm::TestFlag(tNewBasicInfo.cState, PENDING));
	assert(dbcomm::TestFlag(tOldBasicInfo.cState, PENDING));


	// have to check the paxos::PaxosLog;
	paxos::PaxosLog oNewPLog;
	paxos::PaxosLog oOldPLog;
	assert(NewHeadToPlog(oNewHead, oNewPLog) == 0);
	assert(NewHeadToPlog(oOldHead, oOldPLog) == 0);

	AssertCheck(tNewBasicInfo, oNewPLog);
	AssertCheck(tOldBasicInfo, oOldPLog);

    auto new_max_ins = paxos::get_max_ins(oNewPLog);
    auto old_max_ins = paxos::get_max_ins(oOldPLog);
    assert(nullptr != new_max_ins);
    assert(nullptr != old_max_ins);

    const auto& oNewPIns = *new_max_ins;
    const auto& oOldPIns = *old_max_ins;
	assert(oNewPIns.index() == oOldPIns.index());
	logdebug("BuildTestInfo: key %lu new [%lu %lu %lu]"
			" old [%lu %lu %lu]", 
			*oNewHead.pLogID, 
			oNewPIns.proposed_num(), 
            oNewPIns.promised_num(), oNewPIns.accepted_num(),  
			oOldPIns.proposed_num(), 
            oOldPIns.promised_num(), oNewPIns.accepted_num());
	// 1. proposed num
	if (oNewPIns.proposed_num() != oOldPIns.proposed_num())
	{
		return oNewPIns.proposed_num() > oOldPIns.proposed_num();
	}

	assert(oNewPIns.proposed_num() == oOldPIns.proposed_num());
	// 2. promised_num	
	if (oNewPIns.promised_num() != oOldPIns.promised_num())
	{
		return oNewPIns.promised_num() > oOldPIns.promised_num();
	}

	assert(oNewPIns.promised_num() == oOldPIns.promised_num());
	// 3. accepted_num
	if (oNewPIns.accepted_num() != oOldPIns.accepted_num())
	{
		return oNewPIns.accepted_num() > oOldPIns.accepted_num();
	}

	assert(oNewPIns.accepted_num() == oOldPIns.accepted_num());
    // oNewPIns == oOlsPIns;
	return false;
}

bool CheckRecordNewWhenBuildMemIdx(
		clsMemIdx& oMemIdx, 
		clsDataBlockMgr& oDataBlockMgr, 
		const HeadWrapper& oHead, 
		const uint32_t iNewBlockID, 
		const uint32_t iNewOffset, 
		int& iCase)
{
	iCase = 0;
	assert(sizeof(uint64_t) == sizeof(*oHead.pLogID));
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, oHead.pLogID, sizeof(uint64_t));
	tMemKey.cKeyLen = sizeof(uint64_t);

	bool bIsNewRecord = true;
	int ret = oMemIdx.Get(tMemKey);
	switch (ret)
	{
	case 1:
		// exist
		{
			clsDataBlock* poDataBlock = oDataBlockMgr.GetByIdx(tMemKey.iBlockID);
			if (iNewBlockID == tMemKey.iBlockID && iNewOffset == tMemKey.iBlockOffset)
			{
				bIsNewRecord = false;
				iCase = 1;
				assert(NULL != poDataBlock);
				assert(poDataBlock->GetHead(tMemKey.iBlockOffset, *oHead.pLogID).IsNull() == false);
				break;
			}

			if (NULL == poDataBlock)
			{
				assert(bIsNewRecord);
				break;
			}

			assert(NULL != poDataBlock);
			assert(iNewBlockID != tMemKey.iBlockID || iNewOffset != tMemKey.iBlockOffset);
			HeadWrapper oOldHead = 
				poDataBlock->GetHead(tMemKey.iBlockOffset, *oHead.pLogID);
			assert(oOldHead.Ptr() != oHead.Ptr());
			if (oOldHead.IsNull() == false)
			{
				NewBasic_t tBasicInfo, tOldBasicInfo;
				assert(0 == oHead.GetBasicInfo(tBasicInfo));
				assert(0 == oOldHead.GetBasicInfo(tOldBasicInfo));

				bIsNewRecord = CheckRecordNew(oHead, tBasicInfo, oOldHead, tOldBasicInfo);
				
				logdebug("BuildTestInfo: key %lu bIsNewRecord %d"
						" MemIdx [%lu %lu %u %u]"
						" NewRecord [%lu %lu %u %u]", 
						*oHead.pLogID, static_cast<int>(bIsNewRecord), 
						tOldBasicInfo.llMaxIndex, 
						tOldBasicInfo.llReqID, 
						tOldBasicInfo.iVersion, 
						*oOldHead.pDataLen, 
						tBasicInfo.llMaxIndex, 
						tBasicInfo.llReqID, 
						tBasicInfo.iVersion, 
						*oHead.pDataLen);

				if (false == bIsNewRecord)
				{
					iCase = 2;
				}
			}
		}
	case 0:
		// don't exist
		break;
	default:
		logerr("key %lu clsMemIdx::Get ret %d", *oHead.pLogID, ret);
		assert(false);
		break;
	}

	return bIsNewRecord;
}


bool CheckRecordNewWhenMemMerge(
		clsMemIdx& oMemIdx, uint64_t llLogID, uint32_t iBlockID, uint32_t iBlockOffset)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	int ret = oMemIdx.Get(tMemKey);
	if (1 == ret)
	{
		if (iBlockID == tMemKey.iBlockID && iBlockOffset == tMemKey.iBlockOffset)
		{
			return true;
		}
	}

	logerr("MemMergeInfo: key %lu clsMemIdx::Get ret %d MemIdx"
			" Pos[%u %u] vs Pos[%u %u]", llLogID, ret, 
			tMemKey.iBlockID, tMemKey.iBlockOffset, 
            iBlockID, iBlockOffset);
	return false;
}

void BuildMemIdxOnOneBlock(
		clsNewMemKv& oMemKv, 
		clsDataBlock& oDataBlock, 
		int& iIdxBlockID, 
		uint32_t& iKeyCnt, 
		uint32_t& iRealKeyCnt)
{
	iKeyCnt = 0;
	iRealKeyCnt = 0;

	uint32_t iTotalSkipLen = 0;
	uint32_t iOffset = 0;
	while (true)
	{
		HeadWrapper oHead = HeadWrapper::Null;
		uint32_t iSkipLen = 0;
		int iReadLen = oDataBlock.GetRecordSkipErr(iOffset, oHead, iSkipLen);
		hassert(0 <= iReadLen, "iOffset %u iSkipLen %u", iOffset, iSkipLen);
		iTotalSkipLen += iSkipLen;
		if (0 == iReadLen)
		{
			break; // reach end
		}

		assert(0 < iReadLen);
		assert(oHead.IsNull() == false);
		++iKeyCnt;
		int ret = oMemKv.BuildOneRecord(
				oHead, oDataBlock.GetBlockID(), iOffset + iSkipLen, iIdxBlockID);
		if (0 == ret)
		{
			++iRealKeyCnt;
		}
		else
		{
			logerr("BuildOneRecord iBlockID %d iOffset %u iSkipLen %d ret %d", 
					oDataBlock.GetBlockID(), iOffset, iSkipLen, ret);
		}

		iOffset += iReadLen;
	}

	logdebug("BuildInfo: iBlockID %d iIdxBlockID %d "
			"iTotalSkipLen %u iKeyCnt %u iRealKeyCnt %u", 
			oDataBlock.GetBlockID(), iIdxBlockID, 
			iTotalSkipLen, iKeyCnt, iRealKeyCnt);
	return ;
}

void BuildMemIdx(size_t iWorkID, clsNewMemKv& oMemKv)
{
	// Comm::LogThreadName("Build", pthread_self());

	clsDataBlockMgr* poDataBlockMgr = oMemKv.GetDataBlockMgr();
	assert(NULL != poDataBlockMgr);

	uint32_t iBuildBlockCnt = 0;
	uint32_t iTotalKeyCnt = 0;
	uint32_t iTotalRealKeyCnt = 0;

	int iIdxBlockID = -1;
	logerr("BuildInfo: iWorkID %zu start", iWorkID);
	while (true)
	{
		int iCurBuildBlockID = oMemKv.NextBuildBlockID();
		if (-1 == iCurBuildBlockID)
		{
			break;
		}

		printf ( "iCurBuildBlockID %d\n", iCurBuildBlockID );
		clsDataBlock* poDataBlock = 
			poDataBlockMgr->GetByIdx(static_cast<uint32_t>(iCurBuildBlockID));
		if (NULL == poDataBlock)
		{
			logerr("BuildInfo: poDataBlock == NULL BlockID %d", 
					iCurBuildBlockID);
			assert(false);
		}

		uint32_t iKeyCnt = 0;
		uint32_t iRealKeyCnt = 0;
		BuildMemIdxOnOneBlock(oMemKv, *poDataBlock, iIdxBlockID, iKeyCnt, iRealKeyCnt);
		++iBuildBlockCnt;
		iTotalRealKeyCnt += iRealKeyCnt;
		iTotalKeyCnt += iKeyCnt;
		logerr("BuildInfo: iWorkID %zu progress iCurBuildBlockID %d"
				" iBuildBlockCnt %u iTotalKeyCnt %u iTotalRealKeyCnt %u", 
				iWorkID, iCurBuildBlockID, iBuildBlockCnt, 
				iTotalKeyCnt, iTotalRealKeyCnt);
	}

	logerr("BuildInfo: iWorkID %zu end iBuildBlockCnt %u"
			" iTotalKeyCnt %u iTotalRealKeyCnt %u", 
			iWorkID, iBuildBlockCnt, iTotalKeyCnt, iTotalRealKeyCnt);
	return ;
}

int DoMemMergeOn(
		clsNewMemKv& oMemKv, 
		bool& bStop, 
		clsDataBlock* pMergeFromBlock, uint32_t& iOffset, 
		clsDataBlock* pMergeToBlock, 
		uint32_t& iTotalCnt, uint32_t& iRealCnt)
{
	assert(NULL != pMergeFromBlock);
	assert(NULL != pMergeToBlock);

	while (false == bStop)
	{
		HeadWrapper oHead = HeadWrapper::Null;
		uint32_t iSkipLen = 0;
		int iReadLen = 
			pMergeFromBlock->GetRecordSkipErr(iOffset, oHead, iSkipLen);
		assert(0 <= iReadLen);
		if (0 == iReadLen)
		{
			// reach end
			return 0;
		}

		if (0 < iSkipLen)
		{
			logerr("MERGE ERROR: GetRecordSkipErr iOffset %u iSkipLen %u", 
					iOffset, iSkipLen);
		}

		assert(0 < iReadLen && oHead.IsNull() == false);
		int ret = oMemKv.MergeOneRecord(
				oHead, pMergeFromBlock->GetBlockID(), 
				iOffset + iSkipLen, 
				*pMergeFromBlock, *pMergeToBlock);
		if (clsDataBlock::No_Space == ret)
		{
			return 1;
		}

		assert(0 <= ret);
		++iTotalCnt;
		if (0 == ret)
		{
			++iRealCnt;
			*oHead.pFlag = dbcomm::AddFlag(*oHead.pFlag, FLAG_DELETE);
			pMergeFromBlock->ReportDelOneKey();
		}

		iOffset += iReadLen;
	}

	return -1;
}

void StartMerge(clsNewMemKv& oMemKv, bool& bStop)
{
	// Comm::LogThreadName("MemMerge", pthread_self());
	{
        int cpu_cnt = GetCpuCount();
        assert(3 <= cpu_cnt);
        BindToCpu(cpu_cnt - 2, cpu_cnt - 1);
	}

	clsDataBlockMgr* pDataBlockMgr = oMemKv.GetDataBlockMgr();
	assert(NULL != pDataBlockMgr);
	logerr("MergeThread start");

	uint32_t iOffset = 0;
	clsDataBlock* pMergeFromBlock = NULL;
	clsDataBlock* pMergeToBlock = NULL;
	uint32_t iStartMergeBlockID = 0;
	
	struct timeval tStartTime;
	gettimeofday(&tStartTime, NULL);

	uint32_t iTotalCnt = 0;
	uint32_t iRealCnt = 0;
	while (false == bStop)
	{
		// 1. 
		while (NULL == pMergeFromBlock && false == bStop)
		{
			pMergeFromBlock = pDataBlockMgr->GetMergeFrom(iStartMergeBlockID);
			if (NULL == pMergeFromBlock)
			{
				logerr("MemMergeInfo: No MergeFrom Block");
				sleep(1);
				continue;
			}

			assert(NULL != pMergeFromBlock);
			iStartMergeBlockID = pMergeFromBlock->GetBlockID();
			iOffset = 0;
			iTotalCnt = 0;
			iRealCnt = 0;
			logerr("MemMergeInfo: start merge"
					" BlockID %u usesize %u useratio %u", iStartMergeBlockID, 
					pMergeFromBlock->GetUseSize(), pMergeFromBlock->GetUseRatio());
			while (0 == access("/home/qspace/data/kvsvr/stop_mem_merge", F_OK))
			{
				logerr("MemMergeInfo: merge stop by stop_mem_merge");
				sleep(2);
			}

			gettimeofday(&tStartTime, NULL);
		}

		// 2. 
		while (NULL == pMergeToBlock && false == bStop)
		{
			pMergeToBlock = pDataBlockMgr->GetMergeTo();
			if (NULL == pMergeToBlock)
			{
				logerr("MergeInfo: No MergeTo Block");
				sleep(1);
				continue;
			}

			assert(NULL != pMergeToBlock);
			logerr("MemMergeInfo: new MergeToBlock Block %u"
					" usesize %u useratio %u", 
					pMergeToBlock->GetBlockID(), 
					pMergeToBlock->GetUseSize(), pMergeToBlock->GetUseRatio());
		}

		// 3.
		if (bStop)
		{
			break;
		}

		assert(NULL != pMergeFromBlock);
		assert(NULL != pMergeToBlock);

		int ret = DoMemMergeOn(oMemKv, bStop, 
				pMergeFromBlock, iOffset, pMergeToBlock, iTotalCnt, iRealCnt);
		if (0 == ret)
		{
			// reach end
			pDataBlockMgr->Free(pMergeFromBlock);
			pMergeFromBlock = NULL;
		
			struct timeval tEndTime;
			gettimeofday(&tEndTime, NULL);

			int iDiff = (tEndTime.tv_sec - tStartTime.tv_sec) * 1000 + 
				(tEndTime.tv_usec - tStartTime.tv_usec) / 1000;

			logerr("MemMergeInfo: end merge BlockID %u used %d cnt %u %u"
					" ratio %0.1f", iStartMergeBlockID, iDiff, 
					iRealCnt, iTotalCnt, iRealCnt*1.0 / iTotalCnt * 100);
			continue;
		}

		assert(1 == ret || -1 == ret);
		if (-1 == ret)
		{
			assert(true == bStop);
			break; // STOP
		}

		assert(1 == ret);
		pMergeToBlock = NULL; // merge to have no space
	}

	logerr("MergeThread end");
	return ;
}

} // namespace


namespace memkv {

clsNewMemKv::clsNewMemKv()
	: m_pMemIdx(NULL)
	, m_pDataBlockMgr(NULL)
	, m_pHashBaseLock(NULL)
	, m_iNextBuildBlockID(0)
	, m_iIdxHeadSize(0)
	, m_bStop(false)
{
	assert(false == m_tMergeFut.valid());
}

clsNewMemKv::~clsNewMemKv()
{
	m_bStop = true;
	if (m_tMergeFut.valid())
	{
		// wait for merge thread exit
		m_tMergeFut.get();
	}

	delete m_pHashBaseLock;
	m_pHashBaseLock = NULL;
	delete m_pDataBlockMgr;
	m_pDataBlockMgr = NULL;
	delete m_pMemIdx;
	m_pMemIdx = NULL;
}

int clsNewMemKv::Init(
		uint32_t iIdxHeadSize, 
		const char* sPLogPath, 
		uint32_t iMaxAppendBlockNum,
		int idxShmKey,
		int dataBlockShmKey)
{
	assert(NULL == m_pHashBaseLock);
	assert(NULL == m_pDataBlockMgr);
	assert(NULL == m_pMemIdx);
	assert(false == m_tMergeFut.valid());

	assert(0 < iIdxHeadSize);
	m_pMemIdx = new clsMemIdx;
	// m_pMemIdx = cutils::make_unique<clsMemIdx>();
	m_iIdxHeadSize = iIdxHeadSize;
	assert(0 < m_iIdxHeadSize);
	uint32_t iLockSize = m_iIdxHeadSize / 100;
	assert(0 < iLockSize);
	assert(iLockSize < m_iIdxHeadSize);
	assert(0 == m_iIdxHeadSize % iLockSize);

	m_sPLogPath = sPLogPath;
    std::string sMemIdxLocKPath = std::string(m_sPLogPath) + "/clsMemIdx::AllockLock";
	int ret = m_pMemIdx->Init(
			idxShmKey, iIdxHeadSize, MAX_BLOCK, 
            sMemIdxLocKPath.c_str());
			// "//clsNewMemKv/clsMemIdx::AllocLock");
	if (0 != ret)
	{
		logerr("m_pMemIdx->Init idxShmKey %d ret %d", 
				idxShmKey, ret);
		return -1;
	}

	m_pDataBlockMgr = new clsDataBlockMgr(dataBlockShmKey);
	// m_pDataBlockMgr = cutils::make_unique<clsDataBlockMgr>(NEW_DATA_BLOCK_SHM_KEY);
    std::string sDataBlockMgrLockPath = std::string(m_sPLogPath) + "datablockmgr_new.lock";
	ret = m_pDataBlockMgr->Init(
            sDataBlockMgrLockPath.c_str(), 
			// "/home/qspace/data/kvsvr/memkv/datablockmgr_new.lock", 
			iMaxAppendBlockNum);
	if (0 != ret)
	{
		logerr("m_pDataBlockMgr->Init dataBlockShmKey %d ret %d", 
				dataBlockShmKey, ret);
		return -2;
	}

	// const std::string sLockPath = "/home/qspace/data/kvsvr/memkv/memkv_new.lock";
    const std::string sLockPath = m_sPLogPath + "/memkv_new.lock";
	m_pHashBaseLock = new dbcomm::HashBaseLock;
	// m_pHashBaseLock = cutils::make_unique<HashBaseLock>();
	ret = m_pHashBaseLock->Init(sLockPath.c_str(), iLockSize);
	if (0 != ret)
	{
		logerr("m_pHashBaseLock->Init %s iLockSize %u ret %d", 
				sLockPath.c_str(), iLockSize, ret);
		return -3;
	}

	clsMemSizeMng::GetDefault()->AddUseSize(iLockSize * sizeof(pthread_rwlock_t));

//	// 1. rebuild clsMemIdx
//
//	// 2. start mem merge;
//	assert(false == m_tMergeFut.valid());

	return 0;
}

int clsNewMemKv::StartBuildIdx()
{
	assert(NULL != m_pMemIdx);
	assert(NULL != m_pDataBlockMgr);
	std::vector<std::future<void>> vecFut;

	uint32_t iStartTime = time(NULL);
	const uint32_t iBlockNum = m_pDataBlockMgr->GetBlockNum();

	for (size_t idx = 0; idx < 8; ++idx)
	{
		vecFut.emplace_back(
				std::async(std::launch::async, 
                    BuildMemIdx, idx, std::ref(*this)));
	}

	assert(iBlockNum == m_pDataBlockMgr->GetBlockNum());
	for (size_t idx = 0; idx < vecFut.size(); ++idx)
	{
		assert(vecFut[idx].valid());
		vecFut[idx].get();
	}
	vecFut.clear();

	if (m_iNextBuildBlockID != m_pDataBlockMgr->GetBlockNum())
	{
		logerr("BuildInfo: NextBuildBlockID %u BlockNum %u", 
				m_iNextBuildBlockID, m_pDataBlockMgr->GetBlockNum());
		return -2;
	}

	uint32_t iEndTime = time(NULL);
	printf ( "Build use time %u\n", iEndTime - iStartTime );
	logerr("BuildInfo: Build use time %u", iEndTime - iStartTime);
	logerr("BuildInfo: NextBuildBlockID %u BlockNum %u", 
			m_iNextBuildBlockID, m_pDataBlockMgr->GetBlockNum());
	return 0;
}

int clsNewMemKv::StartMemMergeThread()
{
	assert(NULL != m_pMemIdx);
	assert(NULL != m_pDataBlockMgr);
	assert(NULL != m_pHashBaseLock);
	assert(false == m_tMergeFut.valid());
	m_tMergeFut = std::async(
			std::launch::async, StartMerge, std::ref(*this), std::ref(m_bStop));
	assert(m_tMergeFut.valid());
	return 0;
}

int clsNewMemKv::ReloadDiskFile(
		bool bForceFullReload, 
		std::function<int(clsNewMemKv&)> pfnLoadHandler, 
		std::function<int(clsNewMemKv&)> pfnReloadHandler)
{
	assert(NULL != m_pDataBlockMgr);
	if (bForceFullReload || 
			false == m_pDataBlockMgr->GetLoadFlag() || 
			0 >= m_pDataBlockMgr->GetBlockNum())
	{
		// full reload
		
		assert(0 == pfnLoadHandler(*this));
		if (0 == m_pDataBlockMgr->GetBlockNum())
		{
			m_pMemIdx->AllocBlock();
		}

		m_pDataBlockMgr->SetLoadFlag();
		return 0;
	}
	// else
	// => 
	assert(false == bForceFullReload);
	if (0 != access("/home/qspace/data/kvsvr/stop_reload_file", F_OK))
	{
		// partial reload: only reload most recent .w file
		assert(0 == pfnReloadHandler(*this));
	}

	return 0;
}

int clsNewMemKv::GetOnExistKeyNoLock(
		const MemKey_t& tMemKey, 
		NewBasic_t& tBasicInfo, 
		paxos::PaxosLog& oPLog)
{
	assert(NULL != m_pDataBlockMgr);

	clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
	if (NULL == pDataBlock)
	{
		return -3;
	}

	uint64_t llLogID = 0;
	memcpy(&llLogID, tMemKey.sKey, sizeof(llLogID));
	assert(NULL != pDataBlock);
	HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull())
	{
		return -5;
	}

	assert(oHead.IsNull() == false);
	assert(llLogID == *oHead.pLogID);
	//assert(false == dbcomm::TestFlag(pHead->tBasicInfo.cFlag, RECORD_COMPRESSE));
	/*
	if (false == oPLog.ParseFromArray(pHead->pData, pHead->iDataLen))
	{
		return BROKEN_PAXOS_LOG;
	}
	*/
	if (NewHeadToPlog(oHead, oPLog) != 0)
	{
		return BROKEN_PAXOS_LOG;
	}

	// success Parse
	assert(0 == oHead.GetBasicInfo(tBasicInfo));

	CheckConsistency(tBasicInfo, oPLog);
	return 0;
}

int clsNewMemKv::Has(uint64_t llLogID)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock lock(
            m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	lock.ReadLock();

	return m_pMemIdx->Get(tMemKey);
}

int clsNewMemKv::IsPendingOn(uint64_t llLogID, uint64_t llIndex)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock lock(
            m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	lock.ReadLock();

	int ret = m_pMemIdx->Get(tMemKey);
	if (0 >= ret)
	{
		return ret;
	}

	assert(1 == ret);
	// key exist;
	clsDataBlock* poDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
	if (NULL == poDataBlock)
	{
		return -3;
	}

	assert(NULL != poDataBlock);
	HeadWrapper oHead = poDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull())
	{
		return -5;
	}

	assert(llLogID == *oHead.pLogID);
	NewBasic_t tBasicInfo = {0};
	assert(0 == oHead.GetBasicInfo(tBasicInfo));
	if (tBasicInfo.llMaxIndex == llIndex && 
			dbcomm::TestFlag(tBasicInfo.cState, PENDING))
	{
		return 1;
	}

	return 0;
}


int clsNewMemKv::Get(
		uint64_t llLogID, 
		NewBasic_t& tBasicInfo, 
		paxos::PaxosLog& oPLog)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock hashlock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	hashlock.ReadLock();

	int ret = m_pMemIdx->Get(tMemKey);
	switch (ret)
	{
	case 1:
		ret = GetOnExistKeyNoLock(tMemKey, tBasicInfo, oPLog);
		if (0 != ret)
		{
			logerr("GetOnExistKeyNoLock key %lu Pos[%u %u] ret %d", 
					llLogID, tMemKey.iBlockID, tMemKey.iBlockOffset, ret);
		}
		break;
	case 0:
		{
			memset(&tBasicInfo, 0, sizeof(tBasicInfo));
            // oPLog = paxos::zeros_plog();
            oPLog = paxos::PaxosLog();
		}
		break;
	default:
		logerr("m_pMemIdx->Get key %lu ret %d", llLogID, ret);
		ret = -2;
		break;
	}

	return ret;
}

int clsNewMemKv::Get(
		uint64_t llLogID, 
		NewBasic_t& tBasicInfo)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock lock(
            m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	lock.ReadLock();

	int ret = m_pMemIdx->Get(tMemKey);
	switch (ret)
	{
	case 1:
		ret = GetBasicInfoNoLock(tMemKey, tBasicInfo);
		if (0 != ret)
		{
			logerr("GetBasicInfoNoLock key %lu Pos[%u %u] ret %d", 
					llLogID, tMemKey.iBlockID, tMemKey.iBlockOffset, ret);
		}
		break;
	case 0:
		memset(&tBasicInfo, 0, sizeof(tBasicInfo));
		break;
	default:
		logerr("m_pMemIdx->Get key %lu ret %d", llLogID, ret);
		ret = -2;
		break;
	}
	return ret;
}

int clsNewMemKv::AppendSetNoLock(
		uint64_t llLogID, 
		const NewBasic_t& tBasicInfo, 
		const paxos::PaxosLog& oPLog)
{
	assert(false == dbcomm::TestFlag(
                tBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
	assert(NULL != m_pDataBlockMgr);
	assert(NULL != m_pMemIdx);

	uint32_t iNewOffset = 0;
	clsDataBlock* pAppendBlock = NULL;
	int iLoopCnt = 0;
	int ret = 0;
	while (true)
	{
		++iLoopCnt;
		// pAppendBlock = m_pDataBlockMgr->GetAppendBlock();
		pAppendBlock = m_pDataBlockMgr->GetAppendBlockNew();
		if (NULL == pAppendBlock)
		{
			return -6;
		}

		assert(NULL != pAppendBlock);
		ret = pAppendBlock->AppendSet(llLogID, tBasicInfo, oPLog, iNewOffset);
		if (clsDataBlock::No_Space == ret)
		{
			continue;
		}
		else if (0 != ret)
		{
			return -8;
		}

		assert(0 == ret);
		break;
	}

	if (3 <= iLoopCnt)
	{
		logerr("INFO: key %lu iLoopCnt %d (GetAppendBlock)", 
				llLogID, iLoopCnt);
	}

	assert(NULL != pAppendBlock);
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);
	tMemKey.iBlockID = pAppendBlock->GetBlockID();
	tMemKey.iBlockOffset = iNewOffset;
	ret = m_pMemIdx->Set(tMemKey);
	if (0 > ret)
	{
		logerr("m_pMemIdx->Set key %lu Pos[%u %u] ret %d", 
				llLogID, tMemKey.iBlockID, tMemKey.iBlockOffset, ret);
		return -9;
	}

	return 0;
}

int clsNewMemKv::SetNoLock(
		const MemKey_t& tMemKey, 
		const NewBasic_t& tBasicInfo, 
		const paxos::PaxosLog& oPLog)
{
	assert(false == dbcomm::TestFlag(
                tBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
	assert(NULL != m_pDataBlockMgr);
	assert(NULL != m_pMemIdx);
	clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
	if (NULL == pDataBlock)
	{
		return -3;
	}

	uint64_t llLogID = 0;
	memcpy(&llLogID, tMemKey.sKey, sizeof(llLogID));
	assert(NULL != pDataBlock);

	HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull())
	{
		return -5;
	}

	assert(llLogID == *oHead.pLogID);
	assert(0 < oPLog.ByteSize());
	const uint32_t iNewDataLen = oPLog.ByteSize();
	if (iNewDataLen == *oHead.pDataLen)
	{
		if (false == oPLog.SerializeToArray(oHead.pData, *oHead.pDataLen))
		{
			return SERIALIZE_PAXOS_LOG_ERR;
		}

		// success
		(*oHead.pFlag) = dbcomm::ClearFlag(*oHead.pFlag, FLAG_COMPRESSE);
		oHead.SetBasicInfo(tBasicInfo);
		return 0; // success
	}

	assert(iNewDataLen != *oHead.pDataLen);
	logdebug("key %lu pHead->iDataLen %d iNewDataLen %d", 
			llLogID, *oHead.pDataLen, iNewDataLen);

	int ret = AppendSetNoLock(llLogID, tBasicInfo, oPLog);
	if (0 != ret)
	{
		return ret;
	}

	assert(0 == ret);
	(*oHead.pFlag) = dbcomm::AddFlag(*oHead.pFlag, FLAG_DELETE);
	pDataBlock->ReportDelOneKey();

	return 0;
}

int clsNewMemKv::Set(
		uint64_t llLogID, 
		const NewBasic_t& tBasicInfo, 
		const paxos::PaxosLog& oPLog)
{
	assert(false == dbcomm::TestFlag(
                tBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
	CheckConsistency(tBasicInfo, oPLog);
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);

    dbcomm::HashLock hashlock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	hashlock.WriteLock();

	int ret = m_pMemIdx->Get(tMemKey);
	switch (ret)
	{
	case 1:
		ret = SetNoLock(tMemKey, tBasicInfo, oPLog);
		break;
	case 0:
		ret = AppendSetNoLock(llLogID, tBasicInfo, oPLog);
		break;
	default:
		logerr("m_pMemIdx->Get key %lu ret %d", llLogID, ret);
		ret = -2;
		break;
	}

	return ret;
}

int clsNewMemKv::BatchSet(
		const std::vector<uint64_t>& vecLogID, 
		const std::vector<NewBasic_t*>& vecBasicInfo, 
		const std::vector<paxos::PaxosLog*>& vecPLog)
{
	assert(false == vecLogID.empty());
	assert(vecLogID.size() == vecBasicInfo.size());
	assert(vecLogID.size() == vecPLog.size());

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);

	std::set<uint32_t> setHash;
	for (size_t idx = 0; idx < vecLogID.size(); ++idx)
	{
		assert(false == dbcomm::TestFlag(
					vecBasicInfo[idx]->cFlag, dbcomm::RECORD_COMPRESSE));
		assert(NULL != vecBasicInfo[idx]);
		assert(NULL != vecPLog[idx]);
		CheckConsistency(*(vecBasicInfo[idx]), *(vecPLog[idx]));

		setHash.insert(HashFunc(
					reinterpret_cast<
						const char*>(&vecLogID[idx]), sizeof(vecLogID[idx])));
	}

	int iLockTime = 0;
	int iDataSetTime = 0;
	int iUpdateMemIndexTime = 0;

	uint64_t llBeginTime = dbcomm::GetTickMS();
	
    dbcomm::HashLock hashlock(m_pHashBaseLock, setHash);
	int ret = hashlock.BatchWriteLock();
	if (0 != ret)
	{
		logerr("HashLock::BatchWriteLock setHash.size %zu"
				" vecLogID.size %zu ret %d", 
				setHash.size(), vecLogID.size(), ret);
		return -1;
	}

	iLockTime = dbcomm::GetTickMS() - llBeginTime;

	std::vector<uint32_t> vecNewOffset(vecLogID.size(), 0);
	clsDataBlock* pAppendBlock = NULL;
	int iLoopCnt = 0;

	llBeginTime = dbcomm::GetTickMS();
	while (true)
	{
		++iLoopCnt;
		pAppendBlock = m_pDataBlockMgr->GetAppendBlockNew();
		if (NULL == pAppendBlock)
		{
			return -6;
		}

		assert(NULL != pAppendBlock);
		ret = pAppendBlock->BatchAppendSet(
				vecLogID, vecBasicInfo, vecPLog, vecNewOffset);
		if (clsDataBlock::No_Space == ret)
		{
			continue;
		}
		else if (0 != ret)
		{
			return -8;
		}

		assert(0 == ret);
		break;
	}

	if (3 <= iLoopCnt)
	{
		logerr("INFO: vecLogID.size %zu iLoopCnt %d (GetAppendBlock)", 
				vecLogID.size(), iLoopCnt);
	}

	iDataSetTime = dbcomm::GetTickMS() - llBeginTime;
	llBeginTime = dbcomm::GetTickMS();

	MemKey_t tMemKey = {0};
	
	tMemKey.iBlockID = pAppendBlock->GetBlockID();
	tMemKey.cKeyLen = sizeof(uint64_t);
	for (size_t idx = 0; idx < vecLogID.size(); ++idx)
	{
		memcpy(tMemKey.sKey, &vecLogID[idx], sizeof(vecLogID[idx]));

		MemKey_t tPrevMemKey = tMemKey;
		tPrevMemKey.iBlockID = 0;
		tPrevMemKey.iBlockOffset = 0;
		int iRet = m_pMemIdx->Get(tPrevMemKey);

		tMemKey.iBlockOffset = vecNewOffset[idx];
		assert(0 <= tMemKey.iBlockOffset);
		ret = m_pMemIdx->Set(tMemKey);
		if (0 > ret)
		{
			logerr("m_pMemIdx->Set key %lu Pos[%u %u] ret %d", 
					vecLogID[idx], tMemKey.iBlockID, tMemKey.iBlockOffset, ret);
			return -9;
		}

		if (1 != iRet)
		{
			if (0 != iRet)
			{
				logerr("m_pMemIdx->Get key %lu iRet %d", vecLogID[idx], iRet);
			}
			continue;
		}

		assert(1 == iRet);
		// prev exist key;
		clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tPrevMemKey.iBlockID);
		if (NULL == pDataBlock)
		{
			logerr("m_pDataBlockMgr->GetByIdx iBlockID %u NULL", 
					tPrevMemKey.iBlockID);
			continue;
		}

		assert(NULL != pDataBlock);
		HeadWrapper oHead = pDataBlock->GetHead(
				tPrevMemKey.iBlockOffset, vecLogID[idx]);
		if (oHead.IsNull())
		{
			logerr("pDataBlock->GetHead key %lu Pos [%u %u] NULL", 
					vecLogID[idx], tPrevMemKey.iBlockID, tPrevMemKey.iBlockOffset);
			continue;
		}

		assert(vecLogID[idx] == *oHead.pLogID);
		(*oHead.pFlag) = dbcomm::AddFlag(*oHead.pFlag, FLAG_DELETE);
		pDataBlock->ReportDelOneKey();
	}
	iUpdateMemIndexTime = dbcomm::GetTickMS() - llBeginTime;
	int iCostTime = iLockTime + iDataSetTime + iUpdateMemIndexTime;
	if (5 <= iCostTime) {
		logerr("PERFORMANCE %d [%d %d %d] %d", 
				iCostTime, iLockTime, iDataSetTime, iUpdateMemIndexTime, iLoopCnt);
	}

	return 0;
}

int clsNewMemKv::Update(
		uint64_t llLogID, const NewBasic_t& tBasicInfo)
{
	assert(false == dbcomm::TestFlag(
                tBasicInfo.cFlag, dbcomm::RECORD_COMPRESSE));
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock lock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	lock.WriteLock();

	int ret = m_pMemIdx->Get(tMemKey);
	if (1 != ret)
	{
		logerr("m_pMemIdx->Get key %lu ret %d expected 1", llLogID, ret);
		return -2;
	}

	assert(1 == ret);
	clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
	if (NULL == pDataBlock)
	{
		return -3;
	}

	assert(NULL != pDataBlock);
	HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull())
	{
		return -5;
	}

	assert(llLogID == *oHead.pLogID);
	oHead.SetBasicInfo(tBasicInfo);
	return 0;
}

int clsNewMemKv::GetBasicInfoNoLock(
		const MemKey_t& tMemKey, NewBasic_t& tBasicInfo)
{
	assert(NULL != m_pDataBlockMgr);

	clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
	if (NULL == pDataBlock)
	{
		return -3;
	}

	uint64_t llLogID = 0;
	memcpy(&llLogID, tMemKey.sKey, sizeof(llLogID));
	assert(NULL != pDataBlock);
	HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull())
	{
		return -5;
	}

	assert(llLogID == *oHead.pLogID);
	assert(0 == oHead.GetBasicInfo(tBasicInfo));
	return 0;
}


int clsNewMemKv::CheckReqID(
		uint64_t llLogID, uint32_t iExpectedVersion, uint64_t llReqID)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock lock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	lock.ReadLock();

	NewBasic_t tBasicInfo = {0};
	int ret = m_pMemIdx->Get(tMemKey);
	switch (ret)
	{
	case 1:
		ret = GetBasicInfoNoLock(tMemKey, tBasicInfo);
		if (0 != ret)
		{
			logerr("GetBasicInfoNoLock key %lu Pos[%u %u] ret %d", 
					llLogID, tMemKey.iBlockID, tMemKey.iBlockOffset, ret);
		}
		break;
	case 0:
		break;
	default:
		logerr("m_pMemIdx->Get key %lu ret %d", llLogID, ret);
		ret = -2;
		break;
	}

	if (0 != ret)
	{
		return ret;
	}

	assert(0 == ret);
	if (llReqID != tBasicInfo.llReqID || 
			tBasicInfo.iVersion != iExpectedVersion)
	{
		logerr("key %lu llReqID %lu tBasicInfo.llReqID %lu"
				" iExpectedVersion %u tBasicInfo.iVersion %u"
				" tBasicInfo.llMaxIndex %lu", 
				llLogID, llReqID, tBasicInfo.llReqID, iExpectedVersion, 
				tBasicInfo.iVersion, tBasicInfo.llMaxIndex);
		// TODO
		return PAXOS_SET_PREEMPTED;
	}

	return 0; // success check
}


int clsNewMemKv::NextBuildBlockID()
{
	uint32_t iBlockNum = m_pDataBlockMgr->GetBlockNum();
    std::lock_guard<std::mutex> lock(m_tMutex);
	if (m_iNextBuildBlockID < iBlockNum)
	{
		return m_iNextBuildBlockID++;
	}

	return -1;
}

int clsNewMemKv::BuildOneRecord(
		const HeadWrapper& oHead, 
		uint32_t iBlockID, uint32_t iOffset, int& iIdxBlockID)
{
	assert(NULL != m_pMemIdx);
	assert(NULL != m_pDataBlockMgr);
	assert(NULL != m_pHashBaseLock);
	if (dbcomm::TestFlag(*oHead.pFlag, FLAG_DELETE))
	{
		logerr("BuildInfo: TESTINFO iBlockID %u iOffset %u key %lu FLAG_DELETE", 
				iBlockID, iOffset, *oHead.pLogID);
		return 1; // delete
	}

	MemKey_t tMemKey = {0};
	assert(sizeof(uint64_t) == sizeof(*oHead.pLogID));
	memcpy(tMemKey.sKey, oHead.pLogID, sizeof(uint64_t));
	tMemKey.cKeyLen = sizeof(uint64_t);
	tMemKey.iBlockID = iBlockID;
	tMemKey.iBlockOffset = iOffset;

    dbcomm::HashLock lock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	lock.WriteLock();
	int iCase = 0;
	bool bIsNewRecord = CheckRecordNewWhenBuildMemIdx(
			*m_pMemIdx, *m_pDataBlockMgr, oHead, iBlockID, iOffset, iCase);
//	printf ( "BuildInfo: key %lu iBlockID %u iOffset %u bIsNewRecord %d iCase %d\n", 
//			tHead.llLogID, iBlockID, iOffset, static_cast<int>(bIsNewRecord), iCase);
//	logerr("BuildInfo: TESTINFO record key %lu iBlockID %u"
//			" iOffset %u bIsNewRecord %d", 
//			tHead.llLogID, iBlockID, iOffset, static_cast<int>(bIsNewRecord));
	if (false == bIsNewRecord)
	{
		return 2;
	}

	// oHead is newer then the one in m_pMemIdx
	while (true)
	{
		if (0 > iIdxBlockID)
		{
			iIdxBlockID = m_pMemIdx->AllocBlock();
			assert(0 <= iIdxBlockID);
		}

		int ret = m_pMemIdx->Set(iIdxBlockID, tMemKey);
		if (0 <= ret)
		{
			break;
		}

		assert(0 > ret);
		iIdxBlockID = -1; // AllocBlock
	}
	
	return 0;
}

int clsNewMemKv::MergeOneRecord(
		const HeadWrapper& oHead, 
		uint32_t iBlockID, 
		uint32_t iOffset, 
		clsDataBlock& oMergeFromBlock, 
		clsDataBlock& oMergeToBlock)
{
	assert(NULL != m_pMemIdx);
	assert(NULL != m_pDataBlockMgr);
	assert(NULL != m_pHashBaseLock);
	if (dbcomm::TestFlag(*oHead.pFlag, FLAG_DELETE))
	{
		return 100;
	}

    dbcomm::HashLock lock(
			m_pHashBaseLock, 
			HashFunc(
				reinterpret_cast<const char*>(oHead.pLogID), 
				sizeof(uint64_t)));
	lock.WriteLock();

	bool bIsNewRecord = CheckRecordNewWhenMemMerge(
			*m_pMemIdx, *oHead.pLogID, iBlockID, iOffset);
	if (false == bIsNewRecord)
	{
		return 101;
	}

	uint32_t iNewOffset = 0;
	int ret = oMergeToBlock.AppendSetRecord(oHead, iNewOffset);
	if (clsDataBlock::No_Space == ret)
	{
		return ret;
	}
	else if (0 != ret)
	{
		logerr("MemMergeInfo: clsDataBlock::AppendSetRecord key %lu"
				" BlockID %u ret %d", 
				*oHead.pLogID, oMergeToBlock.GetBlockID(), ret);
		assert(false);
	}
	
	// 
	assert(0 == ret);
	MemKey_t tMemKey = {0};
	assert(sizeof(uint64_t) == sizeof(*oHead.pLogID));
	memcpy(tMemKey.sKey, oHead.pLogID, sizeof(uint64_t));
	tMemKey.cKeyLen = sizeof(uint64_t);
	tMemKey.iBlockID = oMergeToBlock.GetBlockID();
	tMemKey.iBlockOffset = iNewOffset;

	ret = m_pMemIdx->Set(tMemKey);
	assert(0 <= ret);
	return 0;
}

int clsNewMemKv::Visit(clsNewMemVisitor* visitor, uint64_t llHashIdx)
{
	assert(NULL != m_pMemIdx);
	return m_pMemIdx->Visit(visitor, llHashIdx);
}

int clsNewMemVisitor::OnMemKey(const MemKey_t& tMemKey)
{
	assert(NULL != m_oMemKv.GetDataBlockMgr());
	uint64_t llLogID = 0;
	if (sizeof(llLogID) != static_cast<size_t>(tMemKey.cKeyLen))
	{
		logerr("INVALID MEMKEY cKeyLen %zu expected sizeof(uint64_t)", 
				static_cast<size_t>(tMemKey.cKeyLen));
		return -1;
	}

	assert(sizeof(llLogID) == static_cast<size_t>(tMemKey.cKeyLen));
	memcpy(&llLogID, tMemKey.sKey, tMemKey.cKeyLen);

	clsDataBlock* pDataBlock = 
		m_oMemKv.GetDataBlockMgr()->GetByIdx(tMemKey.iBlockID);
	if (NULL == pDataBlock)
	{
		logerr("m_pDataBlockMgr->GetByIdx key %lu Pos[%u %u] NULL", 
				llLogID, tMemKey.iBlockID, tMemKey.iBlockOffset);
		return -3;
	}

	assert(NULL != pDataBlock);
	HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull())
	{
		logerr("ERROR: key %lu Pos[%u %u] GetHead NULL", 
				llLogID, tMemKey.iBlockID, tMemKey.iBlockOffset);
		return -4;
	}

	return OnHead(oHead);
}


// inner use function

int clsNewMemKv::RawGet(uint64_t llLogID, char*& pValue, uint32_t& iValLen)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);
    dbcomm::HashLock hashlock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	hashlock.ReadLock();

	int ret = m_pMemIdx->Get(tMemKey);
	switch (ret)
	{
	case 1:
		{
			clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
			if (NULL == pDataBlock)
			{
				ret = -3; 
				break;
			}
			
			assert(NULL != pDataBlock);
			HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
			if (oHead.IsNull())
			{
				ret = -5;
				break;
			}

			assert(llLogID == *oHead.pLogID);
			ret = 0;
			if (0 == *oHead.pDataLen)
			{
				pValue = NULL; 
				iValLen = 0;
				break;
			}

			assert(0 < *oHead.pDataLen);
			ret = MayUnCompresse(oHead, pValue, iValLen);
			if (0 != ret)
			{
				logerr("MemCompressor::MayUnCompresse key %lu ret %d", 
						llLogID, ret);
				ret = -6;
				assert(NULL == pValue);
				assert(0 == iValLen);
				break;
			}
		}
		break;
	case 0:
		pValue = NULL;
		iValLen = 0;
		break;
	default:
		logerr("m_pMemIdx->Get key %lu ret %d", llLogID, ret);
		ret = -2;
		break;
	}

	return ret;
}

int clsNewMemKv::Del(uint64_t llLogID)
{
	MemKey_t tMemKey = {0};
	memcpy(tMemKey.sKey, &llLogID, sizeof(llLogID));
	tMemKey.cKeyLen = sizeof(llLogID);

	assert(NULL != m_pHashBaseLock);
	assert(NULL != m_pMemIdx);

    dbcomm::HashLock hashlock(
			m_pHashBaseLock, HashFunc(tMemKey.sKey, tMemKey.cKeyLen));
	hashlock.WriteLock();

	int ret = m_pMemIdx->Get(tMemKey);
	if (0 >= ret) {
		return ret;
	}

	assert(1 == ret); // has key
	assert(NULL != m_pDataBlockMgr);	
	clsDataBlock* pDataBlock = m_pDataBlockMgr->GetByIdx(tMemKey.iBlockID);
	if (NULL == pDataBlock) {
		return -3;
	}

	assert(NULL != pDataBlock);
	HeadWrapper oHead = pDataBlock->GetHead(tMemKey.iBlockOffset, llLogID);
	if (oHead.IsNull()) {
		return -5;
	}

	assert(llLogID == *oHead.pLogID);
	uint8_t& cFlag = *oHead.pFlag;
	cFlag = dbcomm::AddFlag(cFlag, FLAG_DELETE);
	cFlag = dbcomm::ClearFlag(cFlag, dbcomm::RECORD_COMPRESSE);
	pDataBlock->ReportDelOneKey();

	return m_pMemIdx->Del(tMemKey.sKey, tMemKey.cKeyLen);
}


// end of inner use function


// test function

int TestDoMemMergeOn(
		clsNewMemKv& oMemKv, 
		clsDataBlock* pMergeFromBlock, uint32_t& iOffset, 
		clsDataBlock* pMergeToBlock, 
		uint32_t& iTotalCnt, uint32_t& iRealCnt)
{
	bool bStop = false;
	return DoMemMergeOn(
			oMemKv, bStop, 
			pMergeFromBlock, iOffset, 
			pMergeToBlock, iTotalCnt, iRealCnt);
}

// end of test function

} // namespace memkv


