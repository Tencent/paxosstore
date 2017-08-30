
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
#include <string>
#include <vector>
#include <future>
#include <mutex>
#include <functional>
#include "memcomm.h"

namespace paxos {

	class PaxosLog;
} // namespace paxos

namespace dbcomm {
   
class HashBaseLock;

} // namespace dbcomm

namespace memkv {

class clsDataBlockMgr;
class clsDataBlock;
class clsMemIdx;

typedef struct s_MemKey MemKey_t;
typedef struct s_NewBasic NewBasic_t;
typedef struct s_NewHead NewHead_t;


class clsNewMemVisitor;

class clsNewMemKv {

public:
	clsNewMemKv();
	~clsNewMemKv();

	// init function
	int Init(
			uint32_t iIdxHeadSize, 
			const char* sPLogPath, 
			uint32_t iMaxAppendBlockNum,
			int idxShmKey = NEW_IDX_SHM_KEY,
			int dataBlockShmKey = NEW_DATA_BLOCK_SHM_KEY);

	int StartBuildIdx();

	int StartMemMergeThread();

	int ReloadDiskFile(
			bool bForceFullReload, 
			std::function<int(clsNewMemKv&)> pfnLoadHandler, 
			std::function<int(clsNewMemKv&)> pfnReloadHandler);

	// end of init function

	// no RECORD_COMPRESSE
	int Has(uint64_t llLogID);	

	// < 0: err
	// = 0: no pending;
	// = 1: pending
	int IsPendingOn(uint64_t llLogID, uint64_t llIndex);

	int Get(uint64_t llLogID, 
			NewBasic_t& tBasicInfo, paxos::PaxosLog& oPLog);

	int Get(uint64_t llLogID, NewBasic_t& tBasicInfo);

	// no RECORD_COMPRESSE
	int Update(uint64_t llLogID, const NewBasic_t& tBasicInfo);
	int Set(uint64_t llLogID, 
			const NewBasic_t& tBasicInfo, const paxos::PaxosLog& oPLog);

	int BatchSet(
			const std::vector<uint64_t>& vecLogID, 
			const std::vector<NewBasic_t*>& vecBasicInfo, 
			const std::vector<paxos::PaxosLog*>& vecPLog);

	int CheckReqID(
			uint64_t llLogID, 
            uint32_t iExpectedVersion, uint64_t llReqID);

	// build idx
	clsDataBlockMgr* GetDataBlockMgr() 
	{
		return m_pDataBlockMgr;
	}

	clsMemIdx* GetMemIdx()
	{
		return m_pMemIdx;
	}

	int NextBuildBlockID();

	int BuildOneRecord(
			const HeadWrapper& oHead, uint32_t iBlockID, uint32_t iOffset, 
			int& iIdxBlockID);


	// mem merge
	int MergeOneRecord(
			const HeadWrapper& oHead, 
			uint32_t iBlockID, uint32_t iOffset, 
			clsDataBlock& oMergeFromBlock, clsDataBlock& oMergeToBlock);

	// help function for mem visitor
	uint32_t GetIdxHeadSize() const {
		return m_iIdxHeadSize;
	}

	int Visit(clsNewMemVisitor* visitor, uint64_t llHashIdx);

	// inner use function
	int RawGet(uint64_t llLogID, char*& pValue, uint32_t& iValLen);

	int Del(uint64_t llLogID);
	// end of inner use function

private:
	int GetOnExistKeyNoLock(
			const MemKey_t& tMemKey, 
			NewBasic_t& tBasicInfo, paxos::PaxosLog& oPLog);

	int GetBasicInfoNoLock(
			const MemKey_t& tMemKey, 
			NewBasic_t& tBasicInfo);

	int SetNoLock(
			const MemKey_t& tMemKey, 
			const NewBasic_t& tBasicInfo, 
			const paxos::PaxosLog& oPLog);

	int AppendSetNoLock(
			uint64_t llLogID, 
			const NewBasic_t& tBasicInfo, 
			const paxos::PaxosLog& oPLog);


private:
	clsMemIdx* m_pMemIdx;
	clsDataBlockMgr* m_pDataBlockMgr;
    dbcomm::HashBaseLock* m_pHashBaseLock;
	//std::unique_ptr<clsMemIdx> m_pMemIdx;
	//std::unique_ptr<clsDataBlockMgr> m_pDataBlockMgr;
	//std::unique_ptr<HashBaseLock> m_pHashBaseLock;

    std::mutex m_tMutex;
	uint32_t m_iNextBuildBlockID;
	uint32_t m_iIdxHeadSize;
	
	std::string m_sPLogPath;
	bool m_bStop;
	std::future<void> m_tMergeFut;
	// pthread_t m_tMergeThreadID;
}; 

class clsNewMemVisitor : public clsMemBaseVisitor {

public:
	clsNewMemVisitor(clsNewMemKv& oMemKv)
		: m_oMemKv(oMemKv)
	{

	}

	virtual ~clsNewMemVisitor() = default;

	virtual int OnMemKey(const MemKey_t& tMemKey) override final;

	virtual int OnHead(const HeadWrapper& oHead) = 0;

private:
	clsNewMemKv& m_oMemKv;
};


// test INTERFACE

int TestDoMemMergeOn(
		clsNewMemKv& oMemKv, 
		clsDataBlock* pMergeFromBlock, uint32_t& iOffset, 
		clsDataBlock* pMergeToBlock, 
		uint32_t& iTotalCnt, uint32_t& iRealCnt);

// end of test INTERFACE

} // namespace memkv
