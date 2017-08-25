
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_ENTITYINFOMNG_H_
#define CERTAIN_ENTITYINFOMNG_H_

#include "Common.h"
#include "Configure.h"
#include "EntryInfoMng.h"

namespace Certain
{

struct EntityInfo_t
{
	uint64_t iEntityID;

	volatile uint64_t iMaxPLogEntry;
	volatile uint64_t iMaxChosenEntry;
	volatile uint64_t iMaxContChosenEntry;

	uint64_t iLocalPreAuthEntry;
	uint64_t iCatchUpEntry;
	uint64_t iValueIDGenerator;
	uint64_t iLeaseExpiredTimeMS;
	uint64_t iNotifyedEntry;
	uint64_t iGetAllFinishTimeMS;

	clsClientCmd *poClientCmd;
	clsPaxosCmd **apWaitingMsg;

	CIRCLEQ_HEAD(EntryList_t, EntryInfo_t) tEntryList;

	uint32_t iLocalAcceptorID;
	uint32_t iActiveAcceptorID;
	uint32_t iWaitingSize;

	volatile int32_t iRefCount;

	bool bRangeLoading;
	bool bRangeLoaded;

	bool bGetAllPending;
};

class clsCertainUserBase;

class clsEntityInfoTableBase
{
public:
	virtual ~clsEntityInfoTableBase() { }

	virtual EntityInfo_t *Find(uint64_t iEntityID) = 0;
	virtual void Add(uint64_t iEntityID, EntityInfo_t *ptEntityInfo) = 0;
	virtual void Remove(uint64_t iEntityID) = 0;
	virtual void SetMaxSize(uint32_t iMaxSize) = 0;
	virtual bool CheckForEliminate(EntityInfo_t *&ptEntityInfo) = 0;
	virtual bool Refresh(uint64_t iEntityID) = 0;
};

class clsSmallEntityInfoTable : public clsEntityInfoTableBase
{
private:
	uint32_t m_iMaxEntityID;
	EntityInfo_t *m_ptEntityInfo;
	clsPaxosCmd** m_ptWaitingMsg;

public:
	clsSmallEntityInfoTable(uint32_t iMaxSize, uint32_t iAcceptorNum);
	virtual ~clsSmallEntityInfoTable();

	virtual EntityInfo_t *Find(uint64_t iEntityID);
	virtual void Add(uint64_t iEntityID, EntityInfo_t *ptEntityInfo);
	virtual void Remove(uint64_t iEntityID);
	virtual void SetMaxSize(uint32_t iMaxSize);
	virtual bool CheckForEliminate(EntityInfo_t *&ptEntityInfo);
	virtual bool Refresh(uint64_t iEntityID);
};

class clsLargeEntityInfoTable : public clsEntityInfoTableBase
{
private:
	uint32_t m_iMaxEntityNum;
	clsLRUTable<uint64_t, EntityInfo_t*> *m_poLRUTable;

public:
	clsLargeEntityInfoTable(uint32_t iMaxSize);
	virtual ~clsLargeEntityInfoTable();

	virtual EntityInfo_t *Find(uint64_t iEntityID);
	virtual void Add(uint64_t iEntityID, EntityInfo_t *ptEntityInfo);
	virtual void Remove(uint64_t iEntityID);
	virtual void SetMaxSize(uint32_t iMaxSize);
	virtual bool CheckForEliminate(EntityInfo_t *&ptEntityInfo);
	virtual bool Refresh(uint64_t iEntityID);
};

class clsMemCacheCtrl
{
private:
	clsConfigure *m_poConf;

	uint64_t m_iTotalSize;
	uint64_t m_iMaxSize;

	uint32_t m_iAcceptorNum;
	uint32_t m_iMaxMemCacheSizeMB;

	void UpdateMaxSize();

public:
	clsMemCacheCtrl(clsConfigure *poConf) : m_poConf(poConf),
											m_iTotalSize(0),
											m_iMaxSize(0),
											m_iAcceptorNum(poConf->GetAcceptorNum()),
											m_iMaxMemCacheSizeMB(0) { }

	~clsMemCacheCtrl() { }

	void RemoveFromTotalSize(EntryInfo_t *ptInfo);

	void UpdateTotalSize(EntryInfo_t *ptInfo);

	void RemoveFromTotalSize(EntityInfo_t *ptEntityInfo);

	void UpdateTotalSize(EntityInfo_t *ptEntityInfo);

	uint64_t GetTotalSize() { return m_iTotalSize; }

	bool IsOverLoad(bool bReport = true);

	bool IsAlmostOverLoad();
};

class clsEntityInfoMng
{
private:
	clsConfigure *m_poConf;
	clsCertainUserBase *m_poCertainUser;

	uint32_t m_iAcceptorNum;
	uint32_t m_iMaxEntityNum;
	clsEntityInfoTableBase *m_poEntityInfoTable;

	// For print log.
	uint64_t m_iCreateCnt;
	uint64_t m_iDestroyCnt;
	uint32_t m_iEntityWorkerID;

	clsRWLock m_oRWLock;

	clsMemCacheCtrl *m_poMemCacheCtrl;

public:
	clsEntityInfoMng(clsConfigure *poConf, uint32_t iEntityWorkerID);
	~clsEntityInfoMng();

	static uint64_t GenerateValueID(EntityInfo_t *ptEntityInfo,
			uint32_t iProposalNum);

	EntityInfo_t *FindEntityInfo(uint64_t iEntityID);

	EntityInfo_t *CreateEntityInfo(uint64_t iEntityID);
	void DestroyEntityInfo(EntityInfo_t *ptEntityInfo);

	void RefEntityInfo(EntityInfo_t *ptEntityInfo);

	// This MUST be called after RefEntityInfo.
	// An EntityInfo may be eliminated by LRU in EntityWorker,
	// if it has no external ref.
	void UnRefEntityInfo(EntityInfo_t *ptEntityInfo);

	int GetMaxChosenEntry(uint64_t iEntityID, uint64_t &iMaxContChosenEntry,
			uint64_t &iMaxChosenEntry);

	int GetMaxChosenEntry(uint64_t iEntityID, uint64_t &iMaxContChosenEntry,
			uint64_t &iMaxChosenEntry, uint64_t &iLeaseTimeout);

	// Return true iff entity not limited.
	bool CheckAndEliminate();

	EntityInfo_t *PeekOldest();

	void UpdateSuggestedLease(EntityInfo_t *ptEntityInfo);
	bool IsWaitLeaseTimeout(EntityInfo_t *ptEntityInfo);

	bool Refresh(EntityInfo_t *ptEntityInfo);

	int GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo);

	void SetMemCacheCtrl(clsMemCacheCtrl *poMemCacheCtrl)
	{
		m_poMemCacheCtrl = poMemCacheCtrl;
	}

	clsMemCacheCtrl *GetMemCacheCtrl()
	{
		return m_poMemCacheCtrl;
	}
};

class clsEntityGroupMng : public clsSingleton<clsEntityGroupMng>
{
private:
	clsConfigure *m_poConf;
	clsEntityInfoMng *m_apEntityMng[MAX_ENTITY_WORKER_NUM];

	friend class clsSingleton<clsEntityGroupMng>;
	clsEntityGroupMng() { }

public:
	int Init(clsConfigure *poConf);
	void Destroy() { }

	void AddEntityInfoMng(uint32_t iEntityWorkerID,
			clsEntityInfoMng *poEntityMng);

	int GetMaxChosenEntry(uint64_t iEntityID, uint64_t &iMaxContChosenEntry,
			uint64_t &iMaxChosenEntry);

	int GetMaxChosenEntry(uint64_t iEntityID, uint64_t &iMaxContChosenEntry,
			uint64_t &iMaxChosenEntry, uint64_t &iLeaseTimeout);

	int GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo);
};

class clsAutoUnRefEntityInfo
{
private:
	clsEntityInfoMng *m_poEntityMng;
	EntityInfo_t *m_ptEntityInfo;

public:
	clsAutoUnRefEntityInfo(clsEntityInfoMng *poEntityMng,
			EntityInfo_t *ptEntityInfo)
	{
		AssertNotEqual(ptEntityInfo, NULL);
		m_poEntityMng = poEntityMng;
		m_ptEntityInfo = ptEntityInfo;
		AssertLess(0, ptEntityInfo->iRefCount);
	}

	~clsAutoUnRefEntityInfo()
	{
		m_poEntityMng->UnRefEntityInfo(m_ptEntityInfo);
	}
};

class clsAutoEntityLock
{
private:
	uint64_t m_iEntityID;
	void *m_pLockInfo;

public:
	clsAutoEntityLock(std::map<uint64_t, uint32_t> & mapEntity);
	clsAutoEntityLock(uint64_t iEntityID);
	~clsAutoEntityLock();
};

class clsAutoPLogEntityLock
{
private:
	uint64_t m_iEntityID;
	void *m_pLockInfo;

public:
	clsAutoPLogEntityLock(uint64_t iEntityID);
	~clsAutoPLogEntityLock();
};

} // namespace Certain

#endif
