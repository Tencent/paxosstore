
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_INCLUDE_CERTAIN_H_
#define CERTAIN_INCLUDE_CERTAIN_H_

#include "Command.h"
#include "PerfLog.h"
#include "AsyncPipeMng.h"
#include "AsyncQueueMng.h"
#include "UUIDGroupMng.h"
#include "EntityInfoMng.h"

namespace Certain
{

struct EntityMeta_t
{
	uint64_t iMaxCommitedEntry;
	uint64_t iMaxPLogEntry;
	uint64_t iValueID;
	bool bChosen;

	uint32_t iDBFlag;
};

struct PLogEntityMeta_t
{
	uint64_t iMaxPLogEntry;
};

class clsCertainUserBase
{
public:
	virtual ~clsCertainUserBase()
	{
	}
	virtual void LockEntity(uint64_t iEntityID, void **ppLockInfo)
    {
    }
	virtual void BatchLockEntity(map<uint64_t, uint32_t> &, void **ppLockInfo)
    {
    }
	virtual void UnLockEntity(void *ppLockInfo)
    {
    }

	virtual void LockPLogEntity(uint64_t iEntityID, void **ppLockInfo)
	{
		assert(false);
	}

	virtual void UnLockPLogEntity(void *ppLockInfo)
	{
		assert(false);
	}

	virtual uint32_t GetAcceptorNum()
	{
		return 3;
	}

	virtual int GetLocalAcceptorID(uint64_t iEntityID,
			uint32_t &iLocalAcceptorID)
    {
        return 0;
    }

	virtual int GetServerID(uint64_t iEntityID,
			uint32_t iAcceptorID, uint32_t &iServerID)
    {
        return 0;
    }

	virtual int InitServerAddr(clsConfigure *poConf)
	{
		// If ServerAddrs and ExtAddr have defined in certain.conf,
		// it's no need implement this function.
		return 0;
	}

	virtual const char *GetCertainConfPath()
    {
        return NULL;
    }

	virtual void OnReady()
	{
		// If you have nothing to do when Certain is ready,
		// it's no need implement this function.
	}

	virtual void WaitTimeout(uint64_t iTimeoutMS)
    {
        usleep(iTimeoutMS * 1000);
    }

	virtual int GetSvrAddr(uint64_t iEntityID, uint32_t iAcceptorID,
			Certain::InetAddr_t & tAddr)
	{
		assert(false);
		return -1;
	}

	virtual uint32_t GetStartRoutineID()
    {
        return 0;
    }

	typedef int (*CallBackType)();
	virtual CallBackType HandleLockCallBack()
    {
        return NULL;
    }

	virtual bool IsRejectAll()
	{
		return false;
	}

	virtual int GetAllEntityID(vector<uint64_t> &vecEntityID)
	{
		assert(false);
		return -1;
	}

    virtual string GetConfSuffix()
    {
        return "";
    }

    virtual int GetControlGroupID(uint64_t iEntityID)
    {
        return -1;
    }

    virtual uint32_t GetControlGroupLimit()
    {
        // conservative 50% limit
        return MAX_ASYNC_PIPE_NUM / 2;
    }

	virtual int UpdateServerAddr(Certain::clsConfigure *poConf)
    {
        return 0;
    }

};

// iValue == 0 for PutRecord, iValue > 0 for PutValue
struct PLogReq_t
{
	uint64_t iEntityID;
	uint64_t iEntry;
	uint64_t iValueID;
	string strData;
};

class clsPLogBase
{
public:
	virtual ~clsPLogBase()
	{
	}
	virtual int MultiPut(const vector<PLogReq_t> &vecPLogReq)
	{
		assert(false);
		return -1;
	}

	virtual int PutValue(uint64_t iEntityID, uint64_t iEntry,
			uint64_t iValueID, const string &strValue) = 0;

	virtual int GetValue(uint64_t iEntityID, uint64_t iEntry,
			uint64_t iValueID, string &strValue) = 0;

	virtual int Put(uint64_t iEntityID, uint64_t iEntry,
			const string &strRecord) = 0;

	virtual int PutWithPLogEntityMeta(uint64_t iEntityID, uint64_t iEntry,
			const PLogEntityMeta_t &tMeta, const string &strRecord)
	{
		assert(false);
		return -1;
	}

	virtual int GetPLogEntityMeta(uint64_t iEntityID, PLogEntityMeta_t &tMeta)
	{
		return -1;
	}

	virtual int Get(uint64_t iEntityID, uint64_t iEntry,
			string &strRecord) = 0;

	virtual int LoadUncommitedEntrys(uint64_t iEntityID,
			uint64_t iMaxCommitedEntry, uint64_t iMaxLoadingEntry,
			vector< pair<uint64_t, string> > &vecRecord, bool &bHasMore) = 0;

	int GetRecord(uint64_t iEntityID, uint64_t iEntry, EntryRecord_t &tSrcRecord, const EntryRecord_t* ptDestRecord = NULL);
	int PutRecord(uint64_t iEntityID, uint64_t iEntry, uint64_t iMaxPLogEntry, EntryRecord_t tRecord);
	int PutRecord(uint64_t iEntityID, uint64_t iEntry, EntryRecord_t tRecord, vector<PLogReq_t> &vecPLogReq);

	static void PrintUseTimeStat();
	static void InitUseTimeStat();
};

class clsDBBase
{
public:
	virtual ~clsDBBase()
	{
	}
	struct KeyValue_t 
	{
		uint64_t iEntity;
		uint64_t iEntry;
		std::string  * pStrValue;
	};

	virtual int Submit(clsClientCmd *poClientCmd, string &strWriteBatch)
	{
		// For Certain control.
		assert(false);
		return -1;
	}

	virtual int Commit(uint64_t iEntityID, uint64_t iEntry,
			const string &strWriteBatch) = 0;

	virtual int MultiCommit(KeyValue_t * pRec, int iCnt)
	{
		assert(false);
		return -1;
	}

	virtual int LoadMaxCommitedEntry(uint64_t iEntityID,
			uint64_t &iCommitedEntry, uint32_t &iFlag) = 0;

	virtual int GetAllAndSet(uint64_t iEntityID, uint32_t iAcceptorID,  uint64_t &iMaxCommitPos)
    {
        assert(false);
        return -1;
    }

	virtual int RecoverData(uint32_t iHeadUin, const void* const ptReq)
	{
		assert(false);
		return -1;
	}

	virtual int GetAllForCertain(const uint32_t iHeadUin, const void * const ptReq, void * ptRsp)
	{
		assert(false);
		return -1;
	}
	virtual int GetStartSeq(uint32_t iHeadUin, uint32_t & iStartSeq)
	{
		return -1;
	}
	virtual int UpdateStartSeq(uint32_t iHeadUin,  uint32_t iStartSeq)
	{
		return -1;
	}

	virtual int LoadMaxCommitedEntry(uint64_t iEntityID,
			uint64_t &iCommitedEntry, uint32_t &iFlag, uint32_t & iSeq)
	{
		return -1;
	}	


	static int MultiCommit(uint64_t iEntityID, uint64_t iMaxCommitedEntry, uint64_t iMaxTaskEntry);
};

class clsEntityInfoMng;
class clsCertainWrapper : public clsSingleton<clsCertainWrapper>,
						  public clsThreadBase
{
private:
	clsConfigure *m_poConf;
	clsPLogBase *m_poPLogEngine;
	clsDBBase *m_poDBEngine;
	clsEntityGroupMng *m_poEntityGroupMng;
	clsAsyncQueueMng *m_poQueueMng;
	clsAsyncPipeMng *m_poPipeMng;

	clsCertainUserBase *m_poCertainUser;

	friend class clsSingleton<clsCertainWrapper>;
	clsCertainWrapper() : m_poConf(NULL),
						  m_poPLogEngine(NULL),
						  m_poDBEngine(NULL),
						  m_poEntityGroupMng(NULL),
						  m_poQueueMng(NULL),
						  m_poPipeMng(NULL),
						  m_poCertainUser(NULL) { }

	vector<clsThreadBase *> m_vecWorker;

	int InitWorkers();
	void DestroyWorkers();

	int StartWorkers();
	void StopWorkers();

	int InitManagers();
	void DestroyManagers();

	int SyncWaitCmd(clsClientCmd *poCmd);

public:
	int Init(clsCertainUserBase *poCertainUser, clsPLogBase *poPLogEngine,
			clsDBBase *poDBEngine, int iArgc = 0, char *pArgv[] = NULL);
	void Destroy();

	void Run();

	// compatible with gperftools
	//virtual ~clsCertainWrapper() { }

	bool CheckIfAllWorkerExited();

	void TriggeRecover(uint64_t iEntityID, uint64_t iCommitedEntry);

	int CheckDBStatus(uint64_t iEntityID, uint64_t iCommitedEntry);

	int GetWriteBatch(uint64_t iEntityID, uint64_t iEntry,
			string &strWriteBatch, uint64_t *piValueID = NULL);

	// strWriteBatch.size() == 0 <==> readonly
	int RunPaxos(uint64_t iEntityID, uint64_t iEntry, uint16_t hSubCmdID,
			const vector<uint64_t> &vecWBUUID, const string &strWriteBatch);

	int CatchUpAndRunPaxos(uint64_t iEntityID,  uint16_t hSubCmdID,
			const vector<uint64_t> &vecWBUUID, const string &strWriteBatch);

	int EntityCatchUp(uint64_t iEntityID, uint64_t &iMaxCommitedEntry);

	int GetMaxCommitedPos(uint64_t iEntityID, uint64_t &iEntry);
	int GetMetaInfo(uint64_t iEntityID, uint64_t & iEntry, uint32_t & iSeq);
	int GetMaxChosenEntry(uint64_t iEntityID, uint64_t &iMaxChosenEntry);

	int GetEntrys(uint64_t iEntityID, uint64_t iStartEntry, std::vector< std::pair<uint64_t, std::string> >& vecEntry);

	int GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo, EntityMeta_t &tMeta);

	clsPLogBase *GetPLogEngine() { return m_poPLogEngine; }
	clsDBBase *GetDBEngine() { return m_poDBEngine; }
	clsCertainUserBase *GetCertainUser() { return m_poCertainUser; }

	int EvictEntity(uint64_t iEntityID);

    int ExplicitGetAll(uint64_t iEntityID);

	clsConfigure *GetConf();
};

} // namespace Certian

#endif
