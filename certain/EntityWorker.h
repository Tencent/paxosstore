#ifndef CERTAIN_ENTITYWORKER_H_
#define CERTAIN_ENTITYWORKER_H_

#include "Command.h"
#include "EntryState.h"
#include "PLogWorker.h"
#include "DBWorker.h"
#include "EntryInfoMng.h"
#include "UUIDGroupMng.h"
#include "CatchUpWorker.h"
#include "GetAllWorker.h"

namespace Certain
{

class clsEntityWorker : public clsThreadBase
{
private:
	uint32_t m_iWorkerID;
	clsConfigure *m_poConf;

	uint32_t m_iAcceptorNum;
	uint32_t m_iIOWorkerNum;

	clsUUIDGroupMng *m_poUUIDMng;
	clsEntityInfoMng *m_poEntityMng;
	clsEntryInfoMng *m_poEntryMng;
	clsIOWorkerRouter *m_poIOWorkerRouter;
	clsCmdFactory *m_poCmdFactory;

	clsIOReqQueue *m_poIOReqQueue;
	clsPLogRspQueue *m_poPLogRspQueue;
	clsGetAllRspQueue * m_poGetAllRspQueue;

	// sum of list size <= MAX_ASYNC_PIPE_NUM
	list<clsClientCmd *> m_poWaitingGoList;

	clsMemCacheCtrl *m_poMemCacheCtrl;

    clsRandom *m_poRandom;

	// for DoWithIOReq
	int DoWithRecoverCmd(clsRecoverCmd *poCmd);
	int DoWithClientCmd(clsClientCmd *poCmd);
	int DoWithPaxosCmd(clsPaxosCmd *poPaxosCmd);

	void InvalidClientCmd(EntityInfo_t *ptEntityInfo,
			int iResult = eRetCodeFailed);
	bool InvalidClientCmd(clsClientCmd *poCmd, int iResult = eRetCodeFailed);

	// for DoWithPaxosCmd
	int LimitedCatchUp(EntityInfo_t *ptEntityInfo, uint32_t iDestAcceptorID);
	void CheckForCatchUp(EntityInfo_t *ptEntityInfo,
			uint32_t iDestAcceptorID, uint64_t iGlobalMaxChosenEntry);
	int UpdateRecord(clsPaxosCmd *poPaxosCmd);

	int DoWithWaitingMsg(clsPaxosCmd **apWaitingMsg, uint32_t iCnt);
	int DoWithIOReq(clsCmdBase *poCmd);
	int DoWithPLogRsp(clsCmdBase *poCmd);
	int DoWithPaxosCmdFromPLog(clsPaxosCmd *poPaxosCmd);
	int DoWithTimeout(EntryInfo_t *ptInfo);
	int DoWithGetAllRsp(clsPaxosCmd *poPaxosCmd);

	bool ActivateEntry(EntryInfo_t *ptInfo);

	void BroadcastToRemote(EntryInfo_t *ptInfo,
			clsEntryStateMachine *poMachine, clsClientCmd *poCmd = NULL);
	void CleanUpEntry(EntryInfo_t *ptInfo);
	void SyncEntryRecord(EntryInfo_t *ptInfo, uint32_t iDestAcceptorID,
			uint64_t iUUID);

	void PushCmdToDBWorker(EntryInfo_t *ptInfo);

	int LoadFromPLogWorker(EntryInfo_t *ptInfo);
	int RecoverFromPLogWorker(clsPaxosCmd *poCmd);

	int RangeLoadFromPLog(EntityInfo_t *ptEntityInfo);
	int RangeRecoverFromPLog(clsRecoverCmd *poCmd);

	void UpdateMaxChosenEntry(EntityInfo_t *ptEntityInfo,
			EntryInfo_t *ptInfo);
	void CheckIfWaitRecoverCmd(EntityInfo_t *ptEntityInfo,
			clsRecoverCmd *poCmd);

	bool CheckIfEntryNumLimited(uint64_t iSkippedEntityID = INVALID_ENTITY_ID,
			bool bEliminateMidState = true, bool bIngoreMemOverLoad = false);
	bool CleanUpCommitedDeadEntry(bool bPrintOldestEntry);
	bool EvictEntity(EntityInfo_t *ptEntityInfo);

	int RecoverEntry(EntryInfo_t *ptInfo, const EntryRecord_t &tRecord);

	void CheckIfNeedNotifyDB(EntityInfo_t *ptEntityInfo);

	int ProposeNoop(EntityInfo_t *ptEntityInfo, EntryInfo_t *ptInfo);

	int RecoverEntityInfo(uint64_t iEntityID, const EntityMeta_t &tMeta);

	void ElimateEntryForRoom();

    uint32_t GetPeerAcceptorID(EntityInfo_t *ptEntityInfo);

public:
	clsEntityWorker(uint32_t iWorkerID, clsConfigure *poConf)
			: m_iWorkerID(iWorkerID), m_poConf(poConf)
	{
		m_iAcceptorNum = m_poConf->GetAcceptorNum();
		m_iIOWorkerNum = m_poConf->GetIOWorkerNum();

		clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
		m_poCmdFactory = clsCmdFactory::GetInstance();
		m_poUUIDMng = clsUUIDGroupMng::GetInstance();

		m_poIOReqQueue = poQueueMng->GetIOReqQueue(m_iWorkerID);
		m_poPLogRspQueue = poQueueMng->GetPLogRspQueue(m_iWorkerID);
		m_poGetAllRspQueue = poQueueMng->GetGetAllRspQueue(m_iWorkerID);

		m_poMemCacheCtrl = new clsMemCacheCtrl(poConf);

		m_poEntryMng = new clsEntryInfoMng(poConf, m_iWorkerID);

		m_poEntityMng = new clsEntityInfoMng(m_poConf, m_iWorkerID);
		m_poEntityMng->SetMemCacheCtrl(m_poMemCacheCtrl);

		m_poEntryMng->SetEntityInfoMng(m_poEntityMng);

		clsEntityGroupMng::GetInstance()->AddEntityInfoMng(
				iWorkerID, m_poEntityMng);

		m_poIOWorkerRouter = clsIOWorkerRouter::GetInstance();

        m_poRandom = new clsRandom(20170120);
	}

	virtual ~clsEntityWorker()
	{
		delete m_poEntityMng, m_poEntityMng = NULL;
		delete m_poEntryMng, m_poEntryMng = NULL;
	}

	void Run();

	static int EnterPLogRspQueue(clsCmdBase *poCmd);
	static int EnterGetAllRspQueue(clsPaxosCmd *poCmd);
	static int EnterIOReqQueue(clsClientCmd *poCmd);
};

} // namespace Certain

#endif
