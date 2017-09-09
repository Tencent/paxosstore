#ifndef CERTAIN_IOWORKER_H_
#define CERTAIN_IOWORKER_H_

#include "Command.h"
#include "ConnWorker.h"
#include "AsyncQueueMng.h"
#include "Certain.h"

namespace Certain
{

class clsNotifyPipe : public clsFDBase
{
private:
	int m_iOutFD;
	volatile uint8_t m_bBlockedFlag;

public:
	clsNotifyPipe(clsIOHandlerBase *poHandler, int iInFD, int iOutFD)
			: clsFDBase(iInFD, poHandler),
			  m_iOutFD(iOutFD),
			  m_bBlockedFlag(0) { }

	~clsNotifyPipe() { }

	int NotifyByMultiThread();
	int RecvNotify();
};

class clsNotifyPipeMng : public clsSingleton<clsNotifyPipeMng>
{
private:
	map<uint32_t, clsNotifyPipe *> m_tQueueIDMap;

public:
	void AddNotifyPipe(uint32_t iQueueID, clsNotifyPipe *poPipe)
	{
		if (m_tQueueIDMap.find(iQueueID) != m_tQueueIDMap.end())
		{
			AssertLess(iQueueID, 0);
		}
		m_tQueueIDMap[iQueueID] = poPipe;
	}

	clsNotifyPipe *GetNotifyPipe(uint32_t iQueueID)
	{
		clsNotifyPipe *poPipe = m_tQueueIDMap[iQueueID];
		AssertNotEqual(poPipe, NULL);
		return poPipe;
	}
};

class clsIOWorker;
class clsNotifyPipeHandler : public clsIOHandlerBase
{
private:
	clsIOWorker *m_poIOWorker;

public:
	clsNotifyPipeHandler(clsIOWorker *poIOWorker)
			: m_poIOWorker(poIOWorker) { }

	virtual int HandleRead(clsFDBase *poFD)
	{
		clsNotifyPipe *poPipe = dynamic_cast<clsNotifyPipe *>(poFD);

		int iRet = poPipe->RecvNotify();
		if (iRet != 0)
		{
			CertainLogFatal("TODO RecvNotify ret %d", iRet);
		}

		return iRet;
	}

	virtual int HandleWrite(clsFDBase *poFD)
	{
		CERTAIN_EPOLLIO_UNREACHABLE;
	}
};

class clsCatchUpWorker;
class clsIOWorkerRouter : public clsSingleton<clsIOWorkerRouter>
{
private:
	uint64_t m_iWorkerIDGenerator;
	uint64_t m_iSelectCount;
	
	uint32_t m_iIOWorkerNum;
	uint32_t m_iServerNum;

	clsNotifyPipeMng *m_poNotifyPipeMng;
	clsCatchUpWorker *m_poCatchUpWorker;

	bool **m_aaIdleWorkerMap;
	int32_t **m_aaIntChannelCnt;

	uint32_t SelectWorkerID(uint32_t iServerID)
	{
		uint64_t iCount = __sync_fetch_and_add(&m_iSelectCount, 1);
		uint64_t iGenerator = m_iWorkerIDGenerator;

		if (iCount % 200 == 0)
		{
			iGenerator = __sync_fetch_and_add(&m_iWorkerIDGenerator, 1);
		}

		for (uint32_t i = 0; i < m_iIOWorkerNum; ++i)
		{
			uint32_t iWorkerID = (iGenerator + iServerID) % m_iIOWorkerNum;
			if (m_aaIdleWorkerMap[iWorkerID][iServerID])
			{
				break;
			}
			iGenerator++;
		}

		return (iGenerator + iServerID) % m_iIOWorkerNum;
	}

	uint32_t GetIORspQueueID(clsCmdBase *poCmd);

public:
	clsIOWorkerRouter() : m_iWorkerIDGenerator(0),
	                      m_iSelectCount(0),
						  m_iIOWorkerNum(0),
						  m_iServerNum(0),
						  m_poNotifyPipeMng(NULL),
						  m_poCatchUpWorker(NULL),
						  m_aaIdleWorkerMap(NULL),
						  m_aaIntChannelCnt(NULL) { }

	~clsIOWorkerRouter();

	int Init(clsConfigure *poConf);

	int Go(clsCmdBase *poCmd);

	int GoAndDeleteIfFailed(clsCmdBase *poCmd);

	int32_t GetAndAddIntChannelCnt(uint32_t iIOWorkerID, uint32_t iServerID, int32_t iAddVal)
	{
		return __sync_fetch_and_add(&m_aaIntChannelCnt[iIOWorkerID][iServerID], iAddVal);
	}

	int32_t SubAndGetIntChannelCnt(uint32_t iIOWorkerID, uint32_t iServerID, int32_t iSubVal)
	{
		return __sync_sub_and_fetch(&m_aaIntChannelCnt[iIOWorkerID][iServerID], iSubVal);
	}

	void SetIdleWorkerMap(uint32_t iIOWorkerID, uint32_t iServerID, bool bSetVal)
	{
		__sync_bool_compare_and_swap(&m_aaIdleWorkerMap[iIOWorkerID][iServerID], !bSetVal, bSetVal);
	}
};

class clsIOWorker : public clsThreadBase,
					public clsIOHandlerBase
{
private:
	uint32_t m_iWorkerID;
	clsConfigure *m_poConf;

	clsCertainUserBase *m_poCertainUser;

	clsConnInfoMng *m_poConnMng;
	clsAsyncQueueMng *m_poQueueMng;
	clsEpollIO *m_poEpollIO;
	clsNotifyPipe *m_poNotifyPipe;

	char *m_pcIOBuffer;

	vector<InetAddr_t> m_vecServerAddr;

	vector<uint32_t> m_vecChannelRouter;
	vector< vector<clsIOChannel *> > m_vecIntChannel;

	uint32_t m_iServerNum;
	uint32_t m_iLocalServerID;
	uint32_t m_iIntConnLimit;

	uint32_t m_iNextFDID;
	uint64_t m_iLastMakeSrvConnTimeMS;
	uint64_t m_iLastServeNewConnTimeMS;

	clsNotifyPipeHandler *m_poNotifyPipeHandler;

	//clsObjReusedPool<clsPaxosCmd> *m_poPaxosCmdPool;

	uint64_t m_iNextFlushTimeUS;
	set<clsIOChannel *> m_tWritableChannelSet;

	int MakeSingleSrvConn(uint32_t iServerID);
	int MakeSrvConn();

	int PutToIOReqQueue(clsIOChannel *poChannel, clsCmdBase *poCmd);
	int ParseIOBuffer(clsIOChannel *poChannel, char *pcBuffer, uint32_t iSize);

	virtual int HandleRead(clsFDBase *poFD);
	virtual int HandleWrite(clsFDBase *poFD);

	void ServeNewConn();
	void ConsumeIORspQueue();

	void FlushWritableChannel();
	void CleanBrokenChannel(clsIOChannel *poChannel);

	clsIOChannel *GetIOChannelByServerID(uint32_t iServerID);
	clsIOChannel *GetIOChannel(clsCmdBase *poCmd);

	void AddNotifyPipe();

	//void ReduceChannel();
	void UpdateSvrAddr();
	void RemoveChannel(uint32_t iServerID);

public:
	clsIOWorker(uint32_t iWorkerID, clsConfigure *poConf)
			: m_iWorkerID(iWorkerID),
			  m_poConf(poConf),
			  m_poNotifyPipe(NULL),
			  m_iNextFDID(iWorkerID),
			  m_iLastMakeSrvConnTimeMS(0),
			  m_iLastServeNewConnTimeMS(0)
	{
		m_poCertainUser = clsCertainWrapper::GetInstance()->GetCertainUser();

		m_poConnMng = clsConnInfoMng::GetInstance();
		m_poQueueMng = clsAsyncQueueMng::GetInstance();
		m_poEpollIO = new clsEpollIO;

		m_iServerNum = m_poConf->GetServerNum();
		m_iLocalServerID = m_poConf->GetLocalServerID();

		m_iIntConnLimit = m_poConf->GetIntConnLimit();

		m_pcIOBuffer = (char *)malloc(IO_BUFFER_SIZE);
		assert(m_pcIOBuffer != NULL);

		m_vecServerAddr = m_poConf->GetServerAddrs();

		AssertEqual(m_vecServerAddr.size(), m_iServerNum);

		m_vecChannelRouter.resize(m_iServerNum, 0);
		m_vecIntChannel.resize(m_iServerNum);

		m_poNotifyPipeHandler = new clsNotifyPipeHandler(this);
		AddNotifyPipe();

		//m_poPaxosCmdPool = new clsObjReusedPool<clsPaxosCmd>(10000);

		m_iNextFlushTimeUS = 0;
	}

	virtual ~clsIOWorker()
	{
		delete m_poNotifyPipeHandler, m_poNotifyPipeHandler = NULL;

		free(m_pcIOBuffer), m_pcIOBuffer = NULL;

		delete m_poEpollIO, m_poEpollIO = NULL;
	}

	void Run();

	void PrintConnInfo();

	static void InitUseTimeStat();
	static void PrintUseTimeStat();
};

} // namespace Certain

#endif
