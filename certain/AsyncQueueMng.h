#ifndef CERTAIN_ASYNCQUEUEMNG_H_
#define CERTAIN_ASYNCQUEUEMNG_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsCmdBase;
class clsClientCmd;
class clsPaxosCmd;

typedef clsCircleQueue<clsCmdBase *> clsIOReqQueue;
typedef clsCircleQueue<clsCmdBase *> clsIORspQueue;

typedef clsCircleQueue<clsCmdBase *> clsPLogReqQueue;
typedef clsCircleQueue<clsCmdBase *> clsPLogRspQueue;

typedef clsCircleQueue<clsClientCmd *> clsDBReqQueue;

typedef clsCircleQueue<clsPaxosCmd *> clsGetAllReqQueue;
typedef clsCircleQueue<clsPaxosCmd *> clsGetAllRspQueue;

typedef clsCircleQueue<clsPaxosCmd *> clsCatchUpReqQueue;

typedef clsCircleQueue<clsPaxosCmd *> clsPLogWriteReqQueue;

class clsAsyncQueueMng : public clsSingleton<clsAsyncQueueMng>
{
private:
	uint32_t m_iIOWorkerNum;
	uint32_t m_iEntityWorkerNum;
	uint32_t m_iPLogWorkerNum;
	uint32_t m_iDBWorkerNum;
	uint32_t m_iGetAllWorkerNum;
	uint32_t m_iPLogWriteWorkerNum;

	uint32_t m_iIOQueueSize;
	uint32_t m_iPLogQueueSize;
	uint32_t m_iDBQueueSize;
	uint32_t m_iCatchUpQueueSize;
	uint32_t m_iGetAllQueueSize;
	uint32_t m_iPLogWriteQueueSize;

	clsIOReqQueue **m_ppIOReqQueue;
	clsIORspQueue **m_ppIORspQueue;

	clsPLogReqQueue **m_ppPLogReqQueue;
	clsPLogRspQueue **m_ppPLogRspQueue;

	clsDBReqQueue **m_ppDBReqQueue;

	clsGetAllReqQueue **m_ppGetAllReqQueue;
	clsGetAllRspQueue **m_ppGetAllRspQueue;

	clsCatchUpReqQueue **m_ppCatchUpReqQueue;

	clsPLogWriteReqQueue **m_ppPLogWriteReqQueue;

	// No outside instance allowed
	friend class clsSingleton<clsAsyncQueueMng>;
	clsAsyncQueueMng() : m_ppIOReqQueue(NULL),
					     m_ppIORspQueue(NULL),
						 m_ppPLogReqQueue(NULL),
						 m_ppPLogRspQueue(NULL),
						 m_ppDBReqQueue(NULL),
						 m_ppGetAllReqQueue(NULL),
						 m_ppGetAllRspQueue(NULL),
						 m_ppPLogWriteReqQueue(NULL) { }

public:
	virtual ~clsAsyncQueueMng() { }

	#define NEW_QUEUE_ARRAY(name, arr_size, que_size) \
	do { \
		assert(m_pp##name == NULL); \
		m_pp##name = new cls##name *[arr_size]; \
		for (uint32_t i = 0; i < arr_size; ++i) \
		{ \
			m_pp##name[i] = new cls##name(que_size); \
		} \
	} while (0);

	void PrintAllStat();

	int Init(clsConfigure *poConf)
	{
		m_iIOWorkerNum = poConf->GetIOWorkerNum();
		m_iEntityWorkerNum = poConf->GetEntityWorkerNum();
		m_iPLogWorkerNum = poConf->GetPLogWorkerNum();
		m_iDBWorkerNum = poConf->GetDBWorkerNum();
		m_iGetAllWorkerNum = poConf->GetGetAllWorkerNum();
		m_iPLogWriteWorkerNum = poConf->GetPLogWriteWorkerNum();

		m_iIOQueueSize = poConf->GetIOQueueSize();
		m_iPLogQueueSize = poConf->GetPLogQueueSize();
		m_iDBQueueSize = poConf->GetDBQueueSize();
		m_iCatchUpQueueSize = poConf->GetCatchUpQueueSize();
		m_iGetAllQueueSize = poConf->GetGetAllQueueSize();
		m_iPLogWriteQueueSize = poConf->GetPLogWriteQueueSize();

		// In IO Worker
		NEW_QUEUE_ARRAY(IORspQueue, m_iIOWorkerNum, m_iIOQueueSize);

		// In Entity Worker
		NEW_QUEUE_ARRAY(IOReqQueue, m_iEntityWorkerNum, m_iIOQueueSize);
		NEW_QUEUE_ARRAY(PLogRspQueue, m_iEntityWorkerNum, m_iPLogQueueSize);
		NEW_QUEUE_ARRAY(GetAllRspQueue, m_iEntityWorkerNum, m_iGetAllQueueSize);

		// In PLog Worker
		NEW_QUEUE_ARRAY(PLogReqQueue, m_iPLogWorkerNum, m_iPLogQueueSize);

		// In DB Worker
		NEW_QUEUE_ARRAY(DBReqQueue, m_iDBWorkerNum, m_iDBQueueSize);

		//In GetAll Worker
		NEW_QUEUE_ARRAY(GetAllReqQueue, m_iGetAllWorkerNum, m_iGetAllQueueSize);

		//In CatchUp Worker
		NEW_QUEUE_ARRAY(CatchUpReqQueue, 1, m_iCatchUpQueueSize);

		// In PLogWrite Worker
		NEW_QUEUE_ARRAY(PLogWriteReqQueue, m_iPLogWriteWorkerNum, m_iPLogWriteQueueSize);

		return 0;
	}

	#define DELETE_QUEUE_ARRAY(name, arr_size) \
	do { \
		assert(m_pp##name != NULL); \
		for (uint32_t i = 0; i < arr_size; ++i) \
		{ \
			assert(m_pp##name[i] != NULL); \
			delete m_pp##name[i], m_pp##name[i] = NULL; \
		} \
		delete m_pp##name, m_pp##name = NULL; \
	} while (0);

	void Destroy()
	{
		// In IO Worker
		DELETE_QUEUE_ARRAY(IORspQueue, m_iIOWorkerNum);

		// In Entity Worker
		DELETE_QUEUE_ARRAY(IOReqQueue, m_iEntityWorkerNum);
		DELETE_QUEUE_ARRAY(PLogRspQueue, m_iEntityWorkerNum);
		DELETE_QUEUE_ARRAY(GetAllRspQueue, m_iEntityWorkerNum);

		// In PLog Worker
		DELETE_QUEUE_ARRAY(PLogReqQueue, m_iPLogWorkerNum);

		// In DB Worker
		DELETE_QUEUE_ARRAY(DBReqQueue, m_iDBWorkerNum);

		//In GetAll Worker
		DELETE_QUEUE_ARRAY(GetAllReqQueue, m_iGetAllWorkerNum);

		//In CatchUp Worker
		DELETE_QUEUE_ARRAY(CatchUpReqQueue, 1);

		// In PLogWrite Worker
		DELETE_QUEUE_ARRAY(PLogWriteReqQueue, m_iPLogWriteWorkerNum);
	}

	#define GET_QUEUE_FUNC(name, iQueueNum) \
	cls##name *Get##name(uint32_t iID) \
	{ \
		AssertLess(iID, iQueueNum); \
		return m_pp##name[iID]; \
	}

	GET_QUEUE_FUNC(IORspQueue, m_iIOWorkerNum);

	GET_QUEUE_FUNC(IOReqQueue, m_iEntityWorkerNum);
	GET_QUEUE_FUNC(PLogRspQueue, m_iEntityWorkerNum);
	GET_QUEUE_FUNC(GetAllRspQueue, m_iEntityWorkerNum);

	GET_QUEUE_FUNC(PLogReqQueue, m_iPLogWorkerNum);

	GET_QUEUE_FUNC(DBReqQueue, m_iDBWorkerNum);

	GET_QUEUE_FUNC(GetAllReqQueue, m_iGetAllWorkerNum);

	GET_QUEUE_FUNC(CatchUpReqQueue, 1);

	GET_QUEUE_FUNC(PLogWriteReqQueue, m_iPLogWriteWorkerNum);
};

} // namespace Certain

#endif
