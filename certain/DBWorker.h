
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_DBWORKER_H_
#define CERTAIN_DBWORKER_H_

#include "Certain.h"
#include "IOWorker.h"

namespace Certain
{

class clsDBWorker : public clsThreadBase
{
private:
	uint32_t m_iWorkerID;
	clsConfigure *m_poConf;

	clsIOWorkerRouter *m_poIOWorkerRouter;
	clsDBReqQueue *m_poDBReqQueue;
	clsCertainWrapper *m_poCertain;

#define MAX_BATCH_CNT 50 

	struct DBRoutine_t
	{
		void * pCo;
		void * pData;
		bool bHasJob;
		int iRoutineID;
		void * pMultiData[MAX_BATCH_CNT];
		int iMultiDataCnt;
		clsDBWorker * pSelf;
	};

	clsDBBase *m_poDBEngine;
	stack<DBRoutine_t*> * m_poCoWorkList;

	set<uint64_t> m_tBusyEntitySet;

	uint32_t m_iStartRoutineID;

	void RunApplyTask(clsClientCmd *poCmd, uint64_t &iLoopCnt);
	void MultiRunApplyTask(clsClientCmd ** ppClientCmd, int iCnt);

public:
	clsDBWorker(uint32_t iWorkerID, clsConfigure *poConf,
			clsDBBase *poDBEngine, uint32_t iStartRoutineID)
			: m_iWorkerID(iWorkerID),
			  m_poConf(poConf),
			  m_poDBEngine(poDBEngine),
			  m_iStartRoutineID(iStartRoutineID)
	{
		m_poIOWorkerRouter = clsIOWorkerRouter::GetInstance();
		m_poDBReqQueue = clsAsyncQueueMng::GetInstance()->GetDBReqQueue(m_iWorkerID);
		m_poCertain = clsCertainWrapper::GetInstance();

		m_poCoWorkList = new stack<DBRoutine_t*>;
	}

	virtual ~clsDBWorker()
	{
	}

	void Run();

	clsConfigure * GetConfig()
	{
		return m_poConf;
	}

	static int EnterDBReqQueue(clsClientCmd *poCmd);

	static int CoEpollTick(void * arg);
	static int DBBatch(void * arg);
	static int DBSingle(void * arg);
	static void * DBRoutine(void * arg);
	static int NotifyDBWorker(uint64_t iEntityID);
};

} // namespace Certain

#endif
