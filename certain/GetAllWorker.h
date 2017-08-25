
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_GETALL_WORKER_H_
#define CERTAIN_GETALL_WORKER_H_

#include "Certain.h"
#include "IOWorker.h"
#include "AsyncQueueMng.h"

namespace Certain
{

class clsGetAllWorker : public clsThreadBase
{
private:
	uint32_t m_iWorkerID;
	clsConfigure *m_poConf;
	uint32_t m_iStartRoutineID;

	struct GetAllRoutine_t
	{
		void * pCo;
		void * pData;
		bool bHasJob;
		int iRoutineID;
		clsGetAllWorker * pSelf;
	};

	std::stack<GetAllRoutine_t*> * m_poCoWorkList;
	clsAsyncQueueMng *m_poQueueMng;

	clsDBBase *m_poDBEngine;
	clsPLogBase *m_poPLogEngine;

	int DoWithReq(clsPaxosCmd *poCmd);
	inline uint32_t GetWorkerID() 
	{
		return m_iWorkerID;
   	}

public:
	clsGetAllWorker(uint32_t iWorkerID, clsConfigure *poConf, uint32_t iStartRoutineID) : m_iWorkerID(iWorkerID), m_poConf(poConf), m_iStartRoutineID(iStartRoutineID)

	{
		m_poQueueMng = clsAsyncQueueMng::GetInstance();
		m_poPLogEngine = clsCertainWrapper::GetInstance()->GetPLogEngine();
		m_poDBEngine = clsCertainWrapper::GetInstance()->GetDBEngine();

		m_poCoWorkList = new std::stack<GetAllRoutine_t*>;
	}

	virtual ~clsGetAllWorker()
	{
		delete m_poCoWorkList, m_poCoWorkList = NULL;
	}

	void Run();
	static int EnterReqQueue(clsPaxosCmd *poCmd);

private:

	static int HandleInQueue(clsPaxosCmd * poCmd);
	static int CoEpollTick(void * arg);
	static void * GetAllRoutine(void * arg);
};

} // namespace Certain

#endif
