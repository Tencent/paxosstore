
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_CATCHUPWORKER_H_
#define CERTAIN_CATCHUPWORKER_H_

#include "Certain.h"
#include "IOWorker.h"

namespace Certain
{

class clsCatchUpCtrl
{
private:
	uint32_t m_iMaxCatchUpSpeedKB;
	uint64_t m_iMaxByteSize;
	uint64_t m_iNextSendTimeMS;
	uint64_t m_iRemainByteSize;

	uint32_t m_iMaxCatchUpCnt;
	uint32_t m_iUseCount;
	uint64_t m_iNextCountTimeMS;

public:

	clsCatchUpCtrl() : m_iMaxCatchUpSpeedKB(0),
					   m_iMaxByteSize(0),
					   m_iNextSendTimeMS(0),
					   m_iRemainByteSize(0),
					   m_iMaxCatchUpCnt(0),
					   m_iUseCount(0),
					   m_iNextCountTimeMS(0) { }

	virtual ~clsCatchUpCtrl() { }

	// return 0 on Succ, if return iWaitTimeMS > 0, just wait.
	uint64_t UseByteSize(uint64_t iByteSize);
	uint64_t UseCount();

	void UpdateCatchUpSpeed(uint32_t iMaxCatchUpSpeedKB);
	void UpdateCatchUpCnt(uint32_t iMaxCatchUpCnt);
};

class clsCatchUpWorker : public clsThreadBase,
						 public clsSingleton<clsCatchUpWorker>

{
private:
	clsConfigure *m_poConf;

	uint32_t m_iLocalServerID;
	uint32_t m_iMaxCatchUpConcurr;

	uint32_t m_iAcceptorNum;
	uint32_t m_iServerNum;

	uint64_t m_iPrevPrintTimeMS;
	std::vector<uint64_t> m_iCatchUpCnt;
	std::vector<uint64_t> m_iCatchUpSize;

	clsCatchUpCtrl *m_poCatchUpCtrl;

	typedef clsCircleQueue<clsPaxosCmd *> clsCatchUpQueue;
	clsCatchUpQueue *m_poCatchUpQueue;

	clsIOWorkerRouter *m_poIOWorkerRouter;

	clsCertainUserBase *m_poCertainUser;

	friend class clsSingleton<clsCatchUpWorker>;

	clsCatchUpWorker() : m_poConf(NULL),
						 m_iLocalServerID(0),
						 m_iMaxCatchUpConcurr(0),
						 m_iAcceptorNum(0),
						 m_iServerNum(0),
						 m_iPrevPrintTimeMS(0),
						 m_poCatchUpCtrl(NULL),
						 m_poCatchUpQueue(NULL),
						 m_poIOWorkerRouter(NULL),
						 m_poCertainUser(NULL) { }

	// (TODO): use exclusive channel to make accurate speed.
	uint64_t EstimateSize(clsPaxosCmd *poCatchUpCmd);

	bool ConsumeCatchUpQueue();

	void PrintStat();
	void DoStat(uint64_t iEntityID, uint32_t iDestAcceptorID,
			uint64_t iByteSize);

public:
	int Init(clsConfigure *poConf, clsCertainUserBase *poCertainUser);

	void Destroy();

	int PushCatchUpCmdByMultiThread(clsPaxosCmd *poCatchUpCmd);

	void Run();
};

}; // namespace Certain

#endif
