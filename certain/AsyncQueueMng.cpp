
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "AsyncQueueMng.h"

namespace Certain
{

template<typename QueueType_t>
string PrintStat(const char *pcTag, QueueType_t **ppQueue,
		uint32_t iQueueNum, uint32_t iQueueSize)
{
	uint32_t iSize, iTotalEltNum = 0;
	char pcBuffer[128];
	string strInfo;

	for (uint32_t i = 0; i < iQueueNum; ++i)
	{
		iSize = ppQueue[i]->Size();
		iTotalEltNum += iSize;
		snprintf(pcBuffer, 128, " q[%u] %u", i, iSize);
		strInfo += pcBuffer;
	}

	CertainLogImpt("%s iQueueSize %u iQueueNum %u iTotalEltNum %u %s",
			pcTag, iQueueSize, iQueueNum, iTotalEltNum, strInfo.c_str());

	return strInfo;
}

void clsAsyncQueueMng::PrintAllStat()
{
	PrintStat("io_rsp", m_ppIORspQueue, m_iIOWorkerNum, m_iIOQueueSize);

	PrintStat("io_req", m_ppIOReqQueue, m_iEntityWorkerNum, m_iIOQueueSize);
	PrintStat("plog_rsp", m_ppPLogRspQueue, m_iEntityWorkerNum, m_iPLogQueueSize);
	PrintStat("get_all_rsp", m_ppGetAllRspQueue, m_iEntityWorkerNum, m_iGetAllQueueSize);

	PrintStat("plog_req", m_ppPLogReqQueue, m_iPLogWorkerNum, m_iPLogQueueSize);

	PrintStat("db_req", m_ppDBReqQueue, m_iDBWorkerNum, m_iDBQueueSize);

	PrintStat("get_all_req", m_ppGetAllReqQueue, m_iGetAllWorkerNum, m_iGetAllQueueSize);

	PrintStat("catchup_req", m_ppCatchUpReqQueue, 1, m_iCatchUpQueueSize);

	PrintStat("plog_write_req", m_ppPLogWriteReqQueue,  m_iPLogWriteWorkerNum, m_iPLogQueueSize);
}

} // namespace Certain


