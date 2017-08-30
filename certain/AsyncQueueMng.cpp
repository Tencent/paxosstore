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

	CertainLogImpt("certain_stat %s iQueueSize %u iQueueNum %u iTotalEltNum %u %s",
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
