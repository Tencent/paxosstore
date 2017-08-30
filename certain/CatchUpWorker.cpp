#include "CatchUpWorker.h"

namespace Certain
{

static const uint64_t kSendIntervalMS = 10;

void clsCatchUpCtrl::UpdateCatchUpSpeed(uint32_t iMaxCatchUpSpeedKB)
{
	if (m_iMaxCatchUpSpeedKB == iMaxCatchUpSpeedKB)
	{
		return;
	}

	if (iMaxCatchUpSpeedKB == 0)
	{
		return;
	}

	CertainLogImpt("Update m_iMaxCatchUpSpeedKB %u -> %u",
			m_iMaxCatchUpSpeedKB, iMaxCatchUpSpeedKB);
	m_iMaxCatchUpSpeedKB = iMaxCatchUpSpeedKB;

	uint64_t iMaxCatchUpSpeedByte = m_iMaxCatchUpSpeedKB * uint64_t(1 << 10);

	m_iMaxByteSize = iMaxCatchUpSpeedByte / (1000.0 / kSendIntervalMS);
	assert(m_iMaxByteSize > 0);

	m_iNextSendTimeMS = 0;

	m_iRemainByteSize = 0;

	CertainLogInfo("iMaxCatchUpSpeedByte %lu m_iMaxByteSize %lu",
			iMaxCatchUpSpeedByte, m_iMaxByteSize);
}

void clsCatchUpCtrl::UpdateCatchUpCnt(uint32_t iMaxCatchUpCnt)
{
	if (m_iMaxCatchUpCnt == iMaxCatchUpCnt)
	{
		return;
	}

	CertainLogImpt("Update m_iMaxCatchUpCnt %u -> %u",
			m_iMaxCatchUpCnt, iMaxCatchUpCnt);
	m_iMaxCatchUpCnt = iMaxCatchUpCnt;

	m_iUseCount = 0;
	m_iNextCountTimeMS = GetCurrTimeMS() + 1000;
}

uint64_t clsCatchUpCtrl::UseCount()
{
	if (m_iUseCount < m_iMaxCatchUpCnt)
	{
		m_iUseCount++;
		return 0;
	}

	uint64_t iCurrTimeMS = GetCurrTimeMS();
	if (iCurrTimeMS < m_iNextCountTimeMS)
	{
		return m_iNextCountTimeMS - iCurrTimeMS;
	}

	// At least catchup at 1/s.
	m_iUseCount = 1;
	m_iNextCountTimeMS = iCurrTimeMS + 1000;

	return 0;
}

uint64_t clsCatchUpCtrl::UseByteSize(uint64_t iByteSize)
{
	if (m_iRemainByteSize >= iByteSize)
	{
		m_iRemainByteSize -= iByteSize;
		return 0;
	}

	uint64_t iCurrTimeMS = GetCurrTimeMS();
	if (iCurrTimeMS < m_iNextSendTimeMS)
	{
		return m_iNextSendTimeMS - iCurrTimeMS;
	}

	if (iByteSize <= m_iMaxByteSize)
	{
		m_iRemainByteSize = m_iMaxByteSize - iByteSize;
		m_iNextSendTimeMS = iCurrTimeMS + kSendIntervalMS;
		return 0;
	}

	m_iNextSendTimeMS = iCurrTimeMS + (
			kSendIntervalMS * iByteSize) / m_iMaxByteSize;
	m_iRemainByteSize = 0;

	return 0;
}

uint64_t clsCatchUpWorker::EstimateSize(clsPaxosCmd *poCatchUpCmd)
{
	uint64_t iSize = 42; // around 42 byte for empty
	const EntryRecord_t &tSrcRecord = poCatchUpCmd->GetSrcRecord();
	const EntryRecord_t &tDestRecord = poCatchUpCmd->GetDestRecord();

	if (tSrcRecord.tValue.iValueID > 0
			&& tSrcRecord.tValue.iValueID != tDestRecord.tValue.iValueID)
	{
		iSize += tSrcRecord.tValue.strValue.size();
	}

	return iSize;
}

int clsCatchUpWorker::Init(clsConfigure *poConf,
		clsCertainUserBase *poCertainUser)
{
	m_poConf = poConf;
	m_iLocalServerID = poConf->GetLocalServerID();

	m_iMaxCatchUpConcurr = poConf->GetMaxCatchUpConcurr();

	m_iAcceptorNum = poConf->GetAcceptorNum();
	m_iServerNum = poConf->GetServerNum();

	m_iPrevPrintTimeMS = 0;

	for (uint32_t i = 0; i < m_iServerNum; ++i)
	{
		m_iCatchUpCnt.push_back(0);
		m_iCatchUpSize.push_back(0);
	}

	uint32_t iMaxCatchUpSpeedKB = poConf->GetMaxCatchUpSpeedKB();
	if (iMaxCatchUpSpeedKB == 0)
	{
		CertainLogFatal("iMaxCatchUpSpeedKB == 0");
		return -1;
	}

	m_poCatchUpCtrl = new clsCatchUpCtrl;
	m_poCatchUpCtrl->UpdateCatchUpSpeed(iMaxCatchUpSpeedKB);

	m_poCatchUpQueue = clsAsyncQueueMng::GetInstance()->GetCatchUpReqQueue(0);

	m_poIOWorkerRouter = clsIOWorkerRouter::GetInstance();

	m_poCertainUser = poCertainUser;

	return 0;
}

void clsCatchUpWorker::Destroy()
{
	delete m_poCatchUpQueue, m_poCatchUpQueue = NULL;
	delete m_poCatchUpCtrl, m_poCatchUpCtrl = NULL;
}

int clsCatchUpWorker::PushCatchUpCmdByMultiThread(clsPaxosCmd *poCatchUpCmd)
{
	AssertNotMore(poCatchUpCmd->GetEntry(), poCatchUpCmd->GetMaxChosenEntry());

	if (m_poCatchUpQueue->Size() >= m_iMaxCatchUpConcurr)
	{
		return -1;
	}

	int iRet = m_poCatchUpQueue->PushByMultiThread(poCatchUpCmd);
	if (iRet != 0)
	{
		OSS::ReportCatchUpQueueErr();
		CertainLogError("m_poCatchUpQueue->PushByMultiThread ret %d", iRet);
		return -2;
	}

	return 0;
}

void clsCatchUpWorker::PrintStat()
{
	uint64_t iCurrTimeMS = GetCurrTimeMS();
	if (iCurrTimeMS >= m_iPrevPrintTimeMS + 1000)
	{
		uint64_t iTotalCnt = 0;
		uint64_t iTotalSize = 0;

		bool bHasCatchUp = false;
		string strInfo;

		for (uint32_t i = 0; i < m_iServerNum; ++i)
		{
			char buffer[128];
			sprintf(buffer, " S%u(%lu, %lu)",
					i, m_iCatchUpCnt[i], m_iCatchUpSize[i]);
			strInfo.append(buffer);

			if (m_iCatchUpCnt[i] > 0)
			{
				bHasCatchUp = true;
			}

			iTotalCnt += m_iCatchUpCnt[i];
			iTotalSize += m_iCatchUpSize[i];

			m_iCatchUpCnt[i] = 0;
			m_iCatchUpSize[i] = 0;
		}

		if (bHasCatchUp)
		{
			CertainLogImpt("catchup_stat_for_srv %u Sid(%lu, %lu):%s",
					m_iLocalServerID, iTotalCnt, iTotalSize, strInfo.c_str());
		}
		m_iPrevPrintTimeMS = iCurrTimeMS;
	}
}

void clsCatchUpWorker::DoStat(uint64_t iEntityID,
		uint32_t iDestAcceptorID, uint64_t iByteSize)
{
	uint32_t iLocalAcceptorID = 0;
	AssertEqual(m_poCertainUser->GetLocalAcceptorID(
				iEntityID, iLocalAcceptorID), 0);

	uint32_t iDestServerID = m_iServerNum;
	if (iDestAcceptorID < m_iAcceptorNum)
	{
		AssertNotEqual(iDestAcceptorID, iLocalAcceptorID);
		AssertEqual(m_poCertainUser->GetServerID(
					iEntityID, iDestAcceptorID, iDestServerID), 0);
		AssertLess(iDestServerID, m_iServerNum);

		m_iCatchUpCnt[iDestServerID]++;
		m_iCatchUpSize[iDestServerID] += iByteSize;
	}
	else
	{
		for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
		{
			if (i == iLocalAcceptorID)
			{
				continue;
			}

			AssertEqual(m_poCertainUser->GetServerID(
						iEntityID, i, iDestServerID), 0);
			AssertLess(iDestServerID, m_iServerNum);

			m_iCatchUpCnt[iDestServerID]++;
			m_iCatchUpSize[iDestServerID] += iByteSize;
		}
	}

	PrintStat();
}

void clsCatchUpWorker::Run()
{
	int iRet;
	SetThreadTitle("catchup_%u", m_iLocalServerID);
	CertainLogInfo("catchup_%u run", m_iLocalServerID);

	clsSmartSleepCtrl oSleepCtrl(200, 1000);

	while (1)
	{
		if (CheckIfExiting(1000))
		{
			printf("catchup_%u exit\n", m_iLocalServerID);
			CertainLogInfo("catchup_%u exit", m_iLocalServerID);
			break;
		}

		m_poCatchUpCtrl->UpdateCatchUpSpeed(m_poConf->GetMaxCatchUpSpeedKB());
		m_poCatchUpCtrl->UpdateCatchUpCnt(m_poConf->GetMaxCatchUpCnt());

		clsPaxosCmd *poCmd = NULL;
		iRet = m_poCatchUpQueue->TakeByOneThread(&poCmd);
		if (iRet != 0)
		{
			oSleepCtrl.Sleep();
			PrintStat();
			continue;
		}
		else
		{
			oSleepCtrl.Reset();
		}

		uint64_t iEntityID = poCmd->GetEntityID();
		uint32_t iDestAcceptorID = poCmd->GetDestAcceptorID();
		uint64_t iByteSize = EstimateSize(poCmd);

		while (1)
		{
			uint64_t iWaitTimeMS = m_poCatchUpCtrl->UseByteSize(iByteSize);
			if (iWaitTimeMS == 0)
			{
				break;
			}

			CertainLogImpt("catchup iByteSize %lu iWaitTimeMS %lu",
					iByteSize, iWaitTimeMS);
			usleep(iWaitTimeMS * 1000);
		}

		while (1)
		{
			uint64_t iWaitTimeMS = m_poCatchUpCtrl->UseCount();
			if (iWaitTimeMS == 0)
			{
				break;
			}

			CertainLogImpt("catchup iWaitTimeMS %lu by count", iWaitTimeMS);
			usleep(iWaitTimeMS * 1000);
		}

		iRet = m_poIOWorkerRouter->Go(poCmd);
		if (iRet != 0)
		{
			CertainLogError("Go ret %d cmd: %s",
					iRet, poCmd->GetTextCmd().c_str());
			delete poCmd, poCmd = NULL;
		}
		else
		{
			DoStat(iEntityID, iDestAcceptorID, iByteSize);
		}
	}
}

}; // namespace Certain
