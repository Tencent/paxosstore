#include "PLogWorker.h"
#include "EntityWorker.h"

#include "co_routine.h"
#include <sys/time.h>

namespace Certain
{
	static clsUseTimeStat *s_poPutTimeStat;
	static clsUseTimeStat *s_poGetTimeStat;
	static clsUseTimeStat *s_poLoadMaxCommitedEntryTimeStat;
	static clsUseTimeStat *s_poLoadUncommitedEntrysTimeStat;
	static clsUseTimeStat *s_poPLogCmdOuterTimeStat;
	static clsUseTimeStat *s_poEpollStart2End;
	static clsUseTimeStat *s_poEpollEnd2Start;
	static clsUseTimeStat *s_poPLogReqQueueWait;

	static clsUseTimeStat *s_poCoEpollTick;
	static clsUseTimeStat *s_poGetFromIdleCoListCnt;
	static clsUseTimeStat *s_poHandleLockCallBack;
} // namespace Certain

struct EpollRunStat_t
{
	long long start_us;
	long long end_us;
	long long used_us;
	long long total_us;
	long long max_us;
	long cnt;
	long event_cnt;
};
static __thread EpollRunStat_t * s_epoll_stat = 0;
/*
static long long us()
{
	struct timeval n;
	gettimeofday(&n,0);
	long long ret = n.tv_sec;
	ret *= 1000 * 1000;
	ret += n.tv_usec;
	return ret;
}
static void OnEpollStart(int ret)
{
	if( !s_epoll_stat ) return;

	s_epoll_stat->start_us = us();
	s_epoll_stat->cnt++;
	s_epoll_stat->event_cnt += ret;

	if (s_epoll_stat->end_us > 0)
	{
		Certain::s_poEpollEnd2Start->Update(s_epoll_stat->start_us - s_epoll_stat->end_us);
	}
}
static void OnEpollEnd()
{
	if( !s_epoll_stat ) return;

	s_epoll_stat->end_us = us();

	if (s_epoll_stat->start_us > 0)
	{
		Certain::s_poEpollStart2End->Update(s_epoll_stat->end_us - s_epoll_stat->start_us);
	}
}
*/

namespace Certain
{

void clsPLogBase::InitUseTimeStat()
{
	s_poPutTimeStat = new clsUseTimeStat("Put");
	s_poGetTimeStat = new clsUseTimeStat("Get");
	s_poLoadMaxCommitedEntryTimeStat = new clsUseTimeStat("LoadMaxCommitedEntry");
	s_poLoadUncommitedEntrysTimeStat = new clsUseTimeStat("LoadUncommitedEntrys");
	s_poPLogCmdOuterTimeStat = new clsUseTimeStat("PLogCmdOuter");
	s_poEpollStart2End = new clsUseTimeStat("EpollStart2End");
	s_poEpollEnd2Start = new clsUseTimeStat("EpollEnd2Start");
	s_poPLogReqQueueWait = new clsUseTimeStat("PLogReqQueueWait");
    s_poCoEpollTick = new clsUseTimeStat("CoEpollTick");
    s_poGetFromIdleCoListCnt = new clsUseTimeStat("GetFromIdleCoListCnt");
    s_poHandleLockCallBack = new clsUseTimeStat("HandleLockCallBack");
}

void clsPLogBase::PrintUseTimeStat()
{
    if (clsCertainWrapper::GetInstance()->GetConf()->GetEnableTimeStat() == 0)
    {
        return;
    }

	s_poPutTimeStat->Print();
	s_poGetTimeStat->Print();
	s_poLoadMaxCommitedEntryTimeStat->Print();
	s_poLoadUncommitedEntrysTimeStat->Print();
	s_poPLogCmdOuterTimeStat->Print();
	s_poEpollStart2End->Print();
	s_poEpollEnd2Start->Print();
	s_poPLogReqQueueWait->Print();
    s_poCoEpollTick->Print();
    s_poGetFromIdleCoListCnt->Print();
    s_poHandleLockCallBack->Print();
}

int clsPLogBase::GetRecord(uint64_t iEntityID, uint64_t iEntry,
		EntryRecord_t &tSrcRecord, const EntryRecord_t *ptDestRecord)
{
#if CERTAIN_DEBUG
	RETURN_RANDOM_ERROR_WHEN_IN_DEBUG_MODE();
#endif

	int iRet;
	string strTemp;

	TIMERUS_START(iGetUseTimeUS);
	iRet = Get(iEntityID, iEntry, strTemp);
	TIMERUS_STOP(iGetUseTimeUS);
	s_poGetTimeStat->Update(iGetUseTimeUS);
	OSS::ReportPLogGetTimeMS(0, iGetUseTimeUS / 1000);

	if (iGetUseTimeUS > 100000)
	{
		CertainLogError("E(%lu, %lu) iGetUseTimeUS %lu",
				iEntityID, iEntry, iGetUseTimeUS);
	}

	if (iRet != 0)
	{
		if (iRet == eRetCodeNotFound)
		{
			InitEntryRecord(&tSrcRecord);
			return eRetCodeNotFound;
		}

		CertainLogFatal("BUG probably E(%lu, %lu) Get ret %d",
				iEntityID, iEntry, iRet);

		return -1;
	}

	iRet = StringToEntryRecord(strTemp, tSrcRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) StringToEntryRecord ret %d",
				iEntityID, iEntry, iRet);
		return -2;
	}

	iRet = CheckEntryRecordMayWithVIDOnly(tSrcRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) CheckEntryRecordMayWithVIDOnly ret %d",
				iEntityID, iEntry, iRet);
		return -3;
	}

	uint64_t iValueID = tSrcRecord.tValue.iValueID;
	if (iValueID > 0)
	{
		if (!tSrcRecord.tValue.bHasValue)
		{
			if (ptDestRecord != NULL
					&& ptDestRecord->tValue.iValueID == iValueID)
			{
				tSrcRecord.tValue.bHasValue = true;
#if CERTAIN_DEBUG
				if (ptDestRecord->tValue.bHasValue)
				{
					tSrcRecord.tValue.strValue = ptDestRecord->tValue.strValue;
					iRet = GetValue(iEntityID, iEntry, iValueID, strTemp);
					if (iRet != 0)
					{
						CertainLogFatal("E(%lu, %lu) GetValue ret %d",
								iEntityID, iEntry, iRet);
					}

					if (strTemp != tSrcRecord.tValue.strValue)
					{
						CertainLogFatal("E(%lu, %lu) size(%lu, %lu) CRC32(%u, %u)",
								iEntityID, iEntry,
								strTemp.size(), tSrcRecord.tValue.strValue.size(),
								CRC32(strTemp), CRC32(tSrcRecord.tValue.strValue));
					}
				}
#endif
			}
			else
			{
				TIMERUS_START(iGetValueUseTimeUS);
				iRet = GetValue(iEntityID, iEntry, iValueID, strTemp);
				TIMERUS_STOP(iGetValueUseTimeUS);
				s_poGetTimeStat->Update(iGetValueUseTimeUS);
				OSS::ReportPLogGetValueTimeMS(iRet, iGetValueUseTimeUS / 1000);

				if (iGetValueUseTimeUS > 100000)
				{
					CertainLogError("E(%lu, %lu) iGetValueUseTimeUS %lu",
							iEntityID, iEntry, iGetValueUseTimeUS);
				}

				if (iRet == eRetCodeNotFound)
				{
					return eRetCodeNotFound;
				}

				if (iRet != 0)
				{
					CertainLogFatal("E(%lu, %lu) GetValue ret %d",
							iEntityID, iEntry, iRet);
					return -4;
				}

				tSrcRecord.tValue.strValue = strTemp;
				tSrcRecord.tValue.bHasValue = true;
			}
		}
	}

	iRet = CheckEntryRecord(tSrcRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) CheckEntryRecord ret %d",
				iEntityID, iEntry, iRet);
		return -5;
	}

	if (ptDestRecord != NULL && ptDestRecord->tValue.iValueID == iValueID)
	{
		tSrcRecord.tValue.bHasValue = false;
	}

	return 0;
}

// 1. if chosen, no need to store another key-value.
// 2. if not, and the value is not stored before,
//    store it by it's iValueID first.
int clsPLogBase::PutRecord(uint64_t iEntityID, uint64_t iEntry,
		uint64_t iMaxPLogEntry, EntryRecord_t tRecord)
{
#if CERTAIN_DEBUG
	RETURN_RANDOM_ERROR_WHEN_IN_DEBUG_MODE();
#endif

	int iRet;
	string strRecord;

	CertainLogDebug("E(%lu, %lu) iMaxPLogEntry %lu record: %s",
			iEntityID, iEntry, iMaxPLogEntry, EntryRecordToString(tRecord).c_str());

	iRet = CheckEntryRecord(tRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) CheckEntryRecord ret %d",
				iEntityID, iEntry, iRet);
		return -1;
	}

	clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();
	if (!tRecord.bChosen && tRecord.tValue.strValue.size() > poConf->GetMaxEmbedValueSize())
	{
		if (tRecord.iStoredValueID != tRecord.tValue.iValueID
				&& tRecord.tValue.iValueID > 0)
		{
			TIMERUS_START(iPutValueUseTimeUS);
			iRet = PutValue(iEntityID, iEntry, tRecord.tValue.iValueID,
					tRecord.tValue.strValue);
			CertainLogDebug("E(%lu, %lu) iValueID %lu size %lu ret %d",
					iEntityID, iEntry, tRecord.tValue.iValueID,
					tRecord.tValue.strValue.size(), iRet);
			TIMERUS_STOP(iPutValueUseTimeUS);
			s_poPutTimeStat->Update(iPutValueUseTimeUS);
			OSS::ReportPLogPutTimeMS(iRet, iPutValueUseTimeUS / 1000);

			if (iPutValueUseTimeUS > 100000)
			{
				CertainLogError("E(%lu, %lu) iPutValueUseTimeUS %lu",
						iEntityID, iEntry, iPutValueUseTimeUS);
			}

			if (iRet != 0)
			{
				CertainLogFatal("E(%lu, %lu) PutValue ret %d",
						iEntityID, iEntry, iRet);
				return -2;
			}

#if 0
			CertainLogZero("E(%lu, %lu) record: %s",
					iEntityID, iEntry, EntryRecordToString(tRecord).c_str());
#endif
			tRecord.iStoredValueID = tRecord.tValue.iValueID;
		}
	}

	if (tRecord.tValue.strValue.size() <= poConf->GetMaxEmbedValueSize())
	{
		tRecord.iStoredValueID = 0;
	}

	iRet = EntryRecordToString(tRecord, strRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) EntryRecordToString ret %d",
				iEntityID, iEntry, iRet);
		return -3;
	}

	if (iMaxPLogEntry == INVALID_ENTRY || iEntry <= iMaxPLogEntry)
	{
		TIMERUS_START(iPutUseTimeUS);
		iRet = Put(iEntityID, iEntry, strRecord);
		TIMERUS_STOP(iPutUseTimeUS);
		s_poPutTimeStat->Update(iPutUseTimeUS);
		OSS::ReportPLogPutTimeMS(iRet, iPutUseTimeUS / 1000);
	}
	else
	{
		clsAutoPLogEntityLock oPLogEntityLock(iEntityID);

		PLogEntityMeta_t tMeta = { 0 };
		TIMERUS_START(iGetPLogMetaUseTimeUS);
		iRet = GetPLogEntityMeta(iEntityID, tMeta);
		TIMERUS_STOP(iGetPLogMetaUseTimeUS);
		OSS::ReportPLogGetMetaKeyTimeMS(iRet, iGetPLogMetaUseTimeUS / 1000);

		if (iRet != 0 && iRet != Certain::eRetCodeNotFound)
		{
			CertainLogFatal("E(%lu, %lu) GetPLogEntityMeta ret %d",
					iEntityID, iEntry, iRet);
			return -5;
		}

		CertainLogInfo("E(%lu, %lu) iMaxPLogEntry %lu tMeta.iMaxPLogEntry %lu",
				iEntityID, iEntry, iMaxPLogEntry, tMeta.iMaxPLogEntry);

		TIMERUS_START(iPutUseTimeUS);
		if (tMeta.iMaxPLogEntry < iEntry)
		{
			tMeta.iMaxPLogEntry = iEntry;
			iRet = PutWithPLogEntityMeta(iEntityID, iEntry, tMeta, strRecord);
		}
		else
		{
			// to unlock
			iRet = Put(iEntityID, iEntry, strRecord);
		}
		TIMERUS_STOP(iPutUseTimeUS);
		s_poPutTimeStat->Update(iPutUseTimeUS);
		OSS::ReportPLogPutTimeMS(iRet, iPutUseTimeUS / 1000);
	}

	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) Put ret %d", iEntityID, iEntry, iRet);
		return -4;
	}

#if 0
	CertainLogZero("E(%lu, %lu) record: %s",
			iEntityID, iEntry, EntryRecordToString(tRecord).c_str());
#endif

	return 0;
}

int clsPLogBase::PutRecord(uint64_t iEntityID, uint64_t iEntry,
		EntryRecord_t tRecord, vector<PLogReq_t> &vecPLogReq)
{
	int iRet;
	string strRecord;
	PLogReq_t tPLogReq;

	iRet = CheckEntryRecord(tRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) CheckEntryRecord ret %d",
				iEntityID, iEntry, iRet);
		return -1;
	}

	if (!tRecord.bChosen)
	{
		if (tRecord.iStoredValueID != tRecord.tValue.iValueID
				&& tRecord.tValue.iValueID > 0)
		{
			tPLogReq.iEntityID = iEntityID;
			tPLogReq.iEntry = iEntry;
			tPLogReq.iValueID = tRecord.tValue.iValueID;
			tPLogReq.strData = tRecord.tValue.strValue;
			vecPLogReq.push_back(tPLogReq);

			tRecord.iStoredValueID = tRecord.tValue.iValueID;
		}
	}

	iRet = EntryRecordToString(tRecord, strRecord);
	if (iRet != 0)
	{
		CertainLogFatal("E(%lu, %lu) EntryRecordToString ret %d",
				iEntityID, iEntry, iRet);
		return -2;
	}

	tPLogReq.iEntityID = iEntityID;
	tPLogReq.iEntry = iEntry;
	tPLogReq.iValueID = 0;
	tPLogReq.strData = strRecord;
	vecPLogReq.push_back(tPLogReq);

	return 0;
}

void clsPLogWorker::SendToWriteWorker(clsPaxosCmd *poPaxosCmd)
{
    assert(false);
}

int clsPLogWorker::DoWithPLogRequest(clsPaxosCmd *poPaxosCmd)
{
	int iRet;
	uint64_t iEntityID = poPaxosCmd->GetEntityID();
	uint64_t iEntry = poPaxosCmd->GetEntry();

	if (poPaxosCmd->IsPLogReturn())
	{
		EntryRecord_t tSrcRecord;
		const EntryRecord_t& tDestRecord = poPaxosCmd->GetDestRecord();
		iRet = m_poPLogEngine->GetRecord(iEntityID, iEntry, tSrcRecord, &tDestRecord);
		if(iRet == 0)
		{
			CertainLogInfo("record: %s bChose %d", EntryRecordToString(tSrcRecord).c_str(), tSrcRecord.bChosen);
			if(tSrcRecord.bChosen)
			{
				poPaxosCmd->SetSrcRecord(tSrcRecord);
			}
			else
			{
				poPaxosCmd->SetResult(eRetCodeNotFound);
			}
		}
		else if(iRet == eRetCodeNotFound)
		{
			poPaxosCmd->SetResult(eRetCodeNotFound);
		}
		else
		{
			CertainLogFatal("BUG cmd: %s ret %d",
					poPaxosCmd->GetTextCmd().c_str(), iRet);
			return -1;
		}

		m_poIOWorkerRouter->GoAndDeleteIfFailed(poPaxosCmd);
	}
	else if (poPaxosCmd->IsCheckHasMore() || m_poConf->GetUsePLogWriteWorker() == 0)
	{
		EntryRecord_t tRecord = poPaxosCmd->GetSrcRecord();

		uint64_t iMaxPLogEntry = poPaxosCmd->GetMaxPLogEntry();
		if (m_poConf->GetEnableMaxPLogEntry() == 0)
		{
			iMaxPLogEntry = INVALID_ENTRY;
		}

		iRet = m_poPLogEngine->PutRecord(iEntityID, iEntry, iMaxPLogEntry, tRecord);
		if (iRet != 0)
		{
			CertainLogFatal("E(%lu, %lu) PutRecord ret %d",
					iEntityID, iEntry, iRet);
			poPaxosCmd->SetPLogError(true);
		}

		if (poPaxosCmd->IsCheckHasMore())
		{
			bool bHasMore = false;
			vector< pair<uint64_t, string> > vecRecord;
			TIMERUS_START(iRangeLoadUseTimeUS);
			iRet = m_poPLogEngine->LoadUncommitedEntrys(iEntityID, iEntry,
					iEntry, vecRecord, bHasMore);
			TIMERUS_STOP(iRangeLoadUseTimeUS);
			s_poLoadUncommitedEntrysTimeStat->Update(iRangeLoadUseTimeUS);
			OSS::ReportPLogRangeLoadTimeMS(iRet, iRangeLoadUseTimeUS / 1000);
			if (iRangeLoadUseTimeUS > 100000)
			{
				CertainLogError("E(%lu, %lu) more %u iRangeLoadUseTimeUS %lu",
						iEntityID, iEntry, bHasMore, iRangeLoadUseTimeUS);
			}

			if (iRet != 0)
			{
				CertainLogFatal("E(%lu, %lu) LoadUncommitedEntrys ret %d",
						iEntityID, iEntry, iRet);
				poPaxosCmd->SetPLogError(true);
			}
			else
			{
				poPaxosCmd->SetHasMore(bHasMore);
			}
		}

		clsPLogWorker::EnterPLogRspQueue(poPaxosCmd);
	}
	else
	{
		SendToWriteWorker(poPaxosCmd);
	}

	return 0;
}

int clsPLogWorker::FillRecoverCmd(clsRecoverCmd *poRecoverCmd)
{
	CertainLogDebug("cmd: %s", poRecoverCmd->GetTextCmd().c_str());

	uint64_t iEntityID = poRecoverCmd->GetEntityID();

	uint64_t iMaxCommitedEntry = 0;
	TIMERUS_START(iLoadMaxCommitedEntryUseTimeUS);
	uint32_t iFlag = 0;
	int iRet = m_poDBEngine->LoadMaxCommitedEntry(iEntityID, iMaxCommitedEntry, iFlag);
	TIMERUS_STOP(iLoadMaxCommitedEntryUseTimeUS);
	s_poLoadMaxCommitedEntryTimeStat->Update(iLoadMaxCommitedEntryUseTimeUS);
	if (iRet != 0 && iRet != eRetCodeNotFound)
	{
		CertainLogFatal("LoadMaxCommitedEntry iFlag %u cmd: %s ret %d",
				iFlag, poRecoverCmd->GetTextCmd().c_str(), iRet);
		return -1;
	}

	// For triggling B/C, which may don't recieve requests, to get all.
	if (iFlag == kDBFlagCheckGetAll)
	{
		CertainLogError("iEntityID %lu LoadMaxCommitedEntry iFlag %u", iEntityID, iFlag);
		poRecoverCmd->SetCheckGetAll(true);
		return eRetCodeGetAllPending;
	}
	else if (iFlag != 0)
	{
		CertainLogError("iEntityID %lu LoadMaxCommitedEntry iFlag %u", iEntityID, iFlag);
		return eRetCodeDBStatusErr;
	}

	uint64_t iPLogCommitedEntry = iMaxCommitedEntry;
	if (poRecoverCmd->GetMaxCommitedEntry() > iMaxCommitedEntry)
	{
		// The entrys are probably not commited yet.
		// Because the road to the PLog is more heavy.
		CertainLogInfo("Check if PLog slow iMaxCommitedEntry %lu cmd: %s",
				iMaxCommitedEntry, poRecoverCmd->GetTextCmd().c_str());

		iMaxCommitedEntry = poRecoverCmd->GetMaxCommitedEntry();
	}

	typedef vector< pair<uint64_t, EntryRecord_t > > EntryRecordList_t;
	EntryRecordList_t tEntryRecordList;

	if (m_poConf->GetEnableMaxPLogEntry() > 0 && !poRecoverCmd->IsRangeLoaded())
	{
		AssertEqual(poRecoverCmd->GetMaxPLogEntry(), INVALID_ENTRY);
		PLogEntityMeta_t tMeta = { 0 };
		TIMERUS_START(iGetPLogMetaUseTimeUS);
		iRet = m_poPLogEngine->GetPLogEntityMeta(iEntityID, tMeta);
		TIMERUS_STOP(iGetPLogMetaUseTimeUS);
		OSS::ReportPLogGetMetaKeyTimeMS(iRet, iGetPLogMetaUseTimeUS / 1000);
		if (iRet != 0 && iRet != Certain::eRetCodeNotFound)
		{
			CertainLogFatal("iEntityID %lu GetPLogEntityMeta ret %d", iEntityID, iRet);
			return -5;
		}
		else
		{
			uint64_t iMaxPLogEntry = tMeta.iMaxPLogEntry;

			// This may be caused by getall.
			if (iMaxPLogEntry < iMaxCommitedEntry)
			{
				iMaxPLogEntry = iMaxCommitedEntry;
			}

			poRecoverCmd->SetMaxPLogEntry(iMaxPLogEntry);
			poRecoverCmd->SetMaxCommitedEntry(iMaxCommitedEntry);
			poRecoverCmd->SetMaxLoadingEntry(iMaxCommitedEntry);
			poRecoverCmd->SetEntryRecordList(tEntryRecordList);
			poRecoverCmd->SetHasMore(true);

			return eRetCodeOK;
		}

		poRecoverCmd->SetMaxPLogEntry(INVALID_ENTRY);
	}

	bool bHasMore = false;
	vector< pair<uint64_t, string> > vecRecord;
	uint64_t iMaxLoadingEntry = iMaxCommitedEntry + poRecoverCmd->GetMaxNum();
	TIMERUS_START(iRangeLoadUseTimeUS);
	iRet = m_poPLogEngine->LoadUncommitedEntrys(iEntityID, iMaxCommitedEntry,
			iMaxLoadingEntry, vecRecord, bHasMore);
	TIMERUS_STOP(iRangeLoadUseTimeUS);
	s_poLoadUncommitedEntrysTimeStat->Update(iRangeLoadUseTimeUS);
	OSS::ReportPLogRangeLoadTimeMS(iRet, iRangeLoadUseTimeUS / 1000);
	if (iRangeLoadUseTimeUS > 100000)
	{
		CertainLogError("E(%lu, %lu) entrys: %lu %lu iRangeLoadUseTimeUS %lu",
				iEntityID, iMaxCommitedEntry, iPLogCommitedEntry,
				iMaxLoadingEntry, iRangeLoadUseTimeUS);
	}
	if (iRet != 0)
	{
		CertainLogFatal("LoadUncommitedEntrys E(%lu, %lu) ret %d",
				iEntityID, iMaxCommitedEntry, iRet);
		return -2;
	}

	for (uint32_t i = 0; i < vecRecord.size(); ++i)
	{
		uint64_t iEntry = vecRecord[i].first;

		EntryRecord_t tRecord;
		iRet = StringToEntryRecord(vecRecord[i].second, tRecord);
		if (iRet != 0)
		{
			CertainLogFatal("E(%lu, %lu) StringToEntryRecord ret %d",
					iEntityID, iEntry, iRet);
			return -3;
		}

		if (tRecord.tValue.iValueID > 0 && !tRecord.tValue.bHasValue)
		{
			TIMERUS_START(iGetValueUseTimeUS);
			iRet = m_poPLogEngine->GetValue(iEntityID, iEntry,
					tRecord.tValue.iValueID, tRecord.tValue.strValue);
			TIMERUS_STOP(iGetValueUseTimeUS);
			s_poGetTimeStat->Update(iGetValueUseTimeUS);
			OSS::ReportPLogGetValueTimeMS(iRet, iGetValueUseTimeUS / 1000);

			if (iGetValueUseTimeUS > 100000)
			{
				CertainLogError("E(%lu, %lu) iGetValueUseTimeUS %lu",
						iEntityID, iEntry, iGetValueUseTimeUS);
			}
			if (iRet != 0)
			{
				CertainLogFatal("GetValue ret %d E(%lu, %lu) record: %s",
						iRet, iEntityID, iEntry, EntryRecordToString(tRecord).c_str());
				return -4;
			}
			tRecord.tValue.bHasValue = true;
		}

		tEntryRecordList.push_back(
				pair<uint64_t, EntryRecord_t>(iEntry, tRecord));
	}

	poRecoverCmd->SetMaxCommitedEntry(iMaxCommitedEntry);
	poRecoverCmd->SetMaxLoadingEntry(iMaxLoadingEntry);
	poRecoverCmd->SetEntryRecordList(tEntryRecordList);
	poRecoverCmd->SetHasMore(bHasMore);

	return eRetCodeOK;
}

void clsPLogWorker::DoWithRecoverCmd(clsRecoverCmd *poRecoverCmd)
{
	int iRet = FillRecoverCmd(poRecoverCmd);
	if (iRet != 0)
	{
		CertainLogError("cmd: %s FillRecoverCmd ret %d",
				poRecoverCmd->GetTextCmd().c_str(), iRet);
		poRecoverCmd->SetResult(eRetCodeDBLoadErr);
	}
	else
	{
		CertainLogInfo("cmd: %s", poRecoverCmd->GetTextCmd().c_str());
		poRecoverCmd->SetResult(eRetCodeOK);
	}

	clsPLogWorker::EnterPLogRspQueue(poRecoverCmd);
}

int clsPLogWorker::LoadEntry(clsPaxosCmd *poPaxosCmd)
{
	int iRet;
	uint64_t iEntityID = poPaxosCmd->GetEntityID();
	uint64_t iEntry = poPaxosCmd->GetEntry();

	EntryRecord_t tSrcRecord;
	iRet = m_poPLogEngine->GetRecord(iEntityID, iEntry, tSrcRecord);

	if (iRet == 0)
	{
		poPaxosCmd->SetSrcRecord(tSrcRecord);
	}
	else if (iRet == eRetCodeNotFound)
	{
		poPaxosCmd->SetResult(eRetCodeNotFound);
	}
	else
	{
		CertainLogFatal("BUG cmd: %s ret %d",
				poPaxosCmd->GetTextCmd().c_str(), iRet);
		poPaxosCmd->SetPLogError(true);
	}

	clsPLogWorker::EnterPLogRspQueue(poPaxosCmd);

	return 0;
}

int clsPLogWorker::EnterPLogReqQueue(clsCmdBase *poCmd)
{
	uint64_t iEntityID = poCmd->GetEntityID();
	clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
	clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

	clsPLogReqQueue *poQueue = poQueueMng->GetPLogReqQueue(Hash(iEntityID)
			% poConf->GetPLogWorkerNum());

	poCmd->SetTimestampUS(GetCurrTimeUS());

	int iRet = poQueue->PushByMultiThread(poCmd);
	if (iRet != 0)
	{
		OSS::ReportPLogQueueErr();
		//CertainLogError("PushByMultiThread ret %d cmd: %s",
		//		iRet, poCmd->GetTextCmd().c_str());
		return -1;
	}

	return 0;
}

void clsPLogWorker::DoWithPaxosCmd(clsPaxosCmd *poPaxosCmd)
{
	int iRet;

	iRet = clsPerfLog::GetInstance()->PutPLogSeq(poPaxosCmd->GetEntityID(),
			poPaxosCmd->GetEntry(), poPaxosCmd->GetSrcRecord());
	if (iRet != 0)
	{
		CertainLogFatal("PerfLog failed ret %d", iRet);
		Assert(false);
	}

	CertainLogInfo("PLog_seq cmd: %s", poPaxosCmd->GetTextCmd().c_str());

	if (poPaxosCmd->IsPLogLoad())
	{
		iRet = LoadEntry(poPaxosCmd);
	}
	else
	{
		iRet = DoWithPLogRequest(poPaxosCmd);
	}

	if (iRet != 0)
	{
		CertainLogError("DoWithPLogRequest ret %d cmd: %s",
				iRet, poPaxosCmd->GetTextCmd().c_str());
		delete poPaxosCmd, poPaxosCmd = NULL;
	}
}

void *clsPLogWorker::WakeUpRoutine(void * arg)
{
    /*
	PLogRoutine_t * pPLogRoutine = (PLogRoutine_t *)arg;
	clsPLogWorker * pPLogWorker = (clsPLogWorker * )pPLogRoutine->pSelf;

	int iFD = pPLogWorker->m_iNotifyFd;

	co_add_persist_event(co_get_epoll_ct(), iFD, POLLIN);
	char buff[1024] = {0};
	for ( ;; )
	{
		int ret = read(iFD, buff, 1024);
		if (ret == -1 && errno == EAGAIN )
		{
			co_yield_ct();
			continue;
		}
		if (ret <= 0)
		{
			CertainLogFatal("read fail errno %d, no more notify", errno);
			break;
		}
	}
    */

	return NULL;
}

void *clsPLogWorker::PLogRoutine(void * arg)
{
	PLogRoutine_t * pPLogRoutine = (PLogRoutine_t *)arg;
	//co_enable_hook_sys();

	SetRoutineID(pPLogRoutine->iRoutineID);

	while(1)
	{
		clsPLogWorker * pPLogWorker = (clsPLogWorker * )pPLogRoutine->pSelf;

		if(!pPLogRoutine->bHasJob)
		{
			AssertEqual(pPLogRoutine->pData, NULL);
			pPLogWorker->m_poCoWorkList->push(pPLogRoutine);
			co_yield_ct();
			continue;
		}

		AssertNotEqual(pPLogRoutine->pData, NULL);
		clsCmdBase *poCmd = (clsCmdBase*) pPLogRoutine->pData;

		if (poCmd->GetCmdID() == kPaxosCmd)
		{
			pPLogWorker->DoWithPaxosCmd(dynamic_cast<clsPaxosCmd *>(poCmd));
		}
		else
		{
			AssertEqual(poCmd->GetCmdID(), kRecoverCmd);
			pPLogWorker->DoWithRecoverCmd(dynamic_cast<clsRecoverCmd *>(poCmd));
		}

		pPLogRoutine->bHasJob = false;
		pPLogRoutine->pData = NULL;
	}

	return NULL;
}

int clsPLogWorker::CoEpollTick(void * arg)
{
	clsPLogWorker * pPLogWorker = (clsPLogWorker*)arg;
	stack<PLogRoutine_t *> & IdleCoList = *(pPLogWorker->m_poCoWorkList);

	static __thread uint64_t iLoopCnt = 0;

    TIMERUS_START(iCoEpollTickTimeUS);
    uint64_t iGetFromIdleCoListCnt = 0;
	
	while( !IdleCoList.empty() )
	{
		clsCmdBase *poCmd = NULL;
		int iRet = pPLogWorker->m_poPLogReqQueue->TakeByOneThread(&poCmd);
		if(iRet == 0 && poCmd)
		{
			uint64_t iUseTimeUS = GetCurrTimeUS() - poCmd->GetTimestampUS();
			s_poPLogReqQueueWait->Update(iUseTimeUS);

			if( ( (++iLoopCnt) % 10000) == 0)
			{
				CertainLogImpt("PLogWorkerID %u PLogQueue size %u",
						pPLogWorker->m_iWorkerID, pPLogWorker->m_poPLogReqQueue->Size());
			}

			PLogRoutine_t * w = IdleCoList.top();
			w->pData = (void*)poCmd;
			w->bHasJob = true;
			IdleCoList.pop();
			co_resume( (stCoRoutine_t*)(w->pCo) );
            iGetFromIdleCoListCnt++;
		}
		else
		{
			break;
		}
	}
    s_poGetFromIdleCoListCnt->Update(iGetFromIdleCoListCnt);

    TIMERUS_STOP(iCoEpollTickTimeUS);
    s_poCoEpollTick->Update(iCoEpollTickTimeUS);

    TIMERUS_START(iHandleLockCallBackTimeUS);
	//clsCertainUserBase * pCertainUser = clsCertainWrapper::GetInstance()->GetCertainUser();
	//pCertainUser->HandleLockCallBack()();
    TIMERUS_STOP(iHandleLockCallBackTimeUS);
    s_poHandleLockCallBack->Update(iHandleLockCallBackTimeUS);
		if (pPLogWorker->CheckIfExiting(0))
		{
            return -1;
		}


	return 0;
}

void clsPLogWorker::Run()
{
	int cpu_cnt = GetCpuCount();

	if (cpu_cnt == 48)
	{
		SetCpu(8, cpu_cnt);
	}
	else
	{
		SetCpu(4, cpu_cnt);
	}

	uint32_t iLocalServerID = m_poConf->GetLocalServerID();
	SetThreadTitle("plog_%u_%u", iLocalServerID, m_iWorkerID);
	CertainLogInfo("plog_%u_%u run", iLocalServerID, m_iWorkerID);

	//co_enable_hook_sys();
	stCoEpoll_t * ev = co_get_epoll_ct();
	s_epoll_stat = (EpollRunStat_t*)calloc( sizeof(EpollRunStat_t),1 ); 
	//co_set_eventloop_stat( OnEpollStart,OnEpollEnd );
	for (int i = 0; i < int(m_poConf->GetPLogRoutineCnt()); ++i)
	{
		PLogRoutine_t *w = (PLogRoutine_t*)calloc( 1,sizeof(PLogRoutine_t) );
		stCoRoutine_t *co = NULL;
		co_create( &co, NULL, PLogRoutine, w );

		int iRoutineID = m_iStartRoutineID + i;
		w->pCo = (void*)co;
		w->pSelf = this;
		w->pData = NULL;
		w->bHasJob = false;
        w->iRoutineID = iRoutineID;
		co_resume( (stCoRoutine_t *)(w->pCo) );
        printf("PLogWorker idx %d Routine idx %d\n", m_iWorkerID,  iRoutineID);
        CertainLogImpt("PLogWorker idx %d Routine idx %d", m_iWorkerID,  iRoutineID);
	}
/*
	{
		PLogRoutine_t *w = (PLogRoutine_t*)calloc( 1,sizeof(PLogRoutine_t) );
		stCoRoutine_t *co = NULL;
		co_create(&co, NULL, WakeUpRoutine, w);

		w->pCo = (void*)co;
		w->pSelf = this;
		w->pData = NULL;
		w->bHasJob = false;
		w->iRoutineID = m_iStartRoutineID + m_poConf->GetPLogRoutineCnt();
		co_resume( (stCoRoutine_t *)(w->pCo) );
	}
*/
	co_eventloop( ev, CoEpollTick, this);
}

int clsPLogWorker::EnterPLogRspQueue(clsCmdBase *poCmd)
{
	uint64_t iEntityID = poCmd->GetEntityID();
	clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
	clsConfigure *poConf = clsCertainWrapper::GetInstance()->GetConf();

	clsPLogRspQueue *poQueue = poQueueMng->GetPLogRspQueue(Hash(iEntityID)
			% poConf->GetEntityWorkerNum());

	while (1)
	{
		int iRet = poQueue->PushByMultiThread(poCmd);
		if (iRet == 0)
		{
			break;
		}

		CertainLogError("PushByMultiThread ret %d cmd: %s",
				iRet, poCmd->GetTextCmd().c_str());

		poll(NULL, 0, 1);
	}

	uint64_t iUseTimeUS = GetCurrTimeUS() - poCmd->GetTimestampUS();
	s_poPLogCmdOuterTimeStat->Update(iUseTimeUS);

	return 0;
}

} // namespace Certain
