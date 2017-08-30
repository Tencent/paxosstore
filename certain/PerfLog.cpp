#include "PerfLog.h"

namespace Certain
{

int clsPerfLog::Init(const char *pcPath)
{
	m_iUsePerfLog = 1;

	MakeCertainDir(pcPath);
	string strFile = string(pcPath) + "/perf.log";

	uint64_t iSyncSize = (1ULL << 20); // 1MB
	uint64_t iBufferSize = (1ULL << 21); // 2MB
	m_poAOF = new clsAppendOnlyFile(strFile.c_str(), iSyncSize, iBufferSize);

	return 0;
}

void clsPerfLog::Destroy()
{
	delete m_poAOF, m_poAOF = NULL;
}

int clsPerfLog::PutCommitSeq(uint64_t iEntityID, uint64_t iEntry,
		uint32_t iWriteBatchSize, uint32_t iWriteBatchCRC32)
{
	if (!m_iUsePerfLog)
	{
		return 0;
	}

	CommitSeq_t tCommitSeq;
	PerfHead_t *ptHead = (PerfHead_t *)&tCommitSeq;

	ptHead->cType = ePerfTypeCommitSeq;
	ptHead->iCheckSum = 0;
	ptHead->iTimestampMS = GetCurrTimeMS();
	ptHead->iEntityID = iEntityID;
	ptHead->iEntry = iEntry;

	tCommitSeq.iWriteBatchSize = iWriteBatchSize;
	tCommitSeq.iWriteBatchCRC32 = iWriteBatchCRC32;
	tCommitSeq.cEndFlag = PERFLOG_END_FLAG;

	ptHead->iCheckSum = CRC32(ptHead->pcCheckSumData,
			sizeof(tCommitSeq) - PERFLOG_UNCHECK_SIZE);

	return m_poAOF->Write((const char *)&tCommitSeq, sizeof(tCommitSeq));
}

int clsPerfLog::PutPLogSeq(uint64_t iEntityID, uint64_t iEntry,
		const EntryRecord_t& tRecord)
{
	if (!m_iUsePerfLog)
	{
		return 0;
	}

	PLogSeq_t tPLogSeq;
	PerfHead_t *ptHead = (PerfHead_t *)&tPLogSeq;

	ptHead->cType = ePerfTypePLogSeq;
	ptHead->iCheckSum = 0;
	ptHead->iTimestampMS = GetCurrTimeMS();
	ptHead->iEntityID = iEntityID;
	ptHead->iEntry = iEntry;

	tPLogSeq.iValueSize = tRecord.tValue.strValue.size();
	tPLogSeq.iValueCRC32 = CRC32(tRecord.tValue.strValue);
	tPLogSeq.iStoredValueID = tRecord.iStoredValueID;

	string strRecord;
	EntryRecord_t tTempRecord = tRecord;
	tTempRecord.iStoredValueID = tTempRecord.tValue.iValueID;
	EntryRecordToString(tTempRecord, strRecord);
	AssertEqual(strRecord.size(), sizeof(PackedEntryRecord_t));
	tPLogSeq.tPackedRecord = *(
			const PackedEntryRecord_t *)strRecord.c_str();
	tPLogSeq.cEndFlag = PERFLOG_END_FLAG;

	ptHead->iCheckSum = CRC32(ptHead->pcCheckSumData,
			sizeof(tPLogSeq) - PERFLOG_UNCHECK_SIZE);

	return m_poAOF->Write((const char *)&tPLogSeq, sizeof(tPLogSeq));
}

int clsPerfLog::ParseData(const char *pcData, size_t iSize,
		vector<PerfHead_t *> &vecPerfHead)
{
	uint64_t iCurr = 0, iErrorSkipped = 0;

	while (iCurr < iSize)
	{
		if (iCurr + sizeof(PerfHead_t) > iSize)
		{
			PERF_LOG_ERROR_SKIP(iSize - iCurr);
			break;
		}
		PerfHead_t *ptHead = (PerfHead_t *)(pcData + iCurr);

		uint64_t iRealSize = 0;
		if (ptHead->cType == ePerfTypeCommitSeq)
		{
			iRealSize = sizeof(CommitSeq_t);
		}
		else if (ptHead->cType == ePerfTypePLogSeq)
		{
			iRealSize = sizeof(PLogSeq_t);
		}
		else
		{
			Assert(false);
		}

		if (iCurr + iRealSize > iSize)
		{
			PERF_LOG_ERROR_SKIP(iSize - iCurr);
		}

		uint32_t iCheckSum = CRC32(ptHead->pcCheckSumData,
				iRealSize - PERFLOG_UNCHECK_SIZE);
		if (ptHead->iCheckSum != iCheckSum)
		{
			CertainLogFatal("iCurr %lu ptHead->iCheckSum %u != iCheckSum %u",
					iCurr, ptHead->iCheckSum, iCheckSum);
		}

		vecPerfHead.push_back(ptHead);
		PERF_LOG_SKIP(iRealSize);
	}

	if (iErrorSkipped > 0)
	{
		CertainLogFatal("num %lu iSize %lu iErrorSkipped %lu",
				vecPerfHead.size(), iSize, iErrorSkipped);
	}
	else
	{
		CertainLogInfo("num %lu iSize %lu iErrorSkipped %lu",
				vecPerfHead.size(), iSize, iErrorSkipped);
	}

	return 0;
}

void clsPerfLog::Flush(bool bAsync)
{
	if (!m_iUsePerfLog)
	{
		return;
	}

	m_poAOF->Flush(bAsync);
}

} // namespace Certain
