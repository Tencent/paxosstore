
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "PerfLog.h"
#include "EntryInfoMng.h"

using namespace Certain;

uint32_t g_iPrintEntityCRC32;

string TimeToString(uint64_t iTime)
{
	char acTime[64];
	time_t tTime = iTime;
	struct tm tTM = *localtime(&tTime);
	int iLen = strftime(acTime, 64, "%m-%d %H:%M:%S", &tTM);
	acTime[iLen] = '\0';

	return acTime;
}

void PrintCommitSeq(CommitSeq_t *ptSeq)
{
	printf("[%s %03lu] %u E(%lu, %lu) wb.size %u wb.crc32 %u\n",
			TimeToString(ptSeq->tHead.iTimestampMS / 1000).c_str(),
			ptSeq->tHead.iTimestampMS % 1000,
			ptSeq->tHead.cType, ptSeq->tHead.iEntityID, ptSeq->tHead.iEntry,
			ptSeq->iWriteBatchSize, ptSeq->iWriteBatchCRC32);
}

void PrintPLogSeq(PLogSeq_t *ptSeq)
{
	PackedEntryRecord_t tRecord = ptSeq->tPackedRecord;
	printf("[%s %03lu] %u E(%lu, %lu) [%u %u %u v(%lu:[%lu][%lu][%lu] %lu %u) %u]\n",
			TimeToString(ptSeq->tHead.iTimestampMS / 1000).c_str(),
			ptSeq->tHead.iTimestampMS % 1000,
			ptSeq->tHead.cType, ptSeq->tHead.iEntityID, ptSeq->tHead.iEntry,
			tRecord.iPreparedNum, tRecord.iPromisedNum, tRecord.iAcceptedNum,
			tRecord.iValueID,
			(tRecord.iValueID >> 48),
			((tRecord.iValueID >> 32) & 0xffff),
			(tRecord.iValueID & 0xffffffff),
			ptSeq->iStoredValueID, ptSeq->iValueCRC32, tRecord.bChosen);
}

void Print(PerfHead_t *ptHead)
{
	switch (ptHead->cType)
	{
	case ePerfTypeCommitSeq:
		PrintCommitSeq((CommitSeq_t *)ptHead);
		break;

	case ePerfTypePLogSeq:
		PrintPLogSeq((PLogSeq_t *)ptHead);
		break;

	default:
		assert(false);
	}
}

void GetInfoByPos(vector<PerfHead_t *> vecPerfHead,
		uint64_t iEntityID, uint64_t iEntry)
{
	for (uint32_t i = 0; i < vecPerfHead.size(); ++i)
	{
		if (vecPerfHead[i]->iEntityID == iEntityID
				&& vecPerfHead[i]->iEntry == iEntry)
		{
			Print(vecPerfHead[i]);
		}
	}
}

void GetEntryList(vector<PerfHead_t *> vecPerfHead, uint64_t iEntityID)
{
	for (uint32_t i = 0; i < vecPerfHead.size(); ++i)
	{
		if (vecPerfHead[i]->iEntityID == iEntityID)
		{
			Print(vecPerfHead[i]);
		}
	}
}

void GetInfoByCRC32(vector<PerfHead_t *> vecPerfHead, uint64_t iCRC32)
{
	for (uint32_t i = 0; i < vecPerfHead.size(); ++i)
	{
		if (vecPerfHead[i]->cType == ePerfTypeCommitSeq)
		{
			CommitSeq_t *ptSeq = (CommitSeq_t *)vecPerfHead[i];

			if (ptSeq->iWriteBatchCRC32 == iCRC32)
			{
				Print(vecPerfHead[i]);
			}
		}

		if (vecPerfHead[i]->cType == ePerfTypePLogSeq)
		{
			PLogSeq_t *ptSeq = (PLogSeq_t *)vecPerfHead[i];

			if (ptSeq->iValueCRC32 == iCRC32)
			{
				Print(vecPerfHead[i]);
			}
		}
	}
}

void GetSumOfByte(vector<PerfHead_t *> vecPerfHead)
{
	uint64_t iDBByteSum = 0;
	uint64_t iPLogByteSum = 0;

	for (uint32_t i = 0; i < vecPerfHead.size(); ++i)
	{
		if (vecPerfHead[i]->cType == ePerfTypeCommitSeq)
		{
			CommitSeq_t *ptSeq = (CommitSeq_t *)vecPerfHead[i];
			iDBByteSum += ptSeq->iWriteBatchSize;
		}

		if (vecPerfHead[i]->cType == ePerfTypePLogSeq)
		{
			PLogSeq_t *ptSeq = (PLogSeq_t *)vecPerfHead[i];
			iPLogByteSum += ptSeq->iValueSize;
		}
	}

	printf("iDBByteSum %lu iPLogByteSum %lu\n", iDBByteSum, iPLogByteSum);

	uint64_t iTotalTime = 0;
	if (vecPerfHead.size() > 0)
	{
		iTotalTime = (vecPerfHead[vecPerfHead.size() - 1]->iTimestampMS
				- vecPerfHead[0]->iTimestampMS) / 1000;
		printf("iDBByteSpeedMB %.1lf iPLogByteSpeedMB %.1lf\n",
				iDBByteSum * 1.0 / iTotalTime / (1 << 20),
				iPLogByteSum * 1.0 / iTotalTime / (1 << 20));
	}
}

void DoCheck_0(vector<PerfHead_t *> vecPerfHead)
{
	map<PLogPos_t, uint32_t> tCommitMap;
	map<PLogPos_t, uint32_t> tChosenMap;
	map<PLogPos_t, uint32_t> tCountMap;
	for (vector<PerfHead_t *>::iterator iter = vecPerfHead.begin();
			iter != vecPerfHead.end(); ++iter)
	{
		PLogPos_t tPos;
		tPos.iEntityID = (*iter)->iEntityID;
		tPos.iEntry = (*iter)->iEntry;

		if ((*iter)->cType == ePerfTypePLogSeq)
		{
			PLogSeq_t *ptSeq = (PLogSeq_t *)(*iter);
			if (ptSeq->tPackedRecord.bHasValue)
			{
				CertainLogFatal("E(%lu, %lu) has value",
						tPos.iEntityID, tPos.iEntry);
				assert(false);
			}

			if (!ptSeq->tPackedRecord.bChosen)
			{
				continue;
			}

			if (tChosenMap.find(tPos) != tChosenMap.end())
			{
				CertainLogFatal("Duplicate Chosen E(%lu, %lu)",
						tPos.iEntityID, tPos.iEntry);
				assert(false);
			}

			tChosenMap[tPos] = ptSeq->iValueCRC32;
			tCountMap[tPos]++;
			continue;
		}

		if ((*iter)->cType == ePerfTypeCommitSeq)
		{
			if (tCommitMap.find(tPos) != tCommitMap.end())
			{
				CertainLogFatal("Duplicate Commit E(%lu, %lu)",
						tPos.iEntityID, tPos.iEntry);
				assert(false);
			}

			CommitSeq_t *ptSeq = (CommitSeq_t *)(*iter);

			tCommitMap[tPos] = ptSeq->iWriteBatchCRC32;
			tCountMap[tPos]++;
			continue;
		}
	}

	CertainLogInfo("tCountMap.size %lu", tCountMap.size());
	for (map<PLogPos_t, uint32_t>::iterator iter = tCountMap.begin();
			iter != tCountMap.end(); ++iter)
	{
		if (iter->second != 2)
		{
			CertainLogFatal("iter->second(%u) != 2 E(%lu, %lu)",
					iter->second, iter->first.iEntityID, iter->first.iEntry);
			assert(false);
		}
	}

	map<PLogPos_t, uint32_t>::iterator iterChosen;
	map<PLogPos_t, uint32_t>::iterator iterCommit;

	for (iterChosen = tChosenMap.begin(), iterCommit = tCommitMap.begin(); ;
			++iterChosen, ++iterCommit)
	{
		PLogPos_t tPos;
		int iNotEndCount = 0;

		if (iterChosen != tChosenMap.end())
		{
			tPos = iterChosen->first;
			iNotEndCount++;
		}

		if (iterCommit != tCommitMap.end())
		{
			tPos = iterCommit->first;
			iNotEndCount++;
		}

		if (iNotEndCount == 0)
		{
			break;
		}

		if (iNotEndCount == 1)
		{
			CertainLogFatal("iter error E(%lu, %lu)",
					tPos.iEntityID, tPos.iEntry);
			assert(false);
		}

		if (iterChosen->first != iterCommit->first)
		{
			CertainLogFatal("iter->fist error E(%lu, %lu) != E(%lu, %lu)",
					iterChosen->first.iEntityID, iterChosen->first.iEntry,
					iterCommit->first.iEntityID, iterCommit->first.iEntry);
			assert(false);
		}

		if (iterChosen->second != iterCommit->second)
		{
			CertainLogFatal("CRC32 error E(%lu, %lu)",
					tPos.iEntityID, tPos.iEntry);
			assert(false);
		}
	}
}

void Check_1_Cont(uint64_t iEntityID, set<uint64_t> tEntrySet)
{
	assert(tEntrySet.size() > 0);

	uint64_t iPrevEntry = 0;
	for (set<uint64_t>::iterator iter = tEntrySet.begin();
			iter != tEntrySet.end(); ++iter)
	{
		uint64_t iCurrEntry = *iter;
		if (iCurrEntry != iPrevEntry + 1)
		{
			CertainLogFatal("Not Cont E(%lu, %lu) iCurrEntry %lu",
					iEntityID, iPrevEntry, iCurrEntry);
			assert(false);
		}
		iPrevEntry = iCurrEntry;
	}
}

void DoCheck_1(vector<PerfHead_t *> vecPerfHead)
{
	map< uint64_t, set<uint64_t> > tEntityMap;
	for (uint32_t i = 0; i < vecPerfHead.size(); ++i)
	{
		tEntityMap[vecPerfHead[i]->iEntityID].insert(vecPerfHead[i]->iEntry);
	}

	uint32_t iMin = uint32_t(-1), iMax = 0;
	uint64_t iMinEntity = 0, iMaxEntity = 0;

	for (map< uint64_t, set<uint64_t> >::iterator iter = tEntityMap.begin();
			iter != tEntityMap.end(); ++iter)
	{
		set<uint64_t> tEntrySet = iter->second;
		Check_1_Cont(iter->first, tEntrySet);

		if (iMin > tEntrySet.size())
		{
			iMin = tEntrySet.size();
			iMinEntity = iter->first;
		}

		if (iMax < tEntrySet.size())
		{
			iMax = tEntrySet.size();
			iMaxEntity = iter->first;
		}
	}

	CertainLogInfo("entity_num %lu min_entity %lu count %u max_entity %lu count %u",
			tEntityMap.size(), iMinEntity, iMin, iMaxEntity, iMax);
}

void DoCheck(vector<PerfHead_t *> vecPerfHead)
{
	DoCheck_0(vecPerfHead);
	DoCheck_1(vecPerfHead);
}

void DoStat_0(vector<PerfHead_t *> vecPerfHead)
{
	PerfHead_t *ptFirst = NULL;
	PerfHead_t *ptLast = NULL;

	map<PLogPos_t, uint32_t> tPosMap;
	for (vector<PerfHead_t *>::iterator iter = vecPerfHead.begin();
			iter != vecPerfHead.end(); ++iter)
	{
		if (ptFirst == NULL)
		{
			ptFirst = *iter;
		}
		ptLast = *iter;

		PLogPos_t tPos;
		tPos.iEntityID = (*iter)->iEntityID;
		tPos.iEntry = (*iter)->iEntry;
		tPosMap[tPos]++;
	}

	if (ptFirst != NULL)
	{
		printf("First record:\n");
		Print(ptFirst);
		printf("Last record:\n");
		Print(ptLast);
	}

	map<uint32_t, uint32_t> tCountMap;
	map<uint32_t, PLogPos_t> tCountPosMap;
	for (map<PLogPos_t, uint32_t>::iterator iter = tPosMap.begin();
			iter != tPosMap.end(); ++iter)
	{
		tCountMap[iter->second]++;
		if (tCountMap[iter->second] == 1)
		{
			tCountPosMap[iter->second] = iter->first;
		}
	}

	for (map<uint32_t, uint32_t>::iterator iter = tCountMap.begin();
			iter != tCountMap.end(); ++iter)
	{
		PLogPos_t tPos = tCountPosMap[iter->first];
		CertainLogInfo("times %u count %u example E(%lu, %lu)",
				iter->first, iter->second, tPos.iEntityID, tPos.iEntry);
	}
}

void DoStat(vector<PerfHead_t *> vecPerfHead)
{
	DoStat_0(vecPerfHead);
}

void GetLastNRecord(vector<PerfHead_t *> vecPerfHead, uint32_t iNum)
{
	uint32_t i = 0;
	for (auto iter = vecPerfHead.rbegin(); iter != vecPerfHead.rend(); iter++)
	{
		Print(*iter);
		i++;
		if (i >= iNum)
		{
			break;
		}
	}
}

void DoDistributeStat(vector<PerfHead_t *> vecPerfHead)
{
	uint64_t iEntryCount = 0;
	const char *pcData = NULL;
	uint32_t iLen = 0;

	map< uint64_t, map<uint64_t, uint32_t> > tEntityMap;
	for (vector<PerfHead_t *>::iterator iter = vecPerfHead.begin();
			iter != vecPerfHead.end(); ++iter)
	{
		if ((*iter)->cType != ePerfTypeCommitSeq)
		{
			continue;
		}
		CommitSeq_t *ptSeq = (CommitSeq_t *)(*iter);

		uint64_t iEntityID = (*iter)->iEntityID;
		uint64_t iEntry = (*iter)->iEntry;

		tEntityMap[iEntityID][iEntry] = ptSeq->iWriteBatchCRC32;
	}

	uint32_t iFinalCRC32 = 0;
	map< uint64_t, map<uint64_t, uint32_t> >::iterator iter;
	for (iter = tEntityMap.begin(); iter != tEntityMap.end(); ++iter)
	{
		uint32_t iCurrCRC32 = 0;
		uint64_t iPrevEntry = 0;
		uint64_t iEntityID = iter->first;

		map<uint64_t, uint32_t> &tEntryMap = iter->second;

		for (map<uint64_t, uint32_t>::iterator iterEntry = tEntryMap.begin();
				iterEntry != tEntryMap.end(); ++iterEntry)
		{
			iEntryCount++;

			uint64_t iCurrEntry = iterEntry->first;

			if (iCurrEntry != iPrevEntry + 1)
			{
				CertainLogFatal("Not Cont E(%lu, %lu) iCurrEntry %lu",
						iEntityID, iPrevEntry, iCurrEntry);
				assert(false);
			}
			iPrevEntry = iCurrEntry;

			pcData = (const char *)&iterEntry->second;
			iLen = sizeof(iterEntry->second);

			iCurrCRC32 = ExtendCRC32(iCurrCRC32, pcData, iLen);
		}

		if (g_iPrintEntityCRC32)
		{
			printf("iEntityID %lu max_entry %lu iCurrCRC32 %u\n",
					iEntityID, iPrevEntry, iCurrCRC32);
		}

		pcData = (const char *)&iCurrCRC32;
		iLen = sizeof(iCurrCRC32);
		iFinalCRC32 = ExtendCRC32(iFinalCRC32, pcData, iLen);
	}

	CertainLogInfo("tEntityMap.size %lu iEntryCount %lu iFinalCRC32 %u",
			tEntityMap.size(), iEntryCount, iFinalCRC32);
}

int ReadData(const char *pcFilePath, string &strData)
{
	int iFD = open(pcFilePath, O_RDONLY);
	AssertLess(0, iFD);

	strData.clear();
	uint32_t iLen = 16 * 1024;
	char *pcBuffer = (char *)malloc(iLen);
	assert(pcBuffer != NULL);

	while (1)
	{
		int iRet = read(iFD, pcBuffer, iLen);
		if (iRet == 0)
		{
			break;
		}
		AssertLess(0, iRet);
		strData.append(pcBuffer, iRet);
	}

	close(iFD);

	return 0;
}

void Usage(const char *pcProc)
{
	printf("%s perf_log_path GetByPos iEntityID iEntry\n", pcProc);
	printf("%s perf_log_path GetByCRC32 iCRC32\n", pcProc);
	assert(false);
}

int main(int argc, char *argv[])
{
	if (argc < 2)
	{
		Usage(argv[0]);
	}

	OpenLog("", 5, 1, 1);

	string strData;
	AssertEqual(ReadData(argv[1], strData), 0);

	vector<PerfHead_t *> vecPerfHead;
	AssertEqual(clsPerfLog::ParseData(strData.c_str(), strData.size(),
				vecPerfHead), 0);

	if (argc == 2)
	{
		DoStat(vecPerfHead);
		DoCheck(vecPerfHead);
		DoDistributeStat(vecPerfHead);
		return 0;
	}

	const char *pcFunc = argv[2];

	if (strcmp(pcFunc, "GetEntryList") == 0)
	{
		uint64_t iEntityID;
		iEntityID = strtoull(argv[3], NULL, 10);

		GetEntryList(vecPerfHead, iEntityID);
	}
	else if (strcmp(pcFunc, "GetInfoByPos") == 0)
	{
		uint64_t iEntityID, iEntry;
		iEntityID = strtoull(argv[3], NULL, 10);
		iEntry = strtoull(argv[4], NULL, 10);

		GetInfoByPos(vecPerfHead, iEntityID, iEntry);
	}
	else if (strcmp(pcFunc, "GetInfoByCRC32") == 0)
	{
		uint64_t iCRC32 = strtoull(argv[3], NULL, 10);
		GetInfoByCRC32(vecPerfHead, iCRC32);
	}
	else if (strcmp(pcFunc, "GetSumOfByte") == 0)
	{
		GetSumOfByte(vecPerfHead);
	}
	else if (strcmp(pcFunc, "GetLastNRecord") == 0)
	{
		uint32_t iNum = strtoull(argv[3], NULL, 10);
		GetLastNRecord(vecPerfHead, iNum);
	}
	else
	{
		Usage(argv[0]);
	}

	return 0;
}
