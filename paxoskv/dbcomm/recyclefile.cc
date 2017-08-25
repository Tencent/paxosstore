
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <errno.h>
#include <list>
#include <algorithm>
#include <limits>
#include <cstring>
#include <cstdio>
#include <cassert>
#include "cutils/log_utils.h"
#include "comm/kvsvrcomm.h"
#include "leveldb_log_impl.h"
#include "leveldb_log.h"
#include "db_comm.h"
#include "recyclefile.h"


#define MIN_URGENT_RATIO 90


namespace {

using namespace dbcomm;

const std::string MERGE_POSTFIX = "m";
const std::string WRITE_POSTFIX = "w";

enum {
	RECYCLE_MODE_NIL = 0,
	RECYCLE_MODE_ALL = 1,
	RECYCLE_MODE_PARTIAL = 2, 
};

void* RecycleFileThread(void* args)
{
	assert(NULL != args);
	FileRecycleManager* 
        poFRMng = reinterpret_cast<FileRecycleManager*>(args);

	logerr("FileRecycleManager poFRMng %p", poFRMng);
	// TODO
	int ret = poFRMng->Run();
	logerr("FileRecycleManager::Run ret %d", ret);
	return NULL;
}

int Scan(
		const std::string& sPath, 
		std::list<std::string>& lstFile)
{
	lstFile.clear();
    std::list<std::string> tExcludeFilePattern;
    std::list<std::string> tIncludeFilePattern;
	tIncludeFilePattern.push_back(".w");
	tIncludeFilePattern.push_back(".m");
	return dbcomm::ScanFiles(
			sPath, tExcludeFilePattern, tIncludeFilePattern, lstFile);
}

int SplitFileName(
		const std::string& sFileName, 
		int& iFileNo, std::string& sPostfix)
{
	if (sFileName.empty())
	{
		return -1;
	}

	char sPostFixBuf[16] = {0};
	uint32_t iParseFileNo = 0;
	int ret = sscanf(
            sFileName.c_str(), "%u.%s", &iParseFileNo, sPostFixBuf);
	if (2 != ret)
	{
		return -2;
	}

	if (0 >= static_cast<int>(iParseFileNo))
	{
		return -3;
	}

	sPostfix = std::string(
            sPostFixBuf, strnlen(sPostFixBuf, sizeof(sPostFixBuf)));
	iFileNo = static_cast<int>(iParseFileNo);
	assert(0 < iFileNo);
	return 0;
}

bool CheckDiskRatio(int iDiskRatioUpLimit, const std::string& sKvPath)
{
	int iApproximateDiskRatio = iDiskRatioUpLimit - 5;
	iApproximateDiskRatio = 
        0 > iApproximateDiskRatio ? 0 : iApproximateDiskRatio;
	if (dbcomm::DiskRatio(sKvPath.c_str(), iApproximateDiskRatio) > 0)
	{
		return true;
	}

	return false;
}

void SafeSleep(int iSleepSeconds)
{
	time_t iBeginTime = time(NULL);
	while (true)
	{
		time_t iNow = time(NULL);
		if (iNow - iBeginTime >= iSleepSeconds)
		{
			break;
		}

		// else
		int iRemainSleepTime = iSleepSeconds - (iNow - iBeginTime);
		iRemainSleepTime = std::min(iRemainSleepTime, iSleepSeconds);
		sleep(iRemainSleepTime);
	}
}

int TryToDeleteDataFile(
        const std::string& sPath, const std::string& sFileName)
{
	int iFileNo = 0;
    std::string sPostFix;
	int ret = SplitFileName(sFileName, iFileNo, sPostFix);
	if (0 != ret)
	{
		return -1;
	}

	if (size_t(1) != sPostFix.size())
	{
		return -2;
	}

	if ('w' != sPostFix[0] && 'm' != sPostFix[0])
	{
		return -3;
	}

    std::string sAbsFileName = dbcomm::ConcatePath(sPath, sFileName);
	ret = unlink(sAbsFileName.c_str());
	logerr("unlink %s ret %d %s", sAbsFileName.c_str(), ret, strerror(errno));
	return 0 == ret ? 0 : -4;
}


} // namespace


namespace dbcomm {


FileRecycleManager::FileRecycleManager(
		const char* sKvLogPath, 
		const char* sKvRecyclePath)
	: m_sKvLogPath(sKvLogPath)
	, m_sKvRecyclePath(sKvRecyclePath)
	, m_iStop(0)
	, m_bRunning(false)
{
	assert(NULL != sKvLogPath);
	assert(NULL != sKvRecyclePath);
	assert(0 == dbcomm::CheckAndFixDirPath(sKvRecyclePath));
}

FileRecycleManager::~FileRecycleManager()
{
	m_iStop = 1;
	if (true == m_bRunning)
	{
		pthread_join(m_tidRecycleThread, NULL);
		m_bRunning = false;
	}
}

void FileRecycleManager::Stop()
{
	m_iStop = 1;
}

int FileRecycleManager::StartRecycleFileThread()
{
	if (true == m_bRunning)
	{
		return 1;
	}

	int ret = pthread_create(&m_tidRecycleThread, NULL, RecycleFileThread, this);
	if (0 != ret)
	{
		logerr("pthread_create RecycleFileThread ret %d strerror %s", 
				ret, strerror(errno));
		return -1;
	}

	m_bRunning = true;
	return 0;
}

int FileRecycleManager::RunRecycleAll(int iUrgentDiskRatio)
{
	iUrgentDiskRatio = std::max(0, iUrgentDiskRatio);
	if (m_sKvLogPath == m_sKvRecyclePath)
	{
		return -2;
	}

    std::list<std::string> lstRecycleFile;
	int ret = Scan(m_sKvRecyclePath, lstRecycleFile);
	if (0 != ret)
	{
		logerr("Scan %s ret %d", m_sKvRecyclePath.c_str(), ret);
		return -1;
	}

	int iDeleteFileCnt = 0;
	for (std::list<std::string>::const_iterator iter = lstRecycleFile.begin();
			iter != lstRecycleFile.end(); ++iter)
	{
		ret = TryToDeleteDataFile(m_sKvRecyclePath, *iter);
		if (0 != ret)
		{
			logerr("ITER %s %s TryToDeleteDataFile ret %d", 
					m_sKvRecyclePath.c_str(), iter->c_str(), ret);
			continue;
		}

		assert(0 == ret); // success delete
		++iDeleteFileCnt;
		if (false == CheckDiskRatio(iUrgentDiskRatio, m_sKvLogPath)) {
			SafeSleep(5);
		}
	}

	logerr("STAT %s lstRecycleFile.size %lu iDeleteFileCnt %d", 
			m_sKvRecyclePath.c_str(), lstRecycleFile.size(), iDeleteFileCnt);
	return 0;
}

int FileRecycleManager::RunRecyclePartial(
		int iUrgentDiskRatio, int iStopDiskRatio)
{
	iUrgentDiskRatio = std::max(0, iUrgentDiskRatio);
	iStopDiskRatio = std::max(0, iStopDiskRatio);

    std::list<std::string> lstRecycleFile;
	int ret = Scan(m_sKvRecyclePath, lstRecycleFile);
	if (0 != ret)
	{
		logerr("Scan %s ret %d", m_sKvRecyclePath.c_str(), ret);
		return -1;
	}

	int iDeleteFileCnt = 0;
	for (std::list<std::string>::const_iterator iter = lstRecycleFile.begin();
			iter != lstRecycleFile.end(); ++iter)
	{
		ret = TryToDeleteDataFile(m_sKvRecyclePath, *iter);
		if (0 != ret)
		{
			logerr("ITER %s %s TryToDeleteDataFile ret %d", 
					m_sKvRecyclePath.c_str(), iter->c_str(), ret);
			continue;
		}

		assert(0 == ret); // success delete
		++iDeleteFileCnt;

		if (false == CheckDiskRatio(iStopDiskRatio, m_sKvLogPath))
		{
			break;
		}

		if (false == CheckDiskRatio(iUrgentDiskRatio, m_sKvLogPath)) {
			SafeSleep(5);
		}
		// else => urgent mode
	}

	logerr("STAT %s lstRecycleFile.size %lu iDeleteFileCnt %d", 
			m_sKvRecyclePath.c_str(), lstRecycleFile.size(), iDeleteFileCnt);
	return 0;
}


int FileRecycleManager::Run()
{
    BindToLastCpu();

	int iLoopCnt = 0;
	uint32_t arrDiskRatio[2] = {0};
    arrDiskRatio[0] = 60; // max disk ratio
    arrDiskRatio[1] = 76; // urgent disk ratio
	//arrDiskRatio[0] = m_poConfig->GetMaxDiskRatio();
	//arrDiskRatio[1] = m_poConfig->GetUrgentDiskRatio();
	while (true)
	{
		++iLoopCnt;

		int iMaxDiskRatio = std::min(arrDiskRatio[0], arrDiskRatio[1]);
		int iUrgentDiskRatio = std::max(arrDiskRatio[0], arrDiskRatio[1]);
		assert(0 <= iMaxDiskRatio);
		assert(0 <= iUrgentDiskRatio);
        int iAlarmDiskRatio = 90;
		// int iAlarmDiskRatio = m_poConfig->GetAlarmDiskRatio();
		iAlarmDiskRatio = std::max(iUrgentDiskRatio, iAlarmDiskRatio);
		assert(0 <= iAlarmDiskRatio);

		int iStopDiskRatio = std::min(iMaxDiskRatio - 5, iUrgentDiskRatio - 10);
		iStopDiskRatio = std::max(0, iStopDiskRatio);

		int iRecycleMode = RECYCLE_MODE_NIL;
		if (true == CheckDiskRatio(iAlarmDiskRatio, m_sKvLogPath))
		{
			iRecycleMode = RECYCLE_MODE_ALL;
		}
		else if (true == CheckDiskRatio(iMaxDiskRatio, m_sKvLogPath))
		{
			iRecycleMode = RECYCLE_MODE_PARTIAL;
		}

		int ret = 0;
		switch (iRecycleMode)
		{
			case RECYCLE_MODE_NIL:
				// do nothing
				break;
			case RECYCLE_MODE_ALL:
				ret = RunRecycleAll(iUrgentDiskRatio);
				break;
			case RECYCLE_MODE_PARTIAL:
				ret = RunRecyclePartial(iUrgentDiskRatio, iStopDiskRatio);
				break;
			default:
				ret = -1;
				break;
		}

		if (RECYCLE_MODE_NIL != iRecycleMode) {
			logerr("iRecycleMode %d iMaxDiskRatio %d iUrgentDiskRatio %d iStopDiskRatio %d ret %d", 
					iRecycleMode, iMaxDiskRatio, iUrgentDiskRatio, iStopDiskRatio, ret);
		}

		sleep(5);
	}

	return 0;
}

} // namespace dbcomm


