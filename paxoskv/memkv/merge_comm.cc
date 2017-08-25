
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <sstream>
#include <vector>
#include <algorithm>
#include <cassert>
#include "cutils/log_utils.h"
#include "dbcomm/db_comm.h"
#include "merge_comm.h"

namespace memkv {


namespace CheckMerge {

bool CheckFile(const char* sKvPath)
{
	std::vector<int> vecWriteFile;
	dbcomm::GatherWriteFilesFromDataPath(sKvPath, vecWriteFile);
	std::sort(vecWriteFile.begin(), vecWriteFile.end());
	if (vecWriteFile.empty() || size_t(1) == vecWriteFile.size())
	{
		return false;
	}

	int iMinWriteFileNo = vecWriteFile.front();
	int iMaxWriteFileNo = vecWriteFile.back();
	logerr("MergeStat DEBUG iMinWriteFileNo %d iMaxWriteFileNo %d", 
			iMinWriteFileNo, iMaxWriteFileNo);
	return iMinWriteFileNo != iMaxWriteFileNo;
}

int GetDataFileCountOn(const std::string& sDirPath)
{
	std::vector<int> vecWriteFile;
	dbcomm::GatherWriteFilesFromDataPath(sDirPath, vecWriteFile);
	std::vector<int> vecMergeFile;
	dbcomm::GatherMergeFilesFromDataPath(sDirPath, vecMergeFile);

	return vecWriteFile.size() + vecMergeFile.size();	
}

bool CheckUsageAgainstRecycle(
		const std::string& sKvPath, 
		const std::string& sKvRecyclePath, 
		int iMergeRatio)
{
	iMergeRatio = std::max(iMergeRatio, 0);
	iMergeRatio = std::min(iMergeRatio, 100);

	// write file
	int iKvPathDataFileCnt = GetDataFileCountOn(sKvPath);
	assert(0 <= iKvPathDataFileCnt);
	int iKvRecyclePathDataFileCnt = GetDataFileCountOn(sKvRecyclePath);
	assert(0 <= iKvRecyclePathDataFileCnt);
	logerr("MERGE iMergeRatio %d iKvPathDataFileCnt %d iKvRecyclePathDataFileCnt %d", 
			iMergeRatio, iKvPathDataFileCnt, iKvRecyclePathDataFileCnt);
	return iKvRecyclePathDataFileCnt * iMergeRatio <= iKvPathDataFileCnt * 100;
}

bool CheckDiskRatio(
		const char* sKvPath, const int iMaxDiskRatio)
{
	int iApproximateDiskRatio = iMaxDiskRatio - 5;
	iApproximateDiskRatio = 
        0 > iApproximateDiskRatio ? 0 : iApproximateDiskRatio;
	if (0 < dbcomm::DiskRatio(sKvPath, iApproximateDiskRatio)) {
		logerr("MergeStat DEBUG DiskRatio > iMaxDiskRatio %d"
				" iApproximateDiskRatio %d", 
				iMaxDiskRatio, iApproximateDiskRatio);
		return true;
	}

	return false;
}

bool CheckTime(const int iMergeCount, const int* arrMergeTime)
{
	int iSleepTime = 0;
	int iMinSleepTime = 0;
	int iMergeIndex = 0;
	for (int i = 0; i < iMergeCount; i++) 
	{
		iSleepTime = dbcomm::CalcSleepTime(arrMergeTime[i]);
		if (iSleepTime <= iMinSleepTime) 
		{
			iMergeIndex = i;
			iMinSleepTime = iSleepTime;
		}
	}

	iSleepTime = dbcomm::CalcSleepTime(arrMergeTime[iMergeIndex]);
	if( iSleepTime > 0 ) 
	{
		logerr("MergeStat DEBUG iSleepTime %d", iSleepTime);
		return false;
	}
	else if( iSleepTime == 0 ) 
	{
		logerr("MergeStat DEBUG iSleepTime %d", iSleepTime);
		return true;
	}
	else 
	{
		logerr("MergeStat DEBUG iSleepTime %d", iSleepTime);
		assert(0);
	}

	return true;
}

} // namespace CheckMerge

void Print(const std::vector<int>& vecFile, const char* sMsg)
{
	std::stringstream ss;
	ss << "size "  << vecFile.size();
	for (size_t i = 0; i < vecFile.size(); ++i)
	{
		ss << " " << vecFile[i];
	}

	std::string sOut = ss.str();
	logerr("%s %s", sMsg, sOut.c_str());
}


} // namespac memkv
