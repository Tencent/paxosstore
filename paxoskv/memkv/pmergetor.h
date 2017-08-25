
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <list>
#include <vector>
#include <string>
#include <stdint.h>


namespace dbcomm {

class HashBaseLock;

class clsLevelDBLogBufferIterWriter;

} // namespace dbcomm


namespace memkv {


class clsNewMemKv;

typedef struct s_HitRecord
{
	//uint8_t cStartFlag;
	uint8_t cFlag;
	uint8_t cKeyLen;
	uint32_t iValLen;
	char sKey[8];
	uint32_t iVerA;
	uint32_t iVerB;
	uint32_t iFileNo;
	uint32_t iOffset;
	//uint8_t cEndFlag;
} HitRecord_t;

typedef struct HR_s{
	int iOldFileNo;
	uint32_t iOldOffset;
	HitRecord_t stHitRecord;
} HR_t;

typedef std::vector<HR_t> HitRecordVec;



class PMergetor
{
public:
	PMergetor(
			const char* sPath, 
			const char* sKvRecyclePath, 
			clsNewMemKv& oMemKv, 
			dbcomm::HashBaseLock* poHashBaseLock);

	~PMergetor();

	void Stop() 
	{
		m_iStop = 1;
	}

	int Init();

	int Merge();

private:
//	bool IsNeedToMerge();
	int CalculateMergeMode(
			size_t iWriteFileCount, size_t iMergeFileCount);


	void GatherFilesToMerge(
			std::vector<int>& vecWriteFile, 
            std::vector<int>& vecMergeFile);

	int DumpPartialFile(const HitRecordVec& vecHitRecord);

	template <typename MemKvVisitorType>
	int MergeSomeFile_MemInner(
			const std::vector<int>& vecFile, 
			int iUrgentDiskRatio, 
			MemKvVisitorType& visitor);

	int MergeSomeFile_Mem(
            const std::vector<int>& vecFile, 
            int iUrgentDiskRatio);

	int MergeAllFiles(
			const std::vector<int>& vecFile, 
            int iUrgentDiskRatio, 
            size_t iMaxSplit);

private:
	const std::string m_sKvPath;
	const std::string m_sKvRecyclePath;

	clsNewMemKv& m_oMemKv;

    dbcomm::HashBaseLock* m_poHashBaseLock;

    dbcomm::clsLevelDBLogBufferIterWriter* m_poWriter;

	int m_iStop;
};


} // namespace memkv


