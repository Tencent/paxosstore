
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <algorithm>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include "leveldb/slice.h"
#include "cutils/log_utils.h"
#include "cutils/hassert.h"
#include "hashlock.h"
#include "db_comm.h"
#include "newstorage.h"
#include "leveldb_log.h"
#include "leveldb_log_impl.h"
#include "bitcask_log_impl.h"
#include "mmap_file.h"
#include "universal_batch.h"


#define DEFAULT_BLOCK_SIZE 4 * 1024


using dbimpl::Record_t;

namespace {

using namespace dbcomm;


std::string GetWriteFileName(
		const char* sPath, uint32_t iFileNo)
{
    std::string sFileName;
	sFileName.resize(256, 0);
	int iLen = snprintf(&sFileName[0], 
			sFileName.size()-1, "%s/%u.w", sPath, iFileNo);
	assert(0 < iLen);
	sFileName.resize(iLen);
	return sFileName;
}

std::string GetFileName(
		const char* sPath, int iFileNo)
{
	assert(0 != iFileNo);
    std::string sFileName;
	sFileName.resize(256, 0);
	int iLen = snprintf(&sFileName[0], sFileName.size()-1, 
			"%s/%d.%c", sPath, abs(iFileNo), 0 > iFileNo ? 'm' : 'w');
	assert(0 < iLen);
	sFileName.resize(iLen);
	return sFileName;
}

int InitAddBaseLock(const char* sPath, HashBaseLock*& poAddBaseLock)
{
	assert(NULL == poAddBaseLock);
	poAddBaseLock = new HashBaseLock();
	if (NULL == poAddBaseLock)
	{
		return -1;
	}

	assert(NULL != poAddBaseLock);
	char sLockPath[256] = {0};
	int ret = snprintf(sLockPath, 
			sizeof(sLockPath)-1, "%s/lock/storage_add.lock", sPath);
	if (0 >= ret)
	{
		delete poAddBaseLock;
		poAddBaseLock = NULL;
		return -2;
	}

	ret = poAddBaseLock->Init(sLockPath, 2);
	if (0 > ret)
	{
		logerr("HashBaseLock::Init %s ret %d", sLockPath, ret);
		delete poAddBaseLock;
		poAddBaseLock = NULL;
		return -3;
	}

	return 0;
}

int InitMaxWritePos(
		const char* sPath, stor_cursor_t*& pstMaxWritePos)
{
	assert(NULL == pstMaxWritePos);
	char sWritePosPath[256] = {0};
	int ret = snprintf(sWritePosPath, 
			sizeof(sWritePosPath)-1, "%s/write.pos", sPath);
	if (0 >= ret)
	{
		return -1;
	}

	pstMaxWritePos = stor_cursor_open(sWritePosPath);
	if (NULL == pstMaxWritePos)
	{
		return -2;
	}

	assert(NULL != pstMaxWritePos);
	printf ( "pstMaxWritePos file %s %u %u\n", sWritePosPath, 
			pstMaxWritePos->pos->iFileNo, pstMaxWritePos->pos->iOffset );
	return 0;
}

int ReadMetaInfoAndFixWritePos(
		const char* sPath, 
		const int iBlockSize, 
		stor_cursor_t* pstMaxWritePos, 
		uint64_t& llBlkSeq)
{
    std::vector<int> vecWriteFile;
	dbcomm::GatherWriteFilesFromDataPath(sPath, vecWriteFile);
	if (vecWriteFile.empty())
	{
		pstMaxWritePos->pos->iFileNo = 1;
		pstMaxWritePos->pos->iOffset = 0;
		llBlkSeq = 1; // init blk seq;
		return 0;
	}

	// else
    std::sort(vecWriteFile.begin(), vecWriteFile.end());
	// check [end-2, end-1]
	pstMaxWritePos->pos->iFileNo = vecWriteFile[vecWriteFile.size()-1];
    std::string sCurrentFile = GetWriteFileName(
            sPath, pstMaxWritePos->pos->iFileNo);
	int ret = dbcomm::IsLevelDBLogFormat(sCurrentFile.c_str());
	if (0 >= ret)
	{
		logerr("IsLevelDBLogFormat %s ret %d", sCurrentFile.c_str(), ret);
		if (0 > ret)
		{
			return ret; // ERROR CASE
		}

		// bitcask format;
		++(pstMaxWritePos->pos->iFileNo);
		pstMaxWritePos->pos->iOffset = 0;
		llBlkSeq = 1;
		return 0;
	}

	assert(1 == ret); // leveldb log format;
	int iFileBlockSize = 0;
	uint64_t llFileBlkSeq = 0;
	uint32_t iOffset = 0;
	ret = ReadMetaInfo(
			sCurrentFile.c_str(), 1024 * iBlockSize, 
			iFileBlockSize, llFileBlkSeq, iOffset);
	logdebug("INFO: ReadMetaInfo %s ret %d iFileBlockSize %d "
			"llFileBlkSeq %lu iOffset %u", 
			sCurrentFile.c_str(), ret, 
            iFileBlockSize, llFileBlkSeq, iOffset);
	if (0 != ret)
	{
		assert(0 > ret);
		return ret;
	}

	assert(0 == ret);
	// else=> assert(0 == ret); success read
	pstMaxWritePos->pos->iOffset = iOffset;
	assert(0 < llFileBlkSeq);
	if (iFileBlockSize != iBlockSize)
	{
		++(pstMaxWritePos->pos->iFileNo);
		pstMaxWritePos->pos->iOffset = 0;
	}

	llBlkSeq = llFileBlkSeq + 1;
	logerr("INFO: iFileNo %u iFileBlockSize %d "
            "llFileBlkSeq %lu iOffset %u", 
			pstMaxWritePos->pos->iFileNo, 
            iFileBlockSize, llFileBlkSeq, iOffset);

	// final check
	{
		dbcomm::clsReadOnlyMMapFile oMMFile;
		std::string sMostRecentFile = 
			GetWriteFileName(sPath, pstMaxWritePos->pos->iFileNo);
		assert(false == sMostRecentFile.empty());
		// filename
		assert(0 == oMMFile.OpenFile(sMostRecentFile));

		dbcomm::clsLevelDBLogAttachReader oReader;
		assert(0 == oReader.Attach(oMMFile.Begin(), oMMFile.End(), 0));
		printf ( "TRY: sMostRecentFile %s\n", sMostRecentFile.c_str() );
		while (true) {
			std::string sRawRecord;
			uint32_t iRecordOffset = 0;
			ret = oReader.Read(sRawRecord, iRecordOffset);
			if (1 == ret) {
				break; // EOF
			}

			if (0 > ret) {
				++(pstMaxWritePos->pos->iFileNo);
				pstMaxWritePos->pos->iOffset = 0;
				break;
			}

			assert(0 == ret);
		}

		logerr("INFO: iFileNo %u iFileBlockSize %d "
                "llFileBlkSeq %lu iOffset %u", 
				pstMaxWritePos->pos->iFileNo, 
                iFileBlockSize, llFileBlkSeq, iOffset);
	}
	return 0; // success fix
}

int InitLogIterWriter(
		const char* sPath, 
		const char* sKvRecyclePath, 
		const stor_cursor_t* pstMaxWritePos, 
		const int iBlockSize, 
		const int iMaxDirectIOBufSize, 
		const uint64_t llBlkSeq, 
		const int iAdjustStrategy, 
		dbcomm::clsLevelDBLogIterWriter*& poWriter)
{
	assert(NULL != sPath);
	assert(NULL != sKvRecyclePath);
	assert(NULL != pstMaxWritePos);
	assert(0 < iBlockSize);
	assert(iBlockSize <= iMaxDirectIOBufSize);
	assert(0 == iMaxDirectIOBufSize % iBlockSize);
	assert(0 < llBlkSeq);
	assert(0 <= iAdjustStrategy);
	assert(NULL == poWriter);

	// bIsMergeWrite = false;
	poWriter = new dbcomm::clsLevelDBLogIterWriter(
            sPath, sKvRecyclePath, false);
	logerr("INFO: clsLevelDBLogIterWriter %s %s "
            "llBlkSeq %lu iFileNo %u iOffset %u", 
			sPath, sKvRecyclePath, llBlkSeq, 
            pstMaxWritePos->pos->iFileNo, 
			pstMaxWritePos->pos->iOffset);
	assert(NULL != poWriter);
	int ret = poWriter->Init(
			iBlockSize, iMaxDirectIOBufSize, 
			0, llBlkSeq, iAdjustStrategy, 
			pstMaxWritePos->pos->iFileNo, 
			pstMaxWritePos->pos->iOffset);
	if (0 != ret)
	{
		logerr("clsLevelDBLogIterWriter::Init ret %d", ret);
		delete poWriter;
		poWriter = NULL;
		return -1;
	}

	return 0;
}

//bool NeedCompresse(KvsvrConfig* poConfig, const Record_t& tRecord)
//{
//	assert(NULL != poConfig);
//	uint32_t iValLen = tRecord.iValLen;
//	// 1. ivalLen too small or tRecord already mark as compressed!
//	if (iValLen <= 1 || 0 != (RECORD_COMPRESSE & tRecord.cFlag))
//	{
//		return false;
//	}
//
//	uint32_t iUin = 0;
//	memcpy(&iUin, tRecord.sKey, sizeof(uint32_t));
//
//	static unsigned int iReadConfigCount = 0;
//	static int iSnappyRatio = 0;
//	if ((iReadConfigCount++ % 200000) == 0) 
//	{
//		int iNewSnappyRatio = 0;
//		int ret = poConfig->GetIntByReadConfig(
//				"KVDB", "SnappyRatio", 0, iNewSnappyRatio);
//		if (0 == ret)
//		{
//			iSnappyRatio = iNewSnappyRatio;
//		}
//
//		logerr("INFO:iSnappyRatio %u ret %d", iSnappyRatio, ret);
//	}
//
//	return ((int )(dictIntHashFunction(iUin) % 100) < iSnappyRatio);
//}


// after the call, tValue hold the resp to delete [] the buffer memory
int ToBufferImpl(
		const Record_t& tRecord, leveldb::Slice& tValue)
{
	int iRecordSize = dbimpl::CalculateRecordSize(tRecord);
    assert(0 < iRecordSize);

	char* pRecordBuffer = new char[iRecordSize];
	assert(true == tValue.empty());

    dbimpl::ToBufferImpl(
            tRecord, iRecordSize, pRecordBuffer, iRecordSize);

	// success ToBuffer
	tValue = leveldb::Slice(pRecordBuffer, iRecordSize);	
	return 0;
}

int ToBufferWithRatioCompresse(
		const Record_t& tRecord, leveldb::Slice& tValue)
{
	return ToBufferImpl(tRecord, tValue);
}


int BatchToBufferWithRatioCompresse(
		const dbimpl::RecordWithPos_t* pRecords, 
        const int iCount, 
		std::vector<leveldb::Slice>& vecValue)
{
	assert(NULL != pRecords);
	assert(0 < iCount);

	vecValue.resize(iCount);
	int ret = 0;
	for (int i = 0; i < iCount; ++i)
	{
		const dbimpl::RecordWithPos_t& tRecordWithPos = pRecords[i];
		ret = ToBufferWithRatioCompresse(
				*(tRecordWithPos.pRecord), vecValue[i]);	
		if (0 != ret)
		{
			// interupt
			assert(true == vecValue[i].empty());
			break;
		}
		assert(false == vecValue[i].empty());
	}

	if (0 != ret)
	{
		// ERROR CASE
		assert(static_cast<size_t>(iCount) == vecValue.size());
		for (size_t i = 0; i < vecValue.size(); ++i)
		{
			if (vecValue[i].empty())
			{
				break; // loop till error case
			}

			delete [] const_cast<char*>(vecValue[i].data());
			vecValue[i].clear();
		}
		vecValue.clear();
	}
	return ret;
}


class WriteHelper
{
public:
	WriteHelper(
			HashBaseLock* poAddBaseLock, 
			stor_cursor_t* pstMaxWritePos)
		: m_poAddBaseLock(poAddBaseLock)
		, m_pstMaxWritePos(pstMaxWritePos)
	{
		assert(NULL != m_poAddBaseLock);
		assert(NULL != m_pstMaxWritePos);
	}

	int Write(
			clsLevelDBLogIterWriter& oWriter, 
			const Record_t& tRecord, 
			uint32_t& iFileNo, uint32_t& iOffset)
	{
		leveldb::Slice tValue;
		int ret = ToBufferWithRatioCompresse(tRecord, tValue);
		if (0 != ret)
		{
			logerr("ToBufferWithRatioCompresse ret %d", ret);
			return -1;
		}

		assert(false == tValue.empty());
		// after compress: 
		ret = WriteImpl(oWriter, tValue, iFileNo, iOffset);
		// free tValue
		delete [] const_cast<char*>(tValue.data());
		tValue.clear();

		static __thread uint64_t llTotalWriteCnt = 0;
		++llTotalWriteCnt;
		if (llTotalWriteCnt % 1000 == 0)
		{
            std::string sStatInfo = oWriter.GetStatInfo();
			logerr("WRITE STAT llTotalWriteCnt %lu %s", 
					llTotalWriteCnt, sStatInfo.c_str());
		}

		return ret;
	}

	int BatchWrite(
			clsLevelDBLogIterWriter& oWriter, 
			dbimpl::RecordWithPos_t* pRecords, 
			const int iCount)
	{
		assert(NULL != pRecords);
		assert(0 < iCount);

		// convert record into buffer
		// => use leveldb::Slice for convient, 
        //    but still need delete the buffer
		// => the buffer allocate by new []
		uint64_t llBeginTime = dbcomm::GetTickMS();
		uint64_t llCompressTime = 0;
		uint64_t llBatchWriteTime = 0;
        std::vector<leveldb::Slice> vecValue;
		int ret = BatchToBufferWithRatioCompresse(
                pRecords, iCount, vecValue);
		if (0 != ret)
		{
			logerr("BatchToBufferWithRatioCompresse ret %d", ret);
			assert(true == vecValue.empty());
			return -1;
		}
		llCompressTime = dbcomm::GetTickMS() - llBeginTime;
		llBeginTime = dbcomm::GetTickMS();

		assert(static_cast<size_t>(iCount) == vecValue.size());
		// after compress: batch write back
		ret = BatchWriteImpl(oWriter, vecValue, pRecords, iCount);
		llBatchWriteTime = dbcomm::GetTickMS() - llBeginTime;
		if (llCompressTime + llBatchWriteTime > 10)
		{
			logerr("PERFORMANCE TIMESTAT llCompressTime %lu llBatchWriteTime %lu iCount %d", 
					llCompressTime, llBatchWriteTime, iCount);
		}

		static __thread uint64_t llTotalWriteCnt = 0;
		static __thread uint64_t llBatchSize = 0;
		++llTotalWriteCnt;
		llBatchSize += vecValue.size();
		if (llTotalWriteCnt % 10000 == 0)
		{
            std::string sStatInfo = oWriter.GetStatInfo();
			logerr("WRITE BSTAT llTotalWriteCnt %lu "
					"llBatchSize %lu %s", 
					llTotalWriteCnt, llBatchSize, 
					sStatInfo.c_str());
		}

		// free vecValue
		for (size_t i = 0; i < vecValue.size(); ++i)
		{
			leveldb::Slice& tMem = vecValue[i];
			assert(false == tMem.empty());
			delete [] const_cast<char*>(tMem.data());
			tMem.clear();
		}

		return ret;
	}

private:
	int WriteImpl(
			clsLevelDBLogIterWriter& oWriter, 
			const leveldb::Slice& tValue, 
            uint32_t& iFileNo, uint32_t& iOffset)
	{
		assert(false == tValue.empty());
		int ret = 0;
		{
			// use m_poAddBaseLock LOCK
            dbcomm::HashLock hashlock(m_poAddBaseLock, uint32_t{0});
			hashlock.WriteLock(__FILE__, __LINE__);

			{
				ret = oWriter.WriteNoLock(
					tValue.data(), tValue.size(), iFileNo, iOffset);
			}

			if (0 == ret)
			{
				// update iFileNo & iOffset
				if (iFileNo != m_pstMaxWritePos->pos->iFileNo)
				{
					m_pstMaxWritePos->pos->iFileNo = iFileNo;
				}
				
				// the next write pos
				m_pstMaxWritePos->pos->iOffset = oWriter.GetCurrentOffsetNoLock();
			}
		}

		if (0 != ret) 
		{
			logerr("Write tValue.size %zu ret %d", tValue.size(), ret);
		}

		return ret;
	}

	int BatchWriteImpl(
			clsLevelDBLogIterWriter& oWriter, 
			const std::vector<leveldb::Slice>& vecValue, 
			dbimpl::RecordWithPos_t* pRecords, const int iCount)
	{
		assert(NULL != pRecords);
		assert(static_cast<size_t>(iCount) == vecValue.size());

		uint32_t iFileNo = 0;
        std::vector<uint32_t> vecOffset;
		vecOffset.reserve(vecValue.size());

		int ret = 0;
		uint64_t llBeginTime = 0;
		uint64_t llLockTime = 0;
		uint64_t llWriteTime = 0;

		bool bIterFileNo = false;
		uint32_t iPrevOffset = 0;
		uint32_t iNewOffset = 0;
		{
			llBeginTime = dbcomm::GetTickMS();
            dbcomm::HashLock hashlock(m_poAddBaseLock, uint32_t{0});
			hashlock.WriteLock(__FILE__, __LINE__);
			llLockTime = dbcomm::GetTickMS() - llBeginTime;

			llBeginTime = dbcomm::GetTickMS();
			assert(false == vecValue.empty());
			// add a global lock

			iPrevOffset = oWriter.GetCurrentOffsetNoLock();
			{
				ret = oWriter.BatchWriteNoLock(vecValue, iFileNo, vecOffset);
			}
			llWriteTime = dbcomm::GetTickMS() - llBeginTime;

			if (0 == ret)
			{
				assert(vecValue.size() == vecOffset.size());
				if (iFileNo != m_pstMaxWritePos->pos->iFileNo)
				{
					m_pstMaxWritePos->pos->iFileNo = iFileNo;
					bIterFileNo = true;
				}

				// the next write pos
				m_pstMaxWritePos->pos->iOffset = oWriter.GetCurrentOffsetNoLock();
				iNewOffset = oWriter.GetCurrentOffsetNoLock();
			}
		}

		if (0 != ret) 
		{
			logerr("BatchWrite vecValue.size %zu ret %d", vecValue.size(), ret);
		}

		if (llLockTime + llWriteTime > 40)
		{
			// TODO: writesize ?
			logerr("PERFORMANCE TIMESTAT llLockTime %lu llWriteTime %lu iCount %d [%d %u %u]", 
					llLockTime, llWriteTime, iCount, 
					bIterFileNo, iPrevOffset, iNewOffset);
		}

		// update
		if (0 != ret)
		{
			return ret;
		}

		for (int i = 0; i < iCount; ++i)
		{
            dbimpl::RecordWithPos_t& tRecordWithPos = pRecords[i];
			tRecordWithPos.iFileNo = iFileNo;
			tRecordWithPos.iOffset = vecOffset[i];
		}

		return 0;
	}

private:
	HashBaseLock* m_poAddBaseLock;
	stor_cursor_t* m_pstMaxWritePos;
};


} // namespace 


namespace dbcomm {




int CalculateDumpSize(
        const dbimpl::RecordWithPos_t* pRecords, const int iCount)
{
	if (NULL == pRecords || 0 == iCount)
	{
		return 0;
	}

	int iEstimateRecordSize = 0;
	for (int i = 0; i < iCount; ++i)
	{
		const dbimpl::RecordWithPos_t& tRecordWithPos = pRecords[i];
        int iRecordSize = 
            dbimpl::CalculateRecordSize(*(tRecordWithPos.pRecord));
		assert(0 < iRecordSize);
		iEstimateRecordSize += iRecordSize;
	}

	return iEstimateRecordSize;
}


NewStorage::NewStorage()
	: m_poAddBaseLock(NULL)
	, m_pstMaxWritePos(NULL)
	, m_poWriter(NULL)
	// , m_poBatchWriteHandler(NULL)
{

}

NewStorage::~NewStorage()
{
//	if (NULL != m_poBatchWriteHandler)
//	{
//		delete m_poBatchWriteHandler;
//		m_poBatchWriteHandler = NULL;
//	}

	if (NULL != m_poWriter)
	{
		delete m_poWriter;
		m_poWriter = NULL;
	}
	
	if (NULL != m_pstMaxWritePos)
	{
		stor_cursor_close(m_pstMaxWritePos);
		m_pstMaxWritePos = NULL;
	}

	if (NULL != m_poAddBaseLock)
	{
		delete m_poAddBaseLock;
		m_poAddBaseLock = NULL;
	}
}


int NewStorage::Init(
		const char* sPath, 
		const char* sKvRecyclePath, 
		const int iBlockSize, 
		const int iMaxDirectIOBufSize, 
		const int iAdjustStrategy, 
        const int iWaitTime)
{
	assert(NULL == m_poAddBaseLock);
	assert(NULL == m_pstMaxWritePos);
	assert(NULL == m_poWriter);
	// assert(NULL == m_poBatchWriteHandler);

	assert(NULL != sPath);

	m_sKvLogPath = sPath;

	// lock
	int ret = 0;
	{
		HashBaseLock* poAddBaseLock = NULL;
		ret = InitAddBaseLock(sPath, poAddBaseLock);
		if (0 != ret)
		{
			assert(NULL == poAddBaseLock);
			logerr("InitAddBaseLock ret %d", ret);
			return -1;
		}

        std::swap(m_poAddBaseLock, poAddBaseLock);
		assert(NULL != m_poAddBaseLock);
		assert(NULL == poAddBaseLock);
	}

	// pstMaxWritePos
	{
		stor_cursor_t* pstMaxWritePos = NULL;
		ret = InitMaxWritePos(sPath, pstMaxWritePos);
		if (0 != ret)
		{
			assert(NULL == pstMaxWritePos);
			logerr("InitMaxWritePos ret %d", ret);
			return -2;
		}
		
        std::swap(m_pstMaxWritePos, pstMaxWritePos);
		assert(NULL != m_pstMaxWritePos);
		assert(NULL == pstMaxWritePos);
	}


	uint64_t llBlkSeq = 0;
	ret = ReadMetaInfoAndFixWritePos(
			sPath, iBlockSize, m_pstMaxWritePos, llBlkSeq);
	if (0 != ret)
	{
		logerr("ReadMetaInfoAndFixWritePos ret %d", ret);
		return -3;
	}

	hassert(0 < llBlkSeq, "llBlkSeq %lu", llBlkSeq);
	{
		clsLevelDBLogIterWriter* poWriter = NULL;
		ret = InitLogIterWriter(
				sPath, sKvRecyclePath, m_pstMaxWritePos, iBlockSize, 
				iMaxDirectIOBufSize, llBlkSeq, iAdjustStrategy, poWriter);
		if (0 != ret)
		{
			assert(NULL == poWriter);
			logerr("InitLogIterWriter ret %d", ret);
			return -3;
		}

        std::swap(m_poWriter, poWriter);
		assert(NULL != m_poWriter);
		assert(NULL == poWriter);
	}

	assert(0 <= iWaitTime);
	// m_poBatchWriteHandler = new clsUniversalBatch(this, iWaitTime);
	// assert(NULL != m_poBatchWriteHandler);
	// m_poBatchWriteHandler->StartMonitor();
	
	logerr("INFO: path %s iFileNo %u iOffset %u llBlkSeq %lu", 
			m_sKvLogPath.c_str(), 
			m_poWriter->GetFileNoNoLock(), 
			m_poWriter->GetCurrentOffsetNoLock(), 
			m_poWriter->GetCurrentBlkSeqNoLock());
	return 0;
}

int NewStorage::Add(
		const dbimpl::Record_t* pstRecord, 
		uint32_t* piFileNo, 
        uint32_t* piOffset)
{
	uint32_t iFileNo = 0;
	uint32_t iOffset = 0;

	uint64_t llBeginTime = dbcomm::GetTickMS();
	int ret = AddImpl(*pstRecord, iFileNo, iOffset);
	uint64_t llCostTime = dbcomm::GetTickMS() - llBeginTime;
	if (10 <= llCostTime)
	{
		logerr("PERFORMANCE llCostTime %lu Write ret %d", 
                llCostTime, ret);
	}

	if (0 != ret)
	{
		return ret;
	}

	if (NULL != piFileNo)
	{
		*piFileNo = iFileNo;
	}

	if (NULL != piOffset)
	{
		*piOffset = iOffset;
	}
	return 0;
}

int NewStorage::BatchAdd(
        dbimpl::RecordWithPos_t* pRecords, const int iCount)
{
	if (NULL == pRecords || 0 >= iCount)
	{
		return 0;
	}

	assert(NULL != m_poAddBaseLock);
	assert(NULL != m_pstMaxWritePos);
	assert(NULL != m_poWriter);

	int iEstimateRecordSize = CalculateDumpSize(pRecords, iCount);

	uint32_t iOldFileNo = m_pstMaxWritePos->pos->iFileNo;
	uint64_t llBeginTime = dbcomm::GetTickMS();
	WriteHelper oHelper(
			m_poAddBaseLock, m_pstMaxWritePos);
	int ret = oHelper.BatchWrite(*m_poWriter, pRecords, iCount);
	uint64_t llCostTime = dbcomm::GetTickMS() - llBeginTime;
	if (100 < llCostTime)
	{
		logerr("DISK PERFORMANCE iEstimateRecordSize %d "
                "llCostTime %lu iCount %d ret %d", 
				iEstimateRecordSize, llCostTime, iCount, ret);
	}

	if (0 != ret)
	{
		return ret; // ERROR CASE
	}

	return 0;
}


int NewStorage::AddImpl(
		const dbimpl::Record_t& tRecord, 
		uint32_t& iFileNo, uint32_t& iOffset)
{
	assert(NULL != m_poAddBaseLock);
	assert(NULL != m_pstMaxWritePos);
	assert(NULL != m_poWriter);
	// NormalWrite
	// => Lock
	// every write compete exactly this one lock
	
    int iEstimateRecordSize = dbimpl::CalculateRecordSize(tRecord);

	uint32_t iOldFileNo = m_pstMaxWritePos->pos->iFileNo;

	uint64_t llBeginTime = dbcomm::GetTickMS();
	WriteHelper oHelper(
			// m_pstMaxWritePos must be update under lock !!
			m_poAddBaseLock, m_pstMaxWritePos);
	int ret = oHelper.Write(*m_poWriter, tRecord, iFileNo, iOffset);
	uint64_t llCostTime = dbcomm::GetTickMS() - llBeginTime;
	if (100 < llCostTime) 
	{
		logerr("DISK PERFORMANCE iEstimateRecordSize %d "
                "llCostTime %lu ret %d", 
				iEstimateRecordSize, llCostTime, ret);
	}

	if (0 != ret)
	{
		return ret; // ERROR CASE
	}

	return 0;
}

int NewStorage::Get(
        uint32_t iFileNo, uint32_t iOffset, 
        std::string& sRawRecord)
{
	assert(NULL != m_poAddBaseLock);
	assert(NULL != m_pstMaxWritePos);

	clsLevelDBLogPReader oReader;
    std::string sFileName = GetFileName(
            m_sKvLogPath.c_str(), static_cast<int>(iFileNo));
	int ret = oReader.OpenFile(sFileName.c_str(), DEFAULT_BLOCK_SIZE * 2);
	if (0 != ret)
	{
		return ret;
	}

	uint32_t iNextOffset = 0;
	ret = oReader.Read(iOffset, sRawRecord, iNextOffset);
	if (0 != ret)
	{
		return ret;
	}

	return 0;
}


} // namespace dbcomm




