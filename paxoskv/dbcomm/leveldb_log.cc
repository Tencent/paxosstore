
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <sys/mman.h>
#include <errno.h>
#include <algorithm>
#include <cstring>
#include <cassert>
#include "leveldb_log.h"
#include "leveldb_log_impl.h"
#include "leveldb/slice.h"
#include "db_comm.h"
#include "cutils/log_utils.h"
#include "cutils/hassert.h"



using dbcomm::INVALID_FD;
using dbimpl::DirectIOWriteInfo;
using dbimpl::DirectIOReadInfo;
using namespace std;


namespace dbcomm {

static const char WRITE_POSTFIX = 'w';
static const char MERGE_POSTFIX = 'm';

clsLevelDBLogIterWriter::clsLevelDBLogIterWriter(
		const char* sKvLogPath, const char* sKvRecyclePath, bool bIsMergeWrite)
	: m_sKvLogPath(sKvLogPath)
	, m_sKvRecyclePath(NULL == sKvRecyclePath ? "" : sKvRecyclePath)
	, m_bIsMergeWrite(bIsMergeWrite)
	, m_iFileNo(0)
	, m_ptWInfo(NULL)
{
	assert(false == m_sKvLogPath.empty());
	// assert(0 == pthread_mutex_init(&m_tLogLock, NULL));
}

clsLevelDBLogIterWriter::~clsLevelDBLogIterWriter()
{
	if (NULL != m_ptWInfo)
	{
		delete m_ptWInfo;
		m_ptWInfo = NULL;
	}
	
	// pthread_mutex_destroy(&m_tLogLock);
}

int clsLevelDBLogIterWriter::Init(
		const int iBlockSize, 
		const int iMaxDirectIOBufSize, 
		const int iMinTailRecordSize, 
		const uint64_t llStartBlkSeq, 
		const int iAdjustStrategy, 
		const uint32_t iFileNo, 
		const uint32_t iOffset)
{
	DirectIOWriteInfo* ptNewWInfo = NULL;
	{
		char sFileNameBuf[256] = {0};
		int ret = snprintf(sFileNameBuf, sizeof(sFileNameBuf)-1, 
				"%s/%u.%c", m_sKvLogPath.c_str(), iFileNo, 
				m_bIsMergeWrite ? MERGE_POSTFIX : WRITE_POSTFIX);
		if (0 >= ret)
		{
			return -1;
		}

		ret = dbimpl::CreateADirectIOWriteInfo(
				sFileNameBuf, 
				iBlockSize, iMaxDirectIOBufSize, iMinTailRecordSize, 
				llStartBlkSeq, iAdjustStrategy, iOffset, ptNewWInfo);
		if (0 != ret)
		{
			assert(NULL == ptNewWInfo);
			logerr("CreateADirectIOWriteInfo %s ret %d", sFileNameBuf, ret);
			return -2;
		}
	}
	assert(NULL != ptNewWInfo);

	{
        std::lock_guard<std::mutex> lock(m_tLogLock);
		swap(m_ptWInfo, ptNewWInfo);
		assert(NULL != m_ptWInfo);
		m_iFileNo = iFileNo;
	}

	if (NULL != ptNewWInfo)
	{
		delete ptNewWInfo;
		ptNewWInfo = NULL;
	}
	return 0;
}

int clsLevelDBLogIterWriter::WriteNoLock(
		const char* pValue, int iValLen, uint32_t& iFileNo, uint32_t& iOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (NULL == pValue || 0 >= iValLen)
	{
		return -2;
	}

	assert(NULL != m_ptWInfo);
	assert(NULL != pValue);
	assert(0 < iValLen);

	int ret = 0;
	for (int iRetryCnt = 0; iRetryCnt < 2; ++iRetryCnt)
	{
		ret = dbimpl::DirectIOWrite(m_ptWInfo, pValue, iValLen, iOffset);
		if (DB_WRITE_ERROR_NEXT_FILE == ret && 0 == iRetryCnt)
		{
			ret = IterIntoNextFileNoLock();
			if (0 != ret)
			{
				logerr("IterIntoNextFileNoLock ret %d", ret);
				ret = -3;
				break;
			}
			
			assert(0 == ret);
			assert(0 == m_ptWInfo->iOffset);
			assert(0 == m_ptWInfo->iDirectIOOffset);
			// retry
			continue;
		}

		break;
	}

	if (0 != ret)
	{
		return ret;
	}

	iFileNo = m_iFileNo;
	return 0;
}

int clsLevelDBLogIterWriter::Write(
		const char* pValue, int iValLen, uint32_t& iFileNo, uint32_t& iOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (NULL == pValue || 0 >= iValLen)
	{
		return -2;
	}

    std::lock_guard<std::mutex> lock(m_tLogLock);
	return WriteNoLock(pValue, iValLen, iFileNo, iOffset);
}

int clsLevelDBLogIterWriter::BatchWriteNoLock(
		const std::vector<leveldb::Slice>& vecValue, 
		uint32_t& iFileNo, std::vector<uint32_t>& vecOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (vecValue.empty())
	{
		return -2;
	}

	assert(NULL != m_ptWInfo);
	assert(false == vecValue.empty());

	int ret = 0;
	for (int iRetryCnt = 0; iRetryCnt < 2; ++iRetryCnt)
	{
		ret = dbimpl::BatchDirectIOWrite(m_ptWInfo, vecValue, vecOffset);
		if (DB_WRITE_ERROR_NEXT_FILE == ret && 0 == iRetryCnt)
		{
			ret = IterIntoNextFileNoLock();
			if (0 != ret)
			{
				logerr("IterIntoNextFileNoLock ret %d", ret);
				ret = -3;
				break;
			}

			assert(0 == ret);
			assert(0 == m_ptWInfo->iOffset);
			assert(0 == m_ptWInfo->iDirectIOOffset);
			// retry
			continue;
		}

		break;
	}

	if (0 != ret)
	{
		return ret;
	}

	iFileNo = m_iFileNo;
	return 0;
}

int clsLevelDBLogIterWriter::BatchWrite(
		const std::vector<leveldb::Slice>& vecValue, 
		uint32_t& iFileNo, std::vector<uint32_t>& vecOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (vecValue.empty())
	{
		return -2;
	}

    std::lock_guard<std::mutex> lock(m_tLogLock);
	return BatchWriteNoLock(vecValue, iFileNo, vecOffset);
}

int clsLevelDBLogIterWriter::IterIntoNextFileNoLock()
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	// 2. 
	int ret = dbimpl::IterWriteFile(
			m_ptWInfo, m_sKvLogPath, m_sKvRecyclePath, m_iFileNo + 1, 
			m_bIsMergeWrite ? MERGE_POSTFIX : WRITE_POSTFIX);
	logdebug("IterWriteFile %s %d ret %d", 
			m_sKvLogPath.c_str(), m_iFileNo+1, ret);
	if (0 != ret)
	{
		logerr("IterWriteFile %s %d ret %d", 
				m_sKvLogPath.c_str(), m_iFileNo + 1, ret);
		return ret;
	}

	++m_iFileNo;
	return 0;
}

uint32_t clsLevelDBLogIterWriter::GetCurrentOffset()
{
    std::lock_guard<std::mutex> lock(m_tLogLock);
	return GetCurrentOffsetNoLock();
}

uint32_t clsLevelDBLogIterWriter::GetCurrentOffsetNoLock() const
{
	assert(NULL != m_ptWInfo);
	return m_ptWInfo->iOffset;
}

uint64_t clsLevelDBLogIterWriter::GetCurrentBlkSeqNoLock() const
{
	assert(NULL != m_ptWInfo);
	return m_ptWInfo->oSeqGen.Next();
}


std::string clsLevelDBLogIterWriter::GetStatInfo() const
{
	assert(NULL != m_ptWInfo);
	return dbimpl::GetStatInfo(m_ptWInfo);
}


clsLevelDBLogWriter::clsLevelDBLogWriter()
	: m_ptWInfo(NULL)
{

}

clsLevelDBLogWriter::~clsLevelDBLogWriter()
{
	if (NULL != m_ptWInfo)
	{
		delete m_ptWInfo;
		m_ptWInfo = NULL;
	}
}

int clsLevelDBLogWriter::OpenFile(
		const char* sLevelDBLogFile, 
		const int iBlockSize, const int iMaxDirectIOBufSize, 
		const int iMinTailRecordSize, const uint64_t llStartBlkSeq, 
		const int iAdjustStrategy)
{
	DirectIOWriteInfo* ptNewWInfo = NULL;
	{
		int ret = dbimpl::CreateADirectIOWriteInfo(
				sLevelDBLogFile, iBlockSize, iMaxDirectIOBufSize, 
				iMinTailRecordSize, llStartBlkSeq, iAdjustStrategy, 0, ptNewWInfo);
		if (0 != ret)
		{
			assert(NULL != ptNewWInfo);
			logerr("CreateADirectIOWriteInfo %s ret %d", sLevelDBLogFile, ret);
			return -2;
		}
	}

	assert(NULL != ptNewWInfo);
	swap(m_ptWInfo, ptNewWInfo);
	assert(NULL != m_ptWInfo);
	if (NULL != ptNewWInfo)
	{
		delete ptNewWInfo;
		ptNewWInfo = NULL;
	}

	return 0;
}

int clsLevelDBLogWriter::Write(
		const char* pValue, const int iValLen, uint32_t& iOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (NULL == pValue || 0 >= iValLen)
	{
		return -2;
	}

	assert(NULL != m_ptWInfo);
	assert(NULL != pValue);
	assert(0 < iValLen);

	return dbimpl::DirectIOWrite(m_ptWInfo, pValue, iValLen, iOffset);
}

int clsLevelDBLogWriter::BatchWrite(
		const std::vector<leveldb::Slice>& vecValue, 
		std::vector<uint32_t>& vecOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (vecValue.empty())
	{
		return -2;
	}

	assert(NULL != m_ptWInfo);
	assert(false == vecValue.empty());

	return dbimpl::BatchDirectIOWrite(m_ptWInfo, vecValue, vecOffset);	
}


clsLevelDBLogBufferIterWriter::clsLevelDBLogBufferIterWriter(
		const char* sKvLogPath, const char* sKvRecyclePath, bool bIsMergeWrite)
	: m_sKvLogPath(sKvLogPath)
	, m_sKvRecyclePath(NULL == sKvRecyclePath ? "" : sKvRecyclePath)
	, m_bIsMergeWrite(bIsMergeWrite)
	, m_iFileNo(0)
	, m_ptWInfo(NULL)
{

}

clsLevelDBLogBufferIterWriter::~clsLevelDBLogBufferIterWriter()
{
	if (NULL != m_ptWInfo)
	{
		delete m_ptWInfo;
		m_ptWInfo = NULL;
	}
}

int clsLevelDBLogBufferIterWriter::Init(
		const int iBlockSize, 
		const int iMaxDirectIOBufSize, 
		const int iMinTailRecordSize, 
		const uint64_t llStartBlkSeq, 
		const int iAdjustStrategy, 
		const uint32_t iFileNo, 
		const uint32_t iOffset)
{
	DirectIOWriteInfo* ptNewWInfo = NULL;
	{
		char sFileNameBuf[256] = {0};
		int ret = snprintf(sFileNameBuf, sizeof(sFileNameBuf)-1, 
				"%s/%u.%c", m_sKvLogPath.c_str(), iFileNo, 
				m_bIsMergeWrite ? MERGE_POSTFIX : WRITE_POSTFIX);
		if (0 >= ret)
		{
			return -1;
		}

		ret = dbimpl::CreateADirectIOWriteInfo(
				sFileNameBuf, 
				iBlockSize, iMaxDirectIOBufSize, iMinTailRecordSize, 
				llStartBlkSeq, iAdjustStrategy, iOffset, ptNewWInfo);
		if (0 != ret)
		{
			assert(NULL == ptNewWInfo);
			logerr("CreateADirectIOWriteInfo %s ret %d", sFileNameBuf, ret);
			return -2;
		}
	}
	assert(NULL != ptNewWInfo);

	swap(m_ptWInfo, ptNewWInfo);
	assert(NULL != m_ptWInfo);
	m_iFileNo = iFileNo;
	if (NULL != ptNewWInfo)
	{
		delete ptNewWInfo;
		ptNewWInfo = NULL;
	}
	return 0;
}

int clsLevelDBLogBufferIterWriter::Write(
		const char* pValue, int iValLen, 
		uint32_t& iFileNo, uint32_t& iOffset)
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	if (NULL == pValue || 0 >= iValLen)
	{
		return -2;
	}

	assert(NULL != m_ptWInfo);
	assert(NULL != pValue);
	assert(0 < iValLen);
	int ret = 0;
	for (int iRetryCnt = 0; iRetryCnt < 2; ++iRetryCnt)
	{
		ret = dbimpl::DirectIOBufferWrite(m_ptWInfo, pValue, iValLen, iOffset);
		if (DB_WRITE_ERROR_NEXT_FILE == ret && 0 == iRetryCnt)
		{
			ret = IterIntoNextFile();
			if (0 != ret)
			{
				logerr("IterIntoNextFile ret %d", ret);
				ret = -3;
				break;
			}

			assert(0 == ret);
			assert(0 == m_ptWInfo->iOffset);
			assert(0 == m_ptWInfo->iDirectIOOffset);
			// retry
			continue;
		}
		break;
	}

	if (0 != ret)
	{
		return ret;
	}
	
	iFileNo = m_iFileNo;
	return 0;
}

int clsLevelDBLogBufferIterWriter::Flush()
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	return dbimpl::Flush(m_ptWInfo);
}

int clsLevelDBLogBufferIterWriter::IterIntoNextFile()
{
	if (NULL == m_ptWInfo)
	{
		return -1;
	}

	// 1. flush
	int ret = dbimpl::Flush(m_ptWInfo);
	if (0 != ret)
	{
		logerr("dbimpl::Flush ret %d", ret);
		return ret;
	}

	// 3. iter into next file
	ret = dbimpl::IterWriteFile(
			m_ptWInfo, m_sKvLogPath, m_sKvRecyclePath, m_iFileNo + 1, 
			m_bIsMergeWrite ? MERGE_POSTFIX : WRITE_POSTFIX);
	logdebug("IterWriteFile %s %d ret %d", 
			m_sKvLogPath.c_str(), m_iFileNo+1, ret);
	if (0 != ret)
	{
		logerr("IterWriteFile %s %d ret %d", 
				m_sKvLogPath.c_str(), m_iFileNo + 1, ret);
		return ret;
	}

	++m_iFileNo;
	return 0;
}

uint32_t clsLevelDBLogBufferIterWriter::GetBufferUsedSize() const
{
	assert(NULL != m_ptWInfo);
	uint32_t iUsedSize = 0;
	int ret = dbimpl::GetDirectIOBufferUsedSize(m_ptWInfo, iUsedSize);
	hassert(0 == ret, "GetDirectIOBufferUsedSize ret %d", ret);
	return iUsedSize;
}

uint32_t clsLevelDBLogBufferIterWriter::GetBufferUsedBlockSize() const
{
	assert(NULL != m_ptWInfo);
	uint32_t iUsedBlockSize = 0;
	int ret = dbimpl::GetDirectIOBufferUsedBlockSize(m_ptWInfo, iUsedBlockSize);
	hassert(0 == ret, "GetDirectIOBufferUsedBlockSize ret %d", ret);
	return iUsedBlockSize;
}

uint64_t clsLevelDBLogBufferIterWriter::GetAccWriteSize() const
{
	assert(NULL != m_ptWInfo);
	return dbimpl::GetDirectIOBufferAccWriteSize(m_ptWInfo);
}

uint32_t clsLevelDBLogBufferIterWriter::GetFileNo() const
{
	return m_iFileNo;
}

uint32_t clsLevelDBLogBufferIterWriter::GetCurrentOffset() const
{
	return m_ptWInfo->iOffset;
}

int clsLevelDBLogBufferIterWriter::GetBlockSize() const
{
	assert(NULL != m_ptWInfo);
	return m_ptWInfo->iBlockSize;
}

uint64_t clsLevelDBLogBufferIterWriter::GetCurrentBlkSeq() const
{
	assert(NULL != m_ptWInfo);
	return m_ptWInfo->oSeqGen.Next();
}

clsLevelDBLogReader::clsLevelDBLogReader()
	: m_ptRInfo(NULL)
{

}

clsLevelDBLogReader::~clsLevelDBLogReader()
{
	if (NULL != m_ptRInfo)
	{
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}
}


int clsLevelDBLogReader::OpenFile(
		const char* sLevelDBLogFile, const int iMaxDirectIOBufSize)
{
	if (NULL != m_ptRInfo)
	{
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	assert(NULL == m_ptRInfo);
	DirectIOReadInfo* ptRInfo = NULL;
	int ret = dbimpl::CreateADirectIOReadInfo(
			sLevelDBLogFile, iMaxDirectIOBufSize, ptRInfo);
	if (0 != ret)
	{
		assert(NULL == ptRInfo);
		return ret;
	}

	assert(0 == ret);
	assert(NULL != ptRInfo);
	m_sFileName = sLevelDBLogFile;

	m_ptRInfo = ptRInfo;
	assert(NULL != m_ptRInfo);
	assert(NULL != m_ptRInfo->pDirectIOBuffer);
	return 0;
}

int clsLevelDBLogReader::Read(std::string& sValue)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	uint32_t iOffset = 0;
	return dbimpl::DirectIORead(m_ptRInfo, sValue, iOffset);
}

int clsLevelDBLogReader::ReadSkipError(std::string& sValue, uint32_t& iOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	return dbimpl::DirectIOReadSkipError(m_ptRInfo, sValue, iOffset);
}


clsLevelDBLogPReader::clsLevelDBLogPReader()
	: m_ptRInfo(NULL)
{

}

clsLevelDBLogPReader::~clsLevelDBLogPReader()
{
	if (NULL != m_ptRInfo)
	{
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}
}

int clsLevelDBLogPReader::OpenFile(
		const char* sLevelDBLogFile, const int iMaxDirectIOBufSize)
{
	if (NULL != m_ptRInfo)
	{
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	assert(NULL == m_ptRInfo);
	DirectIOReadInfo* ptRInfo = NULL;
	int ret = dbimpl::CreateADirectIOReadInfo(
			sLevelDBLogFile, iMaxDirectIOBufSize, ptRInfo);
	if (0 != ret)
	{
		assert(NULL == ptRInfo);
		return ret;
	}

	assert(0 == ret);
	assert(NULL != ptRInfo);
	m_sFileName = sLevelDBLogFile;

	m_ptRInfo = ptRInfo;
	assert(NULL != m_ptRInfo);
	assert(NULL != m_ptRInfo->pDirectIOBuffer);
	return 0;
}

int clsLevelDBLogPReader::Read(
		const uint32_t iOffset, 
		std::string& sValue, uint32_t& iNextOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	return dbimpl::DirectIORead(m_ptRInfo, iOffset, sValue, iNextOffset);
}

int clsLevelDBLogPReader::ReadSkipError(
		const uint32_t iOffset, 
		std::string& sValue, uint32_t& iNextOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	return dbimpl::DirectIOReadSkipError(m_ptRInfo, iOffset, sValue, iNextOffset);
}

clsLevelDBLogAttachReader::clsLevelDBLogAttachReader()
	: m_ptRInfo(NULL)
{

}

clsLevelDBLogAttachReader::~clsLevelDBLogAttachReader()
{
	if (NULL != m_ptRInfo)
	{
		assert(dbimpl::CheckMode(m_ptRInfo->iMode, 
					dbimpl::READINFO_MODE_DELEGATE_READONLY));
		m_ptRInfo->pDirectIOBuffer = NULL;
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}
}

int clsLevelDBLogAttachReader::Attach(
		const char* pBegin, const char* pEnd, uint32_t iOffset)
{
	if (NULL != m_ptRInfo)
	{
		assert(dbimpl::CheckMode(m_ptRInfo->iMode, 
					dbimpl::READINFO_MODE_DELEGATE_READONLY));
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	assert(NULL != pBegin);
	assert(NULL != pEnd);
	assert(pBegin <= pEnd);
	dbimpl::DirectIOReadInfo* ptNewRInfo = NULL;
	int ret = 
		dbimpl::CreateADirectIOReadInfoReadOnly(pBegin, pEnd, iOffset, ptNewRInfo);
	if (0 != ret)
	{
		return ret;
	}

	swap(m_ptRInfo, ptNewRInfo);
	assert(NULL != m_ptRInfo);
	assert(NULL == ptNewRInfo);
	return 0;
}

int clsLevelDBLogAttachReader::Read(std::string& sValue, uint32_t& iOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	int ret = dbimpl::DirectIORead(m_ptRInfo, sValue, iOffset);
	ret = DB_READ_ERROR_INVALID_FD == ret ? 1 : ret;
	return ret;
}

int clsLevelDBLogAttachReader::ReadSkipError(
		std::string& sValue, uint32_t& iOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	int ret = dbimpl::DirectIOReadSkipError(m_ptRInfo, sValue, iOffset);
	// DB_READ_ERROR_INVALID_FD => indicate EOF here
	ret = DB_READ_ERROR_INVALID_FD == ret ? 1 : ret;
	return ret;
}

int clsLevelDBLogAttachReader::GetBlockSize() const
{
	assert(NULL != m_ptRInfo);
	return m_ptRInfo->iBlockSize;
}

uint64_t clsLevelDBLogAttachReader::GetStartBlkSeq() const
{
	assert(NULL != m_ptRInfo);
	return m_ptRInfo->llStartBlkSeq;
}


clsLevelDBLogBufferPReader::clsLevelDBLogBufferPReader(const int iMaxDirectIOBufSize)
	: m_iMaxDirectIOBufSize(iMaxDirectIOBufSize)
	, m_pDirectIOBuffer(NULL)
	, m_ptRInfo(NULL)
{
	assert(0 < iMaxDirectIOBufSize);
}

clsLevelDBLogBufferPReader::~clsLevelDBLogBufferPReader()
{
	if (NULL != m_ptRInfo)
	{
		// delegate:
		assert(dbimpl::CheckMode(m_ptRInfo->iMode, 
					dbimpl::READINFO_MODE_DELEGATE));
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	if (NULL != m_pDirectIOBuffer)
	{
		free(m_pDirectIOBuffer);
		m_pDirectIOBuffer = NULL;
	}
}

int clsLevelDBLogBufferPReader::OpenFile(
		const char* sLevelDBLogFile, bool bCheckBlock)
{
	if (NULL != m_ptRInfo)
	{
		assert(dbimpl::CheckMode(m_ptRInfo->iMode, 
					dbimpl::READINFO_MODE_DELEGATE));
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	if (NULL == m_pDirectIOBuffer)
	{
		m_pDirectIOBuffer = dbimpl::AllocDirectIOBuffer(m_iMaxDirectIOBufSize);
	}

	assert(NULL == m_ptRInfo);
	assert(NULL != m_pDirectIOBuffer);
	dbimpl::DirectIOReadInfo* ptRInfo = NULL;
	int ret = dbimpl::CreateADirectIOReadInfo(
			sLevelDBLogFile, m_pDirectIOBuffer, m_iMaxDirectIOBufSize, ptRInfo);
	if (0 != ret)
	{
		assert(NULL == ptRInfo);
		return ret;
	}

	if (bCheckBlock) 
	{
		dbimpl::EnableCheckBlockMode(ptRInfo);
		if (!dbimpl::CheckCurrentBlock(ptRInfo))
		{
			return DB_FORMAT_ERROR_BROKEN_BLK;
		}
	}

	swap(m_ptRInfo, ptRInfo);
	assert(NULL != m_ptRInfo);
	assert(NULL == ptRInfo);
	return 0;
}

int clsLevelDBLogBufferPReader::SkipOneLevelDBRecord(uint32_t& iNextOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	dbimpl::RLogLevelDBRecord tLogRecord = {0};
	string sDropValue;
	uint32_t iOldOffset = m_ptRInfo->iOffset;
	int ret = dbimpl::DirectIORead(m_ptRInfo, tLogRecord, sDropValue);
	if (0 != ret)
	{
		return ret;
	}

	// success read
	iNextOffset = m_ptRInfo->iOffset;
	assert(iOldOffset < iNextOffset);
	logdebug("skip iOldOffset %u iNextOffset %u recordtype %u",
			iOldOffset, iNextOffset, tLogRecord.cRecordType);
	return 0;
}

//int clsLevelDBLogBufferPReader::SkipErrorBlock(uint32_t& iNextOffset)
//{
//	if (NULL == m_ptRInfo)
//	{
//		return -1;
//	}
//
//	dbimpl::SkipErrorBlock(m_ptRInfo);
//	iNextOffset = m_ptRInfo->iOffset;
//	return 0;
//}


int clsLevelDBLogBufferPReader::CloseFile()
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	return dbimpl::CloseFile(m_ptRInfo);
}

int clsLevelDBLogBufferPReader::Read(
		const uint32_t iOffset, std::string& sValue, uint32_t& iNextOffset)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);
	return dbimpl::DirectIORead(m_ptRInfo, iOffset, sValue, iNextOffset);
}

int clsLevelDBLogBufferPReader::GetBlockSize() const
{
	assert(NULL != m_ptRInfo);
	return m_ptRInfo->iBlockSize;
}

void clsLevelDBLogBufferPReader::PrintDirectIOBuffer()
{
	assert(NULL != m_ptRInfo);
	return dbimpl::PrintDirectIOBuffer(m_ptRInfo);
}


int ReadMetaInfo(
		const char* sLevelDBLogFile, 
		const int iDirectIOBufSize, 
		int& iBlockSize, uint64_t& llBlkSeq, uint32_t& iOffset)
{
	if (NULL == sLevelDBLogFile || 0 > iDirectIOBufSize)
	{
		return -1;
	}

	return dbimpl::ReadMetaInfo(
			sLevelDBLogFile, iDirectIOBufSize, iBlockSize, llBlkSeq, iOffset);
}

int ReadFileMetaInfo(
		const char* sFileName, int& iBlockSize, uint64_t& llStartBlkSeq)
{
	if (NULL == sFileName)
	{
		return -1;
	}

	return dbimpl::ReadFileMetaInfo(sFileName, iBlockSize, llStartBlkSeq);
}

int WriteFileMetaInfo(
		const char* sFileName, 
		const int iBlockSize, const uint64_t llStartBlkSeq)
{
	if (NULL == sFileName || 0 > iBlockSize || 0 == llStartBlkSeq)
	{
		return -1;
	}

	return dbimpl::WriteFileMetaInfo(sFileName, iBlockSize, llStartBlkSeq);
}

int DeprecateAllDataBlock(const char* sFileName)
{
	if (NULL == sFileName)
	{
		return -1;
	}

	return dbimpl::DeprecateAllDataBlock(sFileName);
}

int IsLevelDBLogFormat(const char* sFileName)
{
	if (NULL == sFileName)
	{
		return -1;
	}

	return dbimpl::IsLevelDBLogFormatNormalIO(sFileName);
}

} // namespace dbcomm

