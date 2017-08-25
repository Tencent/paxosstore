
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <cstring>
#include <cassert>
#include "bitcask_log.h"
#include "bitcask_log_impl.h"
#include "db_comm.h"
#include "cutils/log_utils.h"


using namespace std;
using dbcomm::INVALID_FD;

static const uint32_t MAX_LOG_FILE_SIZE = 1 * 1024 * 1024 * 1024; // 1GB

static const std::string MERGE_POSTFIX = ".m";
static const std::string WRITE_POSTFIX = ".w";

namespace {


int OpenAt(
		const std::string sKvLogPath, 
		const int iFileNo, 
		const bool bIsMergeWrite, 
		int& iFD, 
		uint32_t& iOffset)
{
	string sFileName;
	int ret = dbcomm::DumpToFileName(iFileNo, 
			bIsMergeWrite ? MERGE_POSTFIX : WRITE_POSTFIX, sFileName);
	if (0 != ret)
	{
		return -2;
	}

	sFileName = dbcomm::ConcatePath(sKvLogPath, sFileName);
	iFD = open(sFileName.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0666);
	if (0 > iFD)
	{
		return -2;
	}

	int iFileSize = dbcomm::GetFileSize(sFileName.c_str());
	if (0 > iFileSize)
	{
		close(iFD);
		return -3;
	}
	
	iOffset = iFileSize;
	return 0;
}

int OpenAt(
		const std::string& sKvLogPath, 
		const int iFileNo, 
		const uint32_t iOffset, 
		int& iFD)
{
	assert(0 < iFileNo);
	
	std::string sFileName;
	int ret = dbcomm::DumpToFileName(iFileNo, WRITE_POSTFIX, sFileName);
	if (0 != ret) {
		return -1;
	}

	assert(0 == ret);
	sFileName = dbcomm::ConcatePath(sKvLogPath, sFileName);
	int iNewFD = open(sFileName.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0666);
	if (0 > iNewFD) {
		return -2;
	}

	assert(0 <= iNewFD);
	int iFileSize = dbcomm::GetFileSize(sFileName.c_str());
	if (0 > iFileSize || static_cast<uint32_t>(iFileSize) > iOffset) {
		close(iNewFD);
		return -3;
	}

	iFD = iNewFD;
	return 0;
}


} // 


namespace dbcomm {


clsBitCaskLogReader::clsBitCaskLogReader()
	: m_iFD(INVALID_FD)
	, m_iOffset(0)
{

}

clsBitCaskLogReader::~clsBitCaskLogReader()
{
	if (INVALID_FD != m_iFD)
	{
		close(m_iFD);
		m_iFD = INVALID_FD;
	}
}


int clsBitCaskLogReader::OpenFile(const char* sBitCaskLogFile)
{
	if (INVALID_FD != m_iFD)
	{
		close(m_iFD);
		m_iFD = INVALID_FD;
	}

	int iNewFD = open(sBitCaskLogFile, O_RDONLY);
	if (0 > iNewFD)
	{
		logerr("open %s ret %d strerror %s", 
				sBitCaskLogFile, iNewFD, strerror(errno));
		return -1;
	}

	swap(m_iFD, iNewFD);
	assert(INVALID_FD != m_iFD);
	assert(INVALID_FD == iNewFD);
	m_iOffset = 0;
	m_sFileName = sBitCaskLogFile;
	return 0;
}

int clsBitCaskLogReader::Read(std::string& sRawRecord)
{
	if (INVALID_FD == m_iFD)
	{
		return -1;
	}

	uint32_t iNextOffset = 0;
	int ret = dbimpl::ReadARawRecord(
			m_iFD, m_iOffset, sRawRecord, iNextOffset);
	if (0 != ret)
	{
		return ret;
	}

	assert(0 == ret);
	assert(m_iOffset < iNextOffset);
	m_iOffset = iNextOffset;
	return 0;
}

int clsBitCaskLogReader::ReadSkipError(std::string& sRawRecord, uint32_t& iOffset)
{
	if (INVALID_FD == m_iFD)
	{
		return -1;
	}

	uint32_t iNextOffset = 0;
	int ret = dbimpl::ReadARawRecordSkipError(
			m_iFD, m_iOffset, sRawRecord, iNextOffset);
	if (0 != ret)
	{
		return ret;
	}

	assert(0 == ret);
	assert(m_iOffset < iNextOffset);
	iOffset = m_iOffset;
	m_iOffset = iNextOffset;
	return 0;
}


clsBitCaskLogBufferIterWriter::clsBitCaskLogBufferIterWriter(
		const char* sKvLogPath, bool bIsMergeWrite)
	: m_sKvLogPath(sKvLogPath)
	, m_bIsMergeWrite(bIsMergeWrite)
	, m_iMaxBufSize(0)
	, m_iFileNo(0)
	, m_iOffset(0)
	, m_iFD(INVALID_FD)
{

}

clsBitCaskLogBufferIterWriter::~clsBitCaskLogBufferIterWriter()
{
	if (INVALID_FD != m_iFD)
	{
		close(m_iFD);
		m_iFD = INVALID_FD;
	}
}


int clsBitCaskLogBufferIterWriter::Init(int iMaxBufSize, uint32_t iFileNo)
{
	assert(0 <= iMaxBufSize);
	assert(0 < iFileNo);
	if (INVALID_FD != m_iFD)
	{
		close(m_iFD);
	}

	int iNewFD = INVALID_FD;
	uint32_t iNewOffset = 0;
	int ret = OpenAt(m_sKvLogPath, iFileNo, m_bIsMergeWrite, iNewFD, iNewOffset);
	if (0 != ret)
	{
		logerr("OpenAt iFileNo %u ret %d", iFileNo, ret);
		return -1;
	}

	m_iMaxBufSize = iMaxBufSize;
	m_iFileNo = iFileNo;
	m_iFD = iNewFD;
	m_iOffset = iNewOffset;
	return 0;
}


int clsBitCaskLogBufferIterWriter::Write(
		const char* pValue, int iValLen, 
		uint32_t& iFileNo, uint32_t& iOffset)
{
	// normally: only expect 1 loop
	if (0 < m_iOffset && m_iOffset + iValLen > MAX_LOG_FILE_SIZE)
	{
		int ret = IterIntoNextFile();
		if (0 != ret)
		{
			return -1;
		}

		assert(INVALID_FD != m_iFD);
	}

	int iWriteSize = 0;

	iFileNo = m_iFileNo;
	iOffset = m_iOffset;
	if (0 == m_iMaxBufSize)
	{
		iWriteSize = dbcomm::SafeWrite(m_iFD, pValue, iValLen);	
		if (iWriteSize != iValLen)
		{
			if (iWriteSize > 0)
			{
				m_iOffset += iWriteSize;
			}
			return -2;
		}

		m_iOffset += iValLen;
		return 0;
	}

	m_sWriteBuffer.append(pValue, iValLen);
	m_iOffset += iValLen;
	if (m_sWriteBuffer.size() > static_cast<size_t>(m_iMaxBufSize))
	{
		return Flush();	
	}

	return 0;
}

int clsBitCaskLogBufferIterWriter::Flush()
{
	if (m_sWriteBuffer.empty())
	{
		return 0; // do nothing
	}

	if (INVALID_FD == m_iFD)
	{
		return -1;
	}

	int iWriteSize = dbcomm::SafeWrite(m_iFD, m_sWriteBuffer.data(), m_sWriteBuffer.size());
	if (static_cast<size_t>(iWriteSize) != m_sWriteBuffer.size())
	{
		return -2;
	}

	m_sWriteBuffer.clear();
	return 0;
}

int clsBitCaskLogBufferIterWriter::IterIntoNextFile()
{
	int ret = Flush();
	if (0 != ret)
	{
		return -1;
	}

	if (INVALID_FD != m_iFD)
	{
		close(m_iFD);
	}

	int iNewFD = INVALID_FD;
	uint32_t iNewOffset = 0;
	ret = OpenAt(m_sKvLogPath, m_iFileNo + 1, m_bIsMergeWrite, iNewFD, iNewOffset);
	if (0 != ret)
	{
		return -2;
	}

	m_iFD = iNewFD;
	m_iOffset = iNewOffset;
	++m_iFileNo;
	return 0;
}


clsBitCaskIterWriter::clsBitCaskIterWriter(const char* sKvLogPath)
	: m_sKvLogPath(sKvLogPath)
	, m_iFileNo(0)
	, m_iOffset(0)
	, m_iFD(-1)
{
	assert(NULL != sKvLogPath);
}


clsBitCaskIterWriter::~clsBitCaskIterWriter()
{
	if (0 <= m_iFD) {
		close(m_iFD);
		m_iFD = -1;
	}

	assert(-1 == m_iFD);
}


int clsBitCaskIterWriter::Init(int iFileNo, uint32_t iOffset)
{
	assert(iOffset <= 1 * 1024 * 1024 * 1024);
	assert(-1 == m_iFD);
	assert(0 == m_iFileNo);
	assert(0 == m_iOffset);
	assert(0 < iFileNo);

	int iNewFD = -1;
	int ret = OpenAt(m_sKvLogPath, iFileNo, iOffset, iNewFD);
	if (0 != ret) {
		logerr("OpenAt %s iFileNo %u iOffset %u ret %d", 
				m_sKvLogPath.c_str(), iFileNo, iOffset, ret);
		assert(0 > ret);
		return ret;
	}

	assert(0 == ret);
	assert(0 <= iNewFD);

	m_iFileNo = iFileNo;
	m_iOffset = iOffset;
	m_iFD = iNewFD;
	return 0;
}

int clsBitCaskIterWriter::IterIntoNextFile()
{
	if (0 <= m_iFD) {
		close(m_iFD);
		m_iFD = -1;
	}

	assert(-1 == m_iFD);

	int iNewFD = -1;
	int ret = OpenAt(m_sKvLogPath, m_iFileNo + 1, 0, iNewFD);
	if (0 != ret) {
		logerr("OpenAt %s fileno %u 0 ret %d", 
				m_sKvLogPath.c_str(), m_iFileNo + 1, ret);
		assert(0 > ret);
		return ret;
	}

	assert(0 <= iNewFD);
	m_iFileNo += 1;
	m_iOffset = 0;
	m_iFD = iNewFD;
	return 0;
}

int clsBitCaskIterWriter::Write(
		const char* pValue, int iValLen, 
		int& iFileNo, uint32_t& iOffset)
{
	if (NULL == pValue || 0 >= iValLen) {
		return -1;
	}

	if (-1 == m_iFD) {
		return -2;
	}

	assert(0 <= m_iFD);
	assert(0 < m_iFileNo);
	if (0 < m_iOffset && m_iOffset + iValLen > MAX_LOG_FILE_SIZE) {
		int ret = IterIntoNextFile();
		if (0 != ret) {
			logerr("IterIntoNextFile ret %d", ret);
			assert(0 > ret);
			return ret;
		}

		assert(0 == ret);
		assert(0 <= m_iFD);
		assert(0 == m_iOffset);
	}

	assert(0 <= m_iFD);
	if (0 >= static_cast<int>(m_iFileNo)) {
		return -3;
	}

	iFileNo = m_iFileNo;
	iOffset = m_iOffset;
	assert(0 < iFileNo);
	int iWriteSize = dbcomm::SafeWrite(m_iFD, pValue, iValLen);
	if (iWriteSize != iValLen) {
		logerr("dbcomm::SafeWrite %s m_iFD %d iFileNo %u iValLen %u ret %d", 
				m_sKvLogPath.c_str(), m_iFD, m_iFileNo, iValLen, iWriteSize);
		return -2;
	}

	assert(0 < iWriteSize);
	assert(iWriteSize == iValLen);
	m_iOffset += iValLen;
	return 0;
}


std::string clsBitCaskIterWriter::GetFileName(const int iFileNo)
{
	assert(0 != iFileNo);

	const bool bIsMergeFile = 0 > iFileNo;

	std::string sFileName;
	int ret = dbcomm::DumpToFileName(
			bIsMergeFile ? -iFileNo : iFileNo, 
			bIsMergeFile ? MERGE_POSTFIX : WRITE_POSTFIX, 
			sFileName);
	assert(0 == ret);

	sFileName = dbcomm::ConcatePath(m_sKvLogPath, sFileName);
	return sFileName;
}



int ReadRecord(
		const std::string& sFileName, uint32_t iOffset, std::string& value)
{
	assert(false == sFileName.empty());

	errno = 0;
	int iFD = open(sFileName.c_str(), O_RDONLY);
	if (0 > iFD) {
		logerr("open %s err %s", sFileName.c_str(), strerror(errno));
		return -1;
	}

	assert(0 <= iFD);

	uint32_t iNextOffset = 0;
	auto ret = dbimpl::ReadARawRecord(iFD, iOffset, value, iNextOffset);
    close(iFD);
    return ret;
}


}  // namespace dbcomm


