
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <sstream>
#include <cstring>
#include <cstdlib>
#include "leveldb_log_impl.h"
#include "bitcask_log_impl.h"
#include "db_comm.h"
#include "cutils/log_utils.h"
#include "cutils/hassert.h"

// dep: leveldb
#include "3rd/log_format.h"
#include "leveldb/slice.h"
// #include "3rd/coding.h"
#include "3rd/crc32c.h"


#if defined(__APPLE__)
#define memalign(a, b) malloc(b)
#else
#include <malloc.h>
#endif

//#define DEFAULT_BLOCK_SIZE 4 * 1024
#define MAX_SMALL_BUFFER_READ_FREQ 100


#define TRY_FIX_READINFO_IF_ERROR(ptInfo)               \
{                                                       \
	if (false == IsValid(ptInfo))                       \
	{                                                   \
		int ret = FixDirectIOReadInfo(ptInfo);          \
		UpdateReadStatus(ptInfo, ret);                  \
		if (0 >= ret)                                   \
		{                                               \
			return 0 == ret ? 1 : ret;                  \
		}                                               \
                                                        \
		assert(ret == ptInfo->iBlockSize);              \
	}                                                   \
}                                                       


using namespace std;
using dbcomm::INVALID_FD;
using dbcomm::SafePRead;
using dbcomm::SafeDirectIOPRead;
using dbcomm::SafeDirectIOPWrite;

namespace dbimpl {

const int kBlockMetaInfoRecordSize = 
	leveldb::log::kHeaderSize + sizeof(LogBlockMetaInfo);

const uint64_t INVALID_BLK_SEQ = 0;

static const uint32_t MAX_LOG_FILE_SIZE = 1 * 1024 * 1024 * 1024; // 1GB
static const int DEFAULT_BLOCK_SIZE = 4 * 1024;
static const uint32_t MIN_LOG_RECORD_SIZE = leveldb::log::kHeaderSize;
const uint64_t MAX_BLK_SEQ_INC_PER_FILE = 
	(static_cast<uint64_t>(MAX_LOG_FILE_SIZE) / DEFAULT_BLOCK_SIZE) * 2;


namespace {


void UpdateReadStatus(DirectIOReadInfo* ptInfo, int ret)
{
	assert(NULL != ptInfo);
	if (0 < ret)
	{
		ptInfo->iReadStatus = 0;
		return ;
	}

	ptInfo->iReadStatus = 0 == ret ? 1 : ret;
}

bool IsAddrAlign(const char* pAddress, const int iAlignBoundary)
{
	if (0 == iAlignBoundary)
	{
		return true; // no align boundary
	}

	int iNumOfTailZero = __builtin_ctz(iAlignBoundary);
	return 0 == (reinterpret_cast<int64_t>(pAddress) & ((1 << iNumOfTailZero) - 1));
}

inline int UpperAlignment(const int iOffset, const int iBlockSize)
{
	assert(0 < iBlockSize);
	return (iOffset + iBlockSize - 1) / iBlockSize * iBlockSize;
}

inline int LowerAlignment(const int iOffset, const int iBlockSize)
{
	assert(0 < iBlockSize);
	return iOffset / iBlockSize * iBlockSize;
}

template <typename DirectIOInfo>
inline char* GetNextBlockPosImpl(DirectIOInfo* ptInfo, char* pBuffer)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);
	assert(ptInfo->pDirectIOBuffer <= pBuffer);
	int iBlkOffset = (pBuffer - ptInfo->pDirectIOBuffer) % ptInfo->iBlockSize;
	if (0 != iBlkOffset)
	{
		return pBuffer - iBlkOffset + ptInfo->iBlockSize;
	}

	return pBuffer == 
		GetDirectBufferEnd(ptInfo) ? pBuffer : pBuffer + ptInfo->iBlockSize;
}

template <typename DirectIOInfo>
inline const char* GetNextBlockPosImpl(
		const DirectIOInfo* ptInfo, const char* pBuffer)
{
	return GetNextBlockPosImpl(
			const_cast<DirectIOInfo*>(ptInfo), const_cast<char*>(pBuffer));
}

void ConcateCStrPath(
		const std::string& sPath, 
		const char* sFileName, int iFileNameLen, 
		std::string& sAbsFileName)
{
	assert(NULL != sFileName);
	assert(0 < iFileNameLen);
	assert(false == sPath.empty());
	if ('/' == sPath[sPath.size()-1])
	{
		sAbsFileName.reserve(sPath.size() + iFileNameLen + 1);
		sAbsFileName.append(sPath);
		sAbsFileName.append(sFileName, iFileNameLen);
		return ;
	}

	sAbsFileName.reserve(sPath.size() + iFileNameLen + 2);
	sAbsFileName.append(sPath);
	sAbsFileName.append(1, '/');
	sAbsFileName.append(sFileName, iFileNameLen);
	return ;
}

void ConcateStrPath(
		const std::string& sPath, 
		const std::string& sFileName, 
		std::string& sAbsFileName)
{
	assert(false == sPath.empty());
	assert(false == sFileName.empty());
	if ('/' == sPath[sPath.size()-1])
	{
		sAbsFileName.reserve(sPath.size() + sFileName.size() + 1);
		sAbsFileName.clear();
		sAbsFileName.append(sPath);
		sAbsFileName.append(sFileName);
		return ;
	}

	sAbsFileName.reserve(sPath.size() + sFileName.size() + 2);
	sAbsFileName.clear();
	sAbsFileName.append(sPath);
	sAbsFileName.append(1, '/');
	sAbsFileName.append(sFileName);
	return ;
}

int GetOneRecycleFile(
		const std::string& sKvRecyclePath, 
		std::string& sRecycleFile)
{
	string sFileName;
	int ret = dbcomm::GetFirstRegularFile(sKvRecyclePath, sFileName);
	if (0 != ret)
	{
		return ret;
	}

	ConcateStrPath(sKvRecyclePath, sFileName, sRecycleFile);
	return 0;
}

int AdjustBlkSeq(
		DirectIOWriteInfo* ptInfo, 
		const std::string& sAbsFileName)
{
	int iFileBlockSize = 0;
	uint64_t llFileStartBlkSeq = 0;
	int ret = ReadFileMetaInfo(
			sAbsFileName.c_str(), iFileBlockSize, llFileStartBlkSeq);
	if (0 != ret)
	{
		// 0 < ret => old or empty file, adjust nothing
		// 0 > ret => error case: 
		//         => hope the next adjust retry will success
		return 0 < ret ? 0 : ret;
	}

	uint64_t llPrevBlkSeq = ptInfo->oSeqGen.GetBlkSeq();
	ptInfo->oSeqGen.Adjust(llFileStartBlkSeq);
	logdebug("llPrevBlkSeq %lu llFileStartBlkSeq %lu llBlkSeq %lu", 
			llPrevBlkSeq, llFileStartBlkSeq, ptInfo->oSeqGen.GetBlkSeq());
	return 0;
}

int TryRecycleOneFile(
		DirectIOWriteInfo* ptInfo, 
		const std::string& sAbsDBFileName, 
		const std::string& sKvRecyclePath)
{
	if (sKvRecyclePath.empty())
	{
		return -1;
	}

	// => try to recycle
	// => mv to recycle stage in merge write guarantee all data block
	// in recycle files are deprecated!
	
	// read first block => adjust Blk Seq of WriteInfo => rename
	// => rename may failed due to merge/write compete or recycle delete
	string sAbsRecycleFileName;	
	int ret = GetOneRecycleFile(sKvRecyclePath, sAbsRecycleFileName);
	logdebug("GetOneRecycleFile ret %d sAbsRecycleFileName %s", 
			ret, sAbsRecycleFileName.c_str());
	if (0 != ret)
	{
		logerr("GetOneRecycleFile ret %d", ret);
		return -2;
	}

	// success find one available file
	// read adjust, and rename
	ret = AdjustBlkSeq(ptInfo, sAbsRecycleFileName);
	if (0 != ret)
	{
		logerr("AdjustBlkSeq ret %d", ret);
		return -3;
	}

	// try to rename
	ret = rename(sAbsRecycleFileName.c_str(), sAbsDBFileName.c_str());
	if (0 != ret)
	{
		logerr("rename %s %s ret %d strerror %s", 
				sAbsRecycleFileName.c_str(), sAbsDBFileName.c_str(), 
				ret, strerror(errno));
		return -4;
	}

	return 0;
}



} // namespace

bool CheckMode(int iMode, int iExpectedMode)
{
	return iExpectedMode & iMode;
}

bool IsDeprecateBlock(uint64_t llStartBlkSeq, uint64_t llBlkSeq)
{
	assert(INVALID_BLK_SEQ != llStartBlkSeq);
	return llStartBlkSeq >= llBlkSeq;
}

bool IsLevelDBFormatError(int iRetCode)
{
	return -100 >= iRetCode && -1000 <= iRetCode;
}

// IMPORTANT: not direct io for mac !!
int OpenForDirectIOWrite(const char* sFileName)
{
#if defined(__APPLE__)
    return open(sFileName, O_CREAT | O_WRONLY, 0666);
#else
	return open(sFileName, O_CREAT | O_WRONLY | O_DIRECT, 0666);
#endif
}

int OpenForDirectIORead(const char* sFileName)
{
#if defined(__APPLE__)
    return open(sFileName, O_RDONLY);
#else
	return open(sFileName, O_RDONLY | O_DIRECT);
#endif
}

char* AllocDirectIOBuffer(const int iDirectIOBufSize)
{
	static const int iPageSize = getpagesize();
	assert(0 < iDirectIOBufSize);
	assert(iPageSize <= iDirectIOBufSize);
	char* pDirectIOBuffer = 
		reinterpret_cast<char*>(memalign(iPageSize, iDirectIOBufSize));
	assert(NULL != pDirectIOBuffer);
	return pDirectIOBuffer;
}

char* DumpFileMetaInfo(
		const LogFileMetaInfo& tFileMetaInfo, 
		char* pBlkBegin, 
		char* const pBlkEnd)
{
	assert(pBlkEnd - pBlkBegin == tFileMetaInfo.iBlockSize);

	dbimpl::Record_t tRecord = {0};
	tRecord.cFlag = 0xFF;
	tRecord.iValLen = sizeof(LogFileMetaInfo);
	tRecord.pVal = const_cast<char*>(reinterpret_cast<const char*>(&tFileMetaInfo));
	
	int iRecordSize = dbimpl::CalculateRecordSize(tRecord);	
	assert(0 < iRecordSize && iRecordSize < tFileMetaInfo.iBlockSize);
	
	char* pNextBuffer = dbimpl::ToBufferImpl(
			tRecord, iRecordSize, pBlkBegin, pBlkEnd - pBlkBegin);
	assert(pNextBuffer == pBlkBegin + iRecordSize);
	return pNextBuffer;
}

int TryPickleFileMetaInfo(
		const char* pBlkBegin, 
		const char* pBlkEnd, 
		LogFileMetaInfo& tFileMetaInfo)
{
	dbimpl::Record_t tRecord = {0};
	{
		// BUG: iRecordSize >= pBlkEnd - pBlkBegin
		// => only check head!!
		const BitCaskRecord::block_head_t* 
			pPtrHead = BitCaskRecord::MakeBitCaskRecordHeadPtr(pBlkBegin);
		assert(NULL != pPtrHead);
		if (false == BitCaskRecord::IsAValidRecordHead(pBlkBegin, pBlkEnd))
		{
			logerr("invalid record head");
			return DB_FORMAT_ERROR_BROKEN_BC_RECORD;
		}

		// valid head
		logdebug("0xFF %u pPtrHead->cFlag %u", 
				0xFF, static_cast<uint8_t>(pPtrHead->cFlag));
		if (0xFF != static_cast<uint8_t>(pPtrHead->cFlag))
		{
			return 1; // old bit-cask file format;
		}

		// else => new leveldb log format;
		// => [pBlkBegin, pBlkEnd) enough for pickle a record
		int iRecordSize = 0;
		int ret = dbimpl::ToRecordImpl(pBlkBegin, pBlkEnd, tRecord, iRecordSize);
		if (0 != ret)
		{
			logerr("ToRecordImpl ret %d", ret);
			return DB_FORMAT_ERROR_BROKEN_BC_RECORD;
		}
	}

	// definely a leveldb log format
	assert(0xFF == static_cast<uint8_t>(tRecord.cFlag));
	if (sizeof(tFileMetaInfo) != static_cast<size_t>(tRecord.iValLen))
	{
		return DB_FORMAT_ERROR_BROKEN_FILEMETA;
	}

	memcpy(&tFileMetaInfo, tRecord.pVal, sizeof(LogFileMetaInfo));	
	return 0;
}

int IsLevelDBLogFormat(const char* sFileName, int iBlockSize)
{
	int iFD = OpenForDirectIORead(sFileName);
	if (0 > iFD)
	{
		logerr("OpenForDirectIORead %s ret %d %s", sFileName, iFD, strerror(errno));
		return -1;
	}

	assert(0 <= iFD);
	CREATE_FD_MANAGER(iFD);
	char* pReadBuf = AllocDirectIOBuffer(iBlockSize);
	if (NULL == pReadBuf)
	{
		return -2;
	}

	CREATE_MALLOC_MEM_MANAGER(pReadBuf);
	int iReadSize = SafePRead(iFD, pReadBuf, iBlockSize, 0);
	if (iReadSize != iBlockSize)
	{
		if (0 > iReadSize)
		{
			logerr("SafePRead iBlockSize %d ret %d", iBlockSize, iReadSize);
			return -3;
		}

		return 0; // else => not a level db log format
	}

	LogFileMetaInfo tFileMetaInfo = {0};
	int ret = TryPickleFileMetaInfo(
			pReadBuf, pReadBuf + iBlockSize, tFileMetaInfo);
	if (0 > ret)
	{
		logerr("TryPickleFileMetaInfo ret %d", ret);
		return -3;
	}

	return 1 == ret ? 0 : 1;
}

int IsLevelDBLogFormat(const char* sFileName)
{
	return IsLevelDBLogFormat(sFileName, getpagesize());
}

int IsLevelDBLogFormatNormalIO(const char* sFileName, int iBlockSize)
{
	int iFD = open(sFileName, O_RDONLY);
	if (0 > iFD)
	{
		logerr("open %s ret %d %s", sFileName, iFD, strerror(errno));
		return -1;
	}

	assert(0 <= iFD);
	CREATE_FD_MANAGER(iFD);
	char* pReadBuf = reinterpret_cast<char*>(malloc(iBlockSize));
	if (NULL == pReadBuf)
	{
		return -2;
	}

	CREATE_MALLOC_MEM_MANAGER(pReadBuf);
	int iReadSize = SafePRead(iFD, pReadBuf, iBlockSize, 0);
	if (iReadSize != iBlockSize)
	{
		if (0 > iReadSize)
		{
			logerr("SafePRead iBlockSize %d ret %d", iBlockSize, iReadSize);
			return -3;
		}

		return 0; // else => not a level db log format
	}

	LogFileMetaInfo tFileMetaInfo = {0};
	int ret = TryPickleFileMetaInfo(
			pReadBuf, pReadBuf + iBlockSize, tFileMetaInfo);
	if (0 > ret)
	{
		logerr("TryPickleFileMetaInfo ret %d", ret);
		return -3;
	}

	return 1 == ret ? 0 : 1;
}

int IsLevelDBLogFormatNormalIO(const char* sFileName)
{
	return IsLevelDBLogFormatNormalIO(sFileName, getpagesize());
}

const char* PickleBlockMetaInfo(
		const char* pBuffer, 
		int iBufLen, 
		LogBlockMetaInfo& tBlkMetaInfo)
{
	assert(static_cast<size_t>(iBufLen) >= sizeof(LogBlockMetaInfo));
	memcpy(&tBlkMetaInfo, pBuffer, sizeof(LogBlockMetaInfo));
	return pBuffer + sizeof(LogBlockMetaInfo);
}

uint32_t CalculateCRC(const RLogLevelDBRecord& tLogRecord)
{
	assert(0 <= tLogRecord.hLength);

	char buf[leveldb::log::kHeaderSize] = {0};
	buf[4] = static_cast<char>(tLogRecord.hLength & 0xFF);
	buf[5] = static_cast<char>(tLogRecord.hLength >> 8);
	buf[6] = static_cast<char>(tLogRecord.cRecordType);

	// crc: + type
	uint32_t crc = leveldb::crc32c::Value(buf+6, 1);
	// crc: + value
	crc = leveldb::crc32c::Extend(crc, tLogRecord.pValue, tLogRecord.hLength);
	crc = leveldb::crc32c::Mask(crc);
    dbcomm::Encode32Bit(buf, crc);
	return crc;
}

char* DumpLevelDBRecord(
		const RecordType t, 
		const char* pRecValue, 
		int iRecValLen, 
		char* pBuffer, 
		char* const pBlkEnd)
{
	assert(0 <= iRecValLen && 0xFFFF >= iRecValLen);
	assert(pBuffer < pBlkEnd);
	assert(pBlkEnd - pBuffer >= leveldb::log::kHeaderSize + iRecValLen);
	assert(0 <= t && kMaxRecordType > t);

	char buf[leveldb::log::kHeaderSize];
	buf[4] = static_cast<char>(iRecValLen & 0xFF);
	buf[5] = static_cast<char>(iRecValLen >> 8);
	buf[6] = static_cast<char>(t);

	// crc: + type
	uint32_t crc = leveldb::crc32c::Value(buf+6, 1);
	// crc: + value
	crc = leveldb::crc32c::Extend(crc, pRecValue, iRecValLen);
	crc = leveldb::crc32c::Mask(crc);
    dbcomm::Encode32Bit(buf, crc);

	memcpy(pBuffer, buf, sizeof(buf));
	pBuffer += sizeof(buf);
	if (0 < iRecValLen)
	{
		assert(NULL != pRecValue);
		memcpy(pBuffer, pRecValue, iRecValLen);
	}

	return pBuffer + iRecValLen;
}

// when a invalid leveldb record income, PickleLevelDBRecord will
// return pNextBlockInBuffer: whole block contain invalid leveldb record
// will be ignore.
const char* PickleLevelDBRecord(
		const char* pBuffer, 
		const char* const pBlkEnd, 
		RLogLevelDBRecord& tLogRecord, 
		bool bCheckCRC)
{
	assert(pBuffer < pBlkEnd);

	int iBufLen = pBlkEnd - pBuffer;
	tLogRecord.pValue = NULL;
	assert(0 < iBufLen);
	if (iBufLen < leveldb::log::kHeaderSize)
	{
		tLogRecord.hLength = 0;
		tLogRecord.cRecordType = kZeroType;
		return pBlkEnd;
	}

	const char* header = pBuffer;
	const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
	const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
	const unsigned int type = header[6];
	const uint32_t length = a | (b << 8);

	if (kZeroType == type
			|| kFakeRecordType == type)
	{
		//  zero padding
		tLogRecord.hLength = iBufLen - leveldb::log::kHeaderSize;
		tLogRecord.cRecordType = type;
		return pBlkEnd;
	}

	if (leveldb::log::kHeaderSize + length > static_cast<uint32_t>(iBufLen))
	{
		tLogRecord.hLength = iBufLen - leveldb::log::kHeaderSize;
		tLogRecord.cRecordType = kBrokenType;
		logerr("TEST kBrokenType iBufLen %d type %u header[6] %u length %u", 
				iBufLen, type, static_cast<unsigned char>(header[6]), length);
		return pBlkEnd; // skip until
	}

	// else block: check crc	
	if (bCheckCRC)
	{
        int iMaskCRC = 0;
        dbcomm::Decode32Bit(header, iMaskCRC);
		uint32_t iExpectedCRC = 
            leveldb::crc32c::Unmask(static_cast<uint32_t>(iMaskCRC));
		// 6 = leveldb::log::kHeaderSize - 1: type pos
		uint32_t iActualCRC = leveldb::crc32c::Value(header + 6, 1 + length);
		if (iActualCRC != iExpectedCRC)
		{
			tLogRecord.hLength = iBufLen;
			tLogRecord.cRecordType = kBrokenType;
			logerr("TEST INVALID pBlkEnd %p pBlkEnd - pBuffer %ld "
					"kBrokenType bCheckCRC failed iBufLen %d "
					"type %u header[6] %u length %u iActualCRC %u iExpectedCRC %u", 
					pBlkEnd, pBlkEnd - pBuffer, 
					iBufLen, type, static_cast<unsigned char>(header[6]), 
					length, iActualCRC, iExpectedCRC);
			return pBlkEnd;
		}
	}

	tLogRecord.hLength = length;
	tLogRecord.cRecordType = type;
	if (0 != length)
	{
		tLogRecord.pValue = header + leveldb::log::kHeaderSize;
	}
	return pBuffer + leveldb::log::kHeaderSize + length;
}

char* DumpBlockMetaInfoRecord(
		const LogBlockMetaInfo& tBlkMetaInfo, 
		char* const pBlkBegin, char* const pBlkEnd)
{
	assert(sizeof(tBlkMetaInfo) < static_cast<size_t>(pBlkEnd - pBlkBegin));
	char* pNext = DumpLevelDBRecord(
			kBlockMetaType, reinterpret_cast<const char*>(&tBlkMetaInfo), 
			sizeof(tBlkMetaInfo), pBlkBegin, pBlkEnd);
	assert(pNext == pBlkBegin + kBlockMetaInfoRecordSize);
	return pNext;
}

void ZeroTailingSpace(
		const int iBlockSize, 
		char* const pNewBuffer, 
		char* const pBlkEnd)
{
	assert(pNewBuffer <= pBlkEnd);
	if (pNewBuffer == pBlkEnd) 
	{
		return ;
	}

	assert(pBlkEnd - pNewBuffer < iBlockSize);
	memset(pNewBuffer, 0, pBlkEnd - pNewBuffer);
}

void AppendFakeLevelDBRecord(
		const int iBlockSize, 
		const int iMinTailRecordSize, 
		char* const pBuffer, 
		char* const pBlkEnd)
{
	assert(0 < iBlockSize);
	assert(leveldb::log::kHeaderSize <= iMinTailRecordSize);
	assert(iMinTailRecordSize < iBlockSize);
	assert(pBlkEnd - pBuffer >= 0);
	assert(pBlkEnd - pBuffer < iBlockSize);
	if (pBuffer == pBlkEnd)
	{
		return ; // no need
	}

	assert(pBlkEnd - pBuffer > 0);
	int iTailingSize = pBlkEnd - pBuffer;
	assert(iTailingSize < iBlockSize);
	if (iTailingSize <= iMinTailRecordSize)
	{
		memset(pBuffer, 0, iTailingSize);
		return ; // kZeroType: reader will ignore these record
	}

	// else: iTailingSize too large can't be ignore: mark as EOF record
	// dump a empty record: only the header to indicate this is a EOF blk
	DumpLevelDBRecord(kFakeRecordType, NULL, 0, pBuffer, pBlkEnd);	
	return ;
}

char* DumpLevelDBLogBlock(
		BlockSeqGenerator& oSeqGen, 
		const int iBlockSize, 
		const int iMinTailRecordSize, 
		char* pBuffer, char* pBlkEnd, 
		const RecordType t, const char* pRecValue, int iRecValLen)
{
	assert(NULL != pRecValue);
	assert(0 < iRecValLen);
	assert(NULL != pBuffer);
	assert(NULL != pBlkEnd);
	assert(pBuffer < pBlkEnd);
	assert(pBuffer + iBlockSize >= pBuffer);

	char* pNewBuffer = pBuffer;
	if (pBlkEnd - iBlockSize == pNewBuffer)
	{
		// head of blk;
		LogBlockMetaInfo tBlkMetaInfo = {0};
		tBlkMetaInfo.llBlkSeq = oSeqGen.Next();
		pNewBuffer = DumpBlockMetaInfoRecord(tBlkMetaInfo, pNewBuffer, pBlkEnd);
		assert(pNewBuffer < pBlkEnd);
	}

	assert(iRecValLen < pBlkEnd - pNewBuffer);
	pNewBuffer = DumpLevelDBRecord(
			t, pRecValue, iRecValLen, pNewBuffer, pBlkEnd);
	AppendFakeLevelDBRecord(iBlockSize, iMinTailRecordSize, pNewBuffer, pBlkEnd);
	return pNewBuffer;
}

int CheckLevelDBRecord(
		const uint64_t llStartBlkSeq, 
		const RLogLevelDBRecord& tLogRecord, 
		bool bHeadOfBlk, bool bPartialValue, bool& bReadComplete)
{
	bReadComplete = false;
	if (true == bHeadOfBlk && kBlockMetaType != tLogRecord.cRecordType)
	{
		return DB_FORMAT_ERROR_BLOCKMETA_MISSING;
	}

	if (true == bPartialValue 
			&& kBlockMetaType != tLogRecord.cRecordType
			&& kMiddleType != tLogRecord.cRecordType
			&& kLastType != tLogRecord.cRecordType)
	{
		return DB_FORMAT_ERROR_UNEXPECTED_TYPE_1;
	}

	int ret = 0;
	switch (tLogRecord.cRecordType)
	{
	case kFullType:
		assert(false == bPartialValue);
		bReadComplete = true;  
		break;
	case kFirstType:
		assert(false == bPartialValue);
		// bReadComplete = false;
//		bPartialValue = true; // kFirstType turn bPartialValue on
		break;
	case kMiddleType:
		if (false == bPartialValue)
		{
			ret = DB_FORMAT_ERROR_UNEXPECTED_TYPE_2;
		}
		// bReadComplete = false;
		break;
	case kLastType:
		if (false == bPartialValue)
		{
			ret = DB_FORMAT_ERROR_UNEXPECTED_TYPE_2;
		}
		else
		{
			// kLastType turn off bPartialValue
			bReadComplete = true;
		}
		break;
	case kBlockMetaType:
		{
			if (false == bHeadOfBlk)
			{
				ret = DB_FORMAT_ERROR_UNEXPECTED_BLOCKMETA;
			}
			else
			{
				LogBlockMetaInfo tBlkMetaInfo = {0};
				PickleBlockMetaInfo(
						tLogRecord.pValue, tLogRecord.hLength, tBlkMetaInfo);
				if (IsDeprecateBlock(llStartBlkSeq, tBlkMetaInfo.llBlkSeq))
				{
					ret = DB_FORMAT_ERROR_DEPRECATE_BLK;
				}
				// bReadComplete = false;
			}
		}
		break;
	case kZeroType:
		assert(false == bPartialValue);
		// bReadComplete = false;
		break;
	case kFakeRecordType:
		assert(false == bPartialValue);
		// bReadComplete = false;
		break;
	case kBrokenType:
	default:
		ret = DB_FORMAT_ERROR_BROKEN_RECORD;
		break;
	}

	return ret;
}


int PickleLevelDBLogBlock(
		const uint64_t llStartBlkSeq, const int iBlockSize, 
		const char* pBuffer, const char* pBlkEnd, 
		std::string& sValue, const char*& pNewBuffer, bool& bTailingFakeRecord)
{
	assert(0 != llStartBlkSeq);
	assert(0 < iBlockSize);
	assert(pBuffer < pBlkEnd); 
	assert(pBuffer >= pBlkEnd - iBlockSize);

	bTailingFakeRecord = false;
	bool bReadComplete = false;
	bool bPartialValue = !sValue.empty();
	pNewBuffer = pBuffer;
	while (pNewBuffer < pBlkEnd
			&& false == bReadComplete)
	{
		bool bHeadOfBlk = (pNewBuffer == (pBlkEnd - iBlockSize));

		RLogLevelDBRecord tLogRecord = {0};
		pNewBuffer = PickleLevelDBRecord(pNewBuffer, pBlkEnd, tLogRecord, true);
		int ret = CheckLevelDBRecord(
				llStartBlkSeq, tLogRecord, 
				bHeadOfBlk, bPartialValue, bReadComplete);
		if (0 != ret)
		{
			assert(0 > ret);
			return ret; // ERROR CASE
		}

		assert(0 == ret);
		if (kFullType <= tLogRecord.cRecordType
				&& kLastType >= tLogRecord.cRecordType)
		{
			assert(NULL != tLogRecord.pValue);
			assert(0 < tLogRecord.hLength);
			sValue.append(tLogRecord.pValue, tLogRecord.hLength);
		}

		bTailingFakeRecord = 
			(pNewBuffer == pBlkEnd && kFakeRecordType == tLogRecord.cRecordType);
	}

	assert(pNewBuffer <= pBlkEnd);
	if (true == bReadComplete)
	{
		assert(false == sValue.empty());
		return 0;
	}

	assert(pNewBuffer == pBlkEnd);
	return 1; // => expand pBuffer
}

// not include FileMetaInfo;
int CalculateDumpSize(
		const int iBlockSize, const int iBlkOffset, const int iValLen)
{
	assert(0 <= iBlkOffset);
	assert(iBlkOffset < iBlockSize);
	assert(0 <= iValLen);
	if (0 == iValLen)
	{
		return 0;
	}

	int iDumpSize = 0;
	int iRemainValLen = iValLen;
	// 1.
	{
		int iBlkTailingSpace = iBlockSize - iBlkOffset;
		assert(leveldb::log::kHeaderSize < iBlkTailingSpace);

		assert(iBlockSize >= iBlkTailingSpace);
		if (iBlockSize > iBlkTailingSpace)
		{
			// at least one block meta info record have been write!
			assert(iBlockSize - iBlkTailingSpace >= kBlockMetaInfoRecordSize);

			if (iRemainValLen <= iBlkTailingSpace - leveldb::log::kHeaderSize)
			{
				// new record can fit into iBlkTailingSpace:
				iDumpSize = iRemainValLen + leveldb::log::kHeaderSize;
				assert(iDumpSize <= iBlkTailingSpace);
				return iDumpSize;
			}

			// the exist tailing space not enough!
			iRemainValLen -= (iBlkTailingSpace - leveldb::log::kHeaderSize);
			iDumpSize = iBlkTailingSpace;
		}
		// iBlockSize == iBlkTailingSpace covered by step 2;
	}
	
	// 2.
	int iMaxConsumeRecordSizePerBlk = 
		iBlockSize - kBlockMetaInfoRecordSize - leveldb::log::kHeaderSize;
	assert(0 < iMaxConsumeRecordSizePerBlk);
	int iNumOfBlk = iRemainValLen / iMaxConsumeRecordSizePerBlk;
	assert(0 <= iNumOfBlk);
	iDumpSize += iBlockSize * iNumOfBlk;
	iRemainValLen -= iNumOfBlk * iMaxConsumeRecordSizePerBlk;
	assert(0 <= iRemainValLen);
	assert(iRemainValLen < iMaxConsumeRecordSizePerBlk);

	// 3. new tailing block
	if (0 < iRemainValLen)
	{	
		iDumpSize += (kBlockMetaInfoRecordSize 
			+ leveldb::log::kHeaderSize + iRemainValLen);
		iRemainValLen = 0;
	}

	assert(0 == iRemainValLen);
	return iDumpSize;
}



// DirectIOWriteInfo

DirectIOWriteInfo::DirectIOWriteInfo(
		const int iBlockSizeI, 
		const int iMaxDirectIOBufSizeI, 
		const int iMinTailRecordSizeI, 
		const uint64_t llStartBlkSeqI, 
		const AdjustStrategyType iAdjustStrategyI)
	: iBlockSize(iBlockSizeI)
	, iMaxDirectIOBufSize(iMaxDirectIOBufSizeI)
	, iMinTailRecordSize(
			max(iMinTailRecordSizeI, 
				// max key len: 8
				static_cast<int>(
                    leveldb::log::kHeaderSize + MIN_RECORD_SIZE + 8)))
	, llStartBlkSeq(llStartBlkSeqI)
	, oSeqGen(llStartBlkSeqI)
	, iDirectFD(INVALID_FD)
	, iOffset(0)
	, iDirectIOOffset(0)
	, pDirectIOBuffer(NULL)
	, iAdjustStrategy(iAdjustStrategyI)
	, iWriteStatus(0)
	, llAccCnt(0)
	, llAccValueLen(0)
	, llAccDumpSize(0)
	, llAccWriteSize(0)
	, llAccCostTime(0)
	, llAccBatchCnt(0)
	, llAccBatchSize(0)
	, llAccBatchValueLen(0)
	, llAccBatchDumpSize(0)
	, llAccBatchWriteSize(0)
	, llAccBatchCostTime(0)
{
	assert(0 < iBlockSize);
	assert(0 == iMaxDirectIOBufSize % iBlockSize);
	assert(iMinTailRecordSize < iBlockSize);

	pDirectIOBuffer = AllocDirectIOBuffer(iMaxDirectIOBufSize);
	assert(NULL != pDirectIOBuffer);
}

DirectIOWriteInfo::~DirectIOWriteInfo()
{
	if (INVALID_FD != iDirectFD)
	{
		close(iDirectFD);
		iDirectFD = INVALID_FD;
	}

	if (NULL != pDirectIOBuffer)
	{
		free(pDirectIOBuffer);
		pDirectIOBuffer = NULL;
	}
}

std::string GetStatInfo(const DirectIOWriteInfo* ptInfo)
{
	assert(NULL != ptInfo);
	stringstream ss;
	ss << ptInfo->oSeqGen.GetBlkSeq() << " "
	   << ptInfo->llAccCnt << " "
	   << ptInfo->llAccValueLen << " "
	   << ptInfo->llAccDumpSize << " "
	   << ptInfo->llAccWriteSize << " "
	   << ptInfo->llAccCostTime << " "
	   << ptInfo->llAccBatchCnt << " "
	   << ptInfo->llAccBatchSize << " "
	   << ptInfo->llAccBatchValueLen << " "
	   << ptInfo->llAccBatchDumpSize << " "
	   << ptInfo->llAccBatchWriteSize << " "
	   << ptInfo->llAccBatchCostTime;
	return ss.str();
}

int OpenFile(
		DirectIOWriteInfo* ptInfo, 
		const char* sLevelDBLogFile, 
		const uint64_t llStartBlkSeq, const int iAdjustStrategy)
{
	assert(NULL != ptInfo);
	assert(NULL != sLevelDBLogFile);
	assert(0 != llStartBlkSeq);
	assert(0 <= iAdjustStrategy && 2 >= iAdjustStrategy);

	int iNewDirectFD = OpenForDirectIOWrite(sLevelDBLogFile);
	if (0 > iNewDirectFD)
	{
		logerr("OpenForDirectIOWrite %s ret %d", sLevelDBLogFile, iNewDirectFD);
		return -1;
	}

	CREATE_FD_MANAGER(iNewDirectFD);
	swap(iNewDirectFD, ptInfo->iDirectFD);
	ptInfo->oSeqGen.Init(llStartBlkSeq);
	ptInfo->iAdjustStrategy = (AdjustStrategyType)iAdjustStrategy;
	ptInfo->iOffset = 0;
	ptInfo->iDirectIOOffset = 0;
	ptInfo->iWriteStatus = 0;

	// prev iDirectFD => swap into iNewDirectFD, 
	// => will closed by fd manager if needed!
//	if (INVALID_FD != iNewDirectFD)
//	{
//		close(iNewDirectFD);
//		iNewDirectFD = INVALID_FD;
//	}
	
	return 0;
}

int CloseFile(DirectIOWriteInfo* ptInfo)
{
	assert(NULL != ptInfo);

	int iDirectFD = INVALID_FD;
	swap(iDirectFD, ptInfo->iDirectFD);
	ptInfo->iOffset = 0;
	ptInfo->iDirectIOOffset = 0;
	ptInfo->iWriteStatus = 0;

	int ret = 0;
	if (INVALID_FD != iDirectFD)
	{
		ret = close(iDirectFD);
		iDirectFD = INVALID_FD;
	}

	return ret;
}

int CreateADirectIOWriteInfo(
		const char* sLevelDBLogFile, 
		const int iBlockSize, const int iMaxDirectIOBufSize,
		const int iMinTailRecordSize, const uint64_t llStartBlkSeq, 
		const int iAdjustStrategy, uint32_t iOffset, 
		DirectIOWriteInfo*& ptInfo)
{
	assert(NULL == ptInfo);
	{
		// check param valid
		assert(0 != llStartBlkSeq);
		assert(0 <= iAdjustStrategy && 2 >= iAdjustStrategy);
		assert(iBlockSize <= iMaxDirectIOBufSize);
		assert(0 <= iMinTailRecordSize);
		const int iPageSize = getpagesize();
		assert(0 < iPageSize);
		assert(iPageSize <= iBlockSize);
		assert(0 == iBlockSize % iPageSize);
		assert(0 == iMaxDirectIOBufSize % iBlockSize);
		// fix:
		iOffset = min(MAX_LOG_FILE_SIZE, iOffset);
	}

	// => simpley move iOffset into next block
	// => adjust iFileNo & iOffset;
	const uint32_t iAdjustOffset = 
		(iOffset + iBlockSize - 1) / iBlockSize * iBlockSize;

	ptInfo = new DirectIOWriteInfo(iBlockSize, 
			iMaxDirectIOBufSize, iMinTailRecordSize, 
			llStartBlkSeq, (AdjustStrategyType)iAdjustStrategy);
	assert(NULL != ptInfo);

	if (NULL != sLevelDBLogFile)
	{
		int ret = OpenFile(ptInfo, sLevelDBLogFile, llStartBlkSeq, iAdjustStrategy);
		if (0 != ret)
		{
			return -2;
		}

		assert(INVALID_FD != ptInfo->iDirectFD);
	}

	// adjust iOffset & iDirectIOOffset
	ptInfo->iOffset = iAdjustOffset;
	ptInfo->iDirectIOOffset = ptInfo->iOffset;
	// TODO: check DirectIOWriteInfo consist with m_sKvLogPath/m_iFileNo ?
	return 0;
}

bool NeedIterWriteFile(
		const DirectIOWriteInfo* ptInfo, const int iEstimateWriteSize)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);
	const int iPrevSize = ptInfo->iOffset / ptInfo->iBlockSize * ptInfo->iBlockSize;
	return MAX_LOG_FILE_SIZE < 
		static_cast<uint32_t>(iPrevSize + iEstimateWriteSize);
}

int IterWriteFile(
		DirectIOWriteInfo* ptInfo, 
		const std::string& sKvLogPath, 
		const std::string& sKvRecyclePath, 
		const uint32_t iNextFileNo, 
		const char cPostfix)
{
	if (NULL == ptInfo)
	{
		return -1;
	}

	char sFileName[32] = {0};
	int ret = snprintf(sFileName, 
			sizeof(sFileName)-1, "%u.%c", iNextFileNo, cPostfix);
	if (0 >= ret)
	{
		return -2;
	}

	string sAbsDBFileName;
	ConcateCStrPath(sKvLogPath, sFileName, strlen(sFileName), sAbsDBFileName);

	ret = TryRecycleOneFile(ptInfo, sAbsDBFileName, sKvRecyclePath);
	logdebug("TryRecycleOneFile %s ret %d", sAbsDBFileName.c_str(), ret);

	int iNextDirectIOFD = OpenForDirectIOWrite(sAbsDBFileName.c_str());
	if (0 > iNextDirectIOFD)
	{
		return -3;
	}

	assert(INVALID_FD != iNextDirectIOFD);
	CREATE_FD_MANAGER(iNextDirectIOFD);
	// commit 
	ptInfo->llStartBlkSeq = ptInfo->oSeqGen.GetBlkSeq();
	ptInfo->iOffset = 0; // reset offset
	ptInfo->iDirectIOOffset = 0;

	swap(ptInfo->iDirectFD, iNextDirectIOFD);
	// prev iDirectFD shift into iNextDirectIOFD, 
	// => will close by fd manager
//	if (INVALID_FD != iNextDirectIOFD)
//	{
//		close(iNextDirectIOFD);
//	}

	return 0;
}

inline bool IsValid(const DirectIOWriteInfo* ptInfo)
{
	return 0 == ptInfo->iWriteStatus;
}

inline void FixDirectIOWriteInfo(DirectIOWriteInfo* ptInfo)
{
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	assert(ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize
			>= ptInfo->iOffset);
	ptInfo->iWriteStatus = 0;
}

// include FileMetaInfo
int CalculateDumpSize(const DirectIOWriteInfo* ptInfo, const int iValLen)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);
	const int iBlkOffset = ptInfo->iOffset % ptInfo->iBlockSize;
	assert(iBlkOffset + ptInfo->iMinTailRecordSize < ptInfo->iBlockSize);
	// not include FileMetaInfo
	int iDumpSize = CalculateDumpSize(ptInfo->iBlockSize, iBlkOffset, iValLen);
	// include FileMetaInfo
	return 0 == ptInfo->iOffset ? iDumpSize + ptInfo->iBlockSize : iDumpSize;
}

int CalculateDumpSize(
		const DirectIOWriteInfo* ptInfo, 
		const std::vector<leveldb::Slice>& vecValue)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);

	// not include FileMetaInfo
	int iDumpSize = 0;
	for (size_t i = 0; i < vecValue.size(); ++i)
	{
		const leveldb::Slice& tSlice = vecValue[i];
		assert(false == tSlice.empty());
		int iBlkOffset = (ptInfo->iOffset + iDumpSize) % ptInfo->iBlockSize;

		int iIncDumpSize = 0;
		if (iBlkOffset + ptInfo->iMinTailRecordSize < ptInfo->iBlockSize)
		{
			iIncDumpSize = CalculateDumpSize(
					ptInfo->iBlockSize, iBlkOffset, tSlice.size());
		}
		else
		{
			iIncDumpSize = CalculateDumpSize(
					ptInfo->iBlockSize, 0, tSlice.size());
			iIncDumpSize += (ptInfo->iBlockSize - iBlkOffset);
		}
		
		assert(0 < iIncDumpSize);
		assert(iDumpSize < iDumpSize + iIncDumpSize);
		iDumpSize += iIncDumpSize;
	}

	// include FileMetaInfo
	return 0 == ptInfo->iOffset ? iDumpSize + ptInfo->iBlockSize : iDumpSize;
}

int GetEstimateWriteSize(const DirectIOWriteInfo* ptInfo, const int iDumpSize)
{
	assert(NULL != ptInfo);
	assert(0 < iDumpSize);
	assert(0 < ptInfo->iBlockSize);

	int iBlkOffset = ptInfo->iOffset % ptInfo->iBlockSize;
	int iNumOfWriteBlock = 
		(iDumpSize + iBlkOffset + ptInfo->iBlockSize - 1) / ptInfo->iBlockSize;
	assert(0 < iNumOfWriteBlock);
	// iDumpSize already include file meta info block !!
	return iNumOfWriteBlock * ptInfo->iBlockSize;
}


namespace AdjustStrategy {

namespace {

int AdjustImpl(DirectIOWriteInfo* ptInfo, 
		const int iDumpSize, const int iEstimateWriteSize)
{
	assert(NULL != ptInfo);
	assert(0 < iDumpSize);
	assert(iDumpSize <= iEstimateWriteSize);

	if (iEstimateWriteSize > ptInfo->iMaxDirectIOBufSize)
	{
		return DB_WRITE_ERROR_LARGE_WRITE;
	}

	if (NeedIterWriteFile(ptInfo, iEstimateWriteSize))
	{
		return DB_WRITE_ERROR_NEXT_FILE;
	}

	assert(iEstimateWriteSize <= ptInfo->iMaxDirectIOBufSize);
	int iBlkOffset = ptInfo->iOffset % ptInfo->iBlockSize;
	assert(iBlkOffset + ptInfo->iMinTailRecordSize < ptInfo->iBlockSize);
	if (0 == iBlkOffset)
	{
		ptInfo->iDirectIOOffset = ptInfo->iOffset;
		return 0;
	}

	uint32_t iNewDirectIOOffset = ptInfo->iOffset - iBlkOffset;
	assert(iNewDirectIOOffset < ptInfo->iOffset);
	assert(iNewDirectIOOffset >= ptInfo->iDirectIOOffset);
	if (iNewDirectIOOffset + iEstimateWriteSize
			<= ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize)
	{
		return 0; // ok: don't need move around the buffer
	}

	assert(iNewDirectIOOffset > ptInfo->iDirectIOOffset);
	assert(iNewDirectIOOffset < ptInfo->iOffset);
	assert(ptInfo->pDirectIOBuffer 
			+ (iNewDirectIOOffset - ptInfo->iDirectIOOffset) 
			<= ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize);
	memcpy(ptInfo->pDirectIOBuffer, ptInfo->pDirectIOBuffer + (
				iNewDirectIOOffset - ptInfo->iDirectIOOffset), iBlkOffset);
	ptInfo->iDirectIOOffset = iNewDirectIOOffset;
	AppendFakeLevelDBRecord(ptInfo->iBlockSize, 
			ptInfo->iMinTailRecordSize, 
			ptInfo->pDirectIOBuffer + iBlkOffset, 
			ptInfo->pDirectIOBuffer + ptInfo->iBlockSize);

	assert(0 < iBlkOffset);
	return iBlkOffset;
}

} // namespace 

template <typename CalculateDumpSizeType>
int MaxCompactAdjust(
		DirectIOWriteInfo* ptInfo, 
		const CalculateDumpSizeType& oObj, int& iDumpSize, int& iEstimateWriteSize)
{
	iDumpSize = CalculateDumpSize(ptInfo, oObj);
	iEstimateWriteSize = GetEstimateWriteSize(ptInfo, iDumpSize);
	assert(0 < iDumpSize);
	assert(iDumpSize <= iEstimateWriteSize);

	return AdjustImpl(ptInfo, iDumpSize, iEstimateWriteSize);
}

template <typename CalculateDumpSizeType>
int DontMoveAdjust(
		DirectIOWriteInfo* ptInfo, 
		const CalculateDumpSizeType& oObj, int& iDumpSize, int& iEstimateWriteSize)
{
	int iBlkOffset = ptInfo->iOffset % ptInfo->iBlockSize;
	assert(iBlkOffset + ptInfo->iMinTailRecordSize < ptInfo->iBlockSize);
	if (0 == iBlkOffset)
	{
		ptInfo->iDirectIOOffset = ptInfo->iOffset;
	}
	else
	{
		// else skip iOffset into next blk;
		uint32_t iNewOffset = ptInfo->iOffset - iBlkOffset + ptInfo->iBlockSize;
		assert(iNewOffset > ptInfo->iOffset);
		ptInfo->iOffset = iNewOffset;
		ptInfo->iDirectIOOffset = ptInfo->iOffset;
	}

	iDumpSize = CalculateDumpSize(ptInfo, oObj);
	iEstimateWriteSize = GetEstimateWriteSize(ptInfo, iDumpSize);
	return AdjustImpl(ptInfo, iDumpSize, iEstimateWriteSize);
}

template <typename CalculateDumpSizeType>
int MinWriteAdjust(
		DirectIOWriteInfo* ptInfo, 
		const CalculateDumpSizeType& oObj, int& iDumpSize, int& iEstimateWriteSize)
{
	iDumpSize = CalculateDumpSize(ptInfo, oObj);
	iEstimateWriteSize = GetEstimateWriteSize(ptInfo, iDumpSize);
	assert(0 < iDumpSize);
	assert(iDumpSize <= iEstimateWriteSize);

	int iBlkOffset = ptInfo->iOffset % ptInfo->iBlockSize;
	assert(iBlkOffset + ptInfo->iMinTailRecordSize < ptInfo->iBlockSize);
	if (0 != iBlkOffset)
	{
		uint32_t iOldOffset = ptInfo->iOffset;
		uint32_t iNewOffset = ptInfo->iOffset - iBlkOffset + ptInfo->iBlockSize;
		assert(iNewOffset > iOldOffset);

		int iNewDumpSize = 0;
		int iNewEstimateWriteSize = 0;
		{
			ptInfo->iOffset = iNewOffset;
			iNewDumpSize = CalculateDumpSize(ptInfo, oObj);
			iNewEstimateWriteSize = GetEstimateWriteSize(ptInfo, iNewDumpSize);
			ptInfo->iOffset = iOldOffset;
		}
		assert(ptInfo->iOffset == iOldOffset);
		assert(0 < iNewDumpSize);
		assert(iNewDumpSize <= iNewEstimateWriteSize);

		if (iNewEstimateWriteSize < iEstimateWriteSize)
		{
			// prefer iNewOffset: skip to next blk
			ptInfo->iOffset = iNewOffset;
			ptInfo->iDirectIOOffset = ptInfo->iOffset;
			iDumpSize = iNewDumpSize;
			iEstimateWriteSize = iNewEstimateWriteSize;
			iBlkOffset = 0;
		}
	}

	return AdjustImpl(ptInfo, iDumpSize, iEstimateWriteSize);
}

template <typename CalculateDumpSizeType>
int AdjustDirectIOWriteBuffer(
		DirectIOWriteInfo* ptInfo, 
		const CalculateDumpSizeType& oObj, int& iDumpSize, int& iEstimateWriteSize)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	assert(ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize >= ptInfo->iOffset);

	int ret = 0;
	switch (ptInfo->iAdjustStrategy)	
	{
		case kMaxCompactStrategy:
			ret = MaxCompactAdjust(ptInfo, oObj, iDumpSize, iEstimateWriteSize);
			break;
		case kDontMoveStrategy:
			ret = DontMoveAdjust(ptInfo, oObj, iDumpSize, iEstimateWriteSize);
			break;
		case kMinWriteStrategy:
			ret = MinWriteAdjust(ptInfo, oObj, iDumpSize, iEstimateWriteSize);
			break;
		default:
			assert(0);
			break;
	}

	return ret;
}

} // namespace AdjustStrategy

inline char* GetCurrentPos(DirectIOWriteInfo* ptInfo)
{
	assert(NULL != ptInfo);
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	assert(ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize >= ptInfo->iOffset);
	return ptInfo->pDirectIOBuffer + (
			ptInfo->iOffset - ptInfo->iDirectIOOffset);
}

inline const char* GetCurrentPos(const DirectIOWriteInfo* ptInfo)
{
	return GetCurrentPos(const_cast<DirectIOWriteInfo*>(ptInfo));
}

inline char* GetDirectBufferEnd(DirectIOWriteInfo* ptInfo)
{
	assert(NULL != ptInfo);
	assert(NULL != ptInfo->pDirectIOBuffer);
	return ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize;
}

inline char* GetNextBlockPos(
		DirectIOWriteInfo* ptInfo, char* pBuffer)
{
	assert(ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize >= pBuffer);
	return GetNextBlockPosImpl(ptInfo, pBuffer);
}

inline char* GetPrevBlockPos(
		DirectIOWriteInfo* ptInfo, char* pBuffer)
{
	assert(NULL != ptInfo);
	assert(ptInfo->pDirectIOBuffer <= pBuffer);
	assert(ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize >= pBuffer);
	assert(0 < ptInfo->iBlockSize);
	int iBlkOffset = (pBuffer - ptInfo->pDirectIOBuffer) % ptInfo->iBlockSize;
	assert(0 <= iBlkOffset);
	return pBuffer - iBlkOffset;
}

inline const char* GetPrevBlockPos(
		const DirectIOWriteInfo* ptInfo, const char* pBuffer)
{
	return GetPrevBlockPos(
			const_cast<DirectIOWriteInfo*>(ptInfo), 
			const_cast<char*>(pBuffer));
}

void CheckBlockMetaInfo(
		const DirectIOWriteInfo* ptInfo, 
		const char* pBeginBlock, const char* pEndBlock)
{
	const int iBlockSize = ptInfo->iBlockSize;

	if (0 == ptInfo->iDirectIOOffset)
	{
		pBeginBlock += iBlockSize;
	}

	while (pBeginBlock < pEndBlock)
	{
		RLogLevelDBRecord tLogRecord = {0};
		assert(0 == (pBeginBlock - ptInfo->pDirectIOBuffer) % iBlockSize);
		const char* pNewBuffer = PickleLevelDBRecord(
				pBeginBlock, pBeginBlock + iBlockSize, tLogRecord, true);
		if (kBlockMetaType == tLogRecord.cRecordType)
		{
			LogBlockMetaInfo tBlkMetaInfo = {0};
			PickleBlockMetaInfo(
					tLogRecord.pValue, tLogRecord.hLength, tBlkMetaInfo);
			if (IsDeprecateBlock(ptInfo->llStartBlkSeq, tBlkMetaInfo.llBlkSeq))
			{
				printf ( "ptInfo->iDirectIOOffset %u ptInfo->iOffset %u "
						"ptInfo->pDirectIOBuffer %p pBeginBlock %p pEndBlock %p "
						"tLogRecord.cRecordType %u tLogRecord.hLength %u\n", 
						ptInfo->iDirectIOOffset, ptInfo->iOffset, 
						ptInfo->pDirectIOBuffer, pBeginBlock, pEndBlock, 
						tLogRecord.cRecordType, tLogRecord.hLength);
				assert(0);
			}
		}
		else
		{
			printf ( "ptInfo->iDirectIOOffset %u ptInfo->iOffset %u "
					"ptInfo->pDirectIOBuffer %p pBeginBlock %p pEndBlock %p "
					"tLogRecord.cRecordType %u tLogRecord.hLength %u\n", 
					ptInfo->iDirectIOOffset, ptInfo->iOffset, 
					ptInfo->pDirectIOBuffer, pBeginBlock, pEndBlock, 
					tLogRecord.cRecordType, tLogRecord.hLength);
		}
		assert(kBlockMetaType == tLogRecord.cRecordType);	
		assert(pNewBuffer < pBeginBlock + iBlockSize);
		pBeginBlock += iBlockSize;
	}
}

void CheckWriteBuffer(
		const DirectIOWriteInfo* ptInfo, 
		const char* pBeginBlock, const char* pEndBlock)
{
	const int iBlockSize = ptInfo->iBlockSize;
	const char* pBuffer = GetCurrentPos(ptInfo);
	const char* pNextBlock = pBeginBlock + iBlockSize;
	assert(pBuffer != pNextBlock);
	assert(pNextBlock - pBuffer > 0);
	assert(pNextBlock - pBuffer <= iBlockSize);
	assert(pNextBlock <= pEndBlock);
	if (0 == ptInfo->iOffset)
	{
		pBuffer = pNextBlock;
		pNextBlock = pNextBlock == pEndBlock ? pEndBlock : pNextBlock + iBlockSize;
	}

	bool bEOF = false;
	while (false == bEOF && pBuffer < pEndBlock)
	{
		assert(pBuffer < pNextBlock);
		bool bReadComplete = false;
		const char* pNewBuffer = pBuffer;
		bool bPartialValue = false;
//		if (pNewBuffer >= pNextBlock)
//		{
//			printf ( "pNewBuffer %p pNextBlock %p pBuffer %p pEndBlock %p pBeginBlock %p bReadComplete %u "
//					"iOffset %u iDirectIOOffset %u bPartialValue %u\n", 
//					pNewBuffer, pNextBlock, pBuffer, pEndBlock, pBeginBlock, bReadComplete, 
//					ptInfo->iOffset, ptInfo->iDirectIOOffset, 
//					bPartialValue);
//
//		}
		assert(pNewBuffer < pNextBlock);
		while (false == bReadComplete)
		{
			bool bHeadOfBlk = (pNewBuffer == pNextBlock - iBlockSize);
//			if (pNewBuffer >= pNextBlock)
//			{
//				printf ( "pNewBuffer %p pNextBlock %p pBuffer %p pEndBlock %p pBeginBlock %p bReadComplete %u "
//						"iOffset %u iDirectIOOffset %u bHeadOfBlk %u bPartialValue %u\n", 
//						pNewBuffer, pNextBlock, pBuffer, pEndBlock, pBeginBlock, bReadComplete, 
//						ptInfo->iOffset, ptInfo->iDirectIOOffset, 
//						bHeadOfBlk, bPartialValue);
//			}
			assert(pNewBuffer < pNextBlock);

			RLogLevelDBRecord tLogRecord = {0};	
			pNewBuffer = PickleLevelDBRecord(pNewBuffer, pNextBlock, tLogRecord, true);
//			printf ( "tLogRecord.cRecordType %u tLogRecord.hLength %u\n", 
//					tLogRecord.cRecordType, tLogRecord.hLength );
			int ret = CheckLevelDBRecord(ptInfo->llStartBlkSeq, 
					tLogRecord, bHeadOfBlk, bPartialValue, bReadComplete);
			if (0 != ret)
			{
				printf ( "ret %d ptInfo->iOffset %u ptInfo->iDirectIOOffset %u "
						"pBeginBlock %p pEndBlock %p"
						" pBuffer %p pNewBuffer %p pNextBlock %p cRecordType %u\n", 
						ret, ptInfo->iOffset, ptInfo->iDirectIOOffset, 
						pBeginBlock, pEndBlock, 
						pBuffer, pNewBuffer, pNextBlock, tLogRecord.cRecordType);
			}
			assert(0 == ret);

			if (kFirstType == tLogRecord.cRecordType)
			{
				bPartialValue = true;
			}

			if (kZeroType == tLogRecord.cRecordType
					|| kFakeRecordType == tLogRecord.cRecordType)
			{
				assert(false == bPartialValue);
				bReadComplete = true;
			}

			if (pNewBuffer == pNextBlock)
			{
				pNextBlock = pNextBlock == pEndBlock ? pEndBlock : pNextBlock + iBlockSize;
			}

//			if (false == bReadComplete && pNewBuffer >=  pNextBlock)
//			{
//				printf ( "tLogRecord.cRecordType %u tLogRecord.hLength %u "
//						"pBuffer %p pNewBuffer %p pNextBlock %p "
//						"bReadComplete %u bPartialValue %u iOffset %u\n", 
//						tLogRecord.cRecordType, tLogRecord.hLength, 
//						pBuffer, pNewBuffer, pNextBlock, bReadComplete, bPartialValue, 
//						ptInfo->iOffset);
//			}
			assert(true == bReadComplete
					|| pNewBuffer < pNextBlock);
		}

		assert(pNewBuffer < pNextBlock || pNewBuffer == pEndBlock);

		pBuffer = pNewBuffer;
		if (pBuffer == pNextBlock)
		{
			pNextBlock = pNextBlock == pEndBlock ? pEndBlock : pNextBlock + iBlockSize;
		}
		assert(pBuffer < pNextBlock || pBuffer == pEndBlock);
	}
}


int SmallWriteImpl(
		const DirectIOWriteInfo* ptInfo, 
		const char* pBeginBlock, const char* pEndBlock)
{
	assert(pEndBlock - pBeginBlock > 0);
	assert(ptInfo->pDirectIOBuffer <= pBeginBlock);
	assert(pBeginBlock + ptInfo->iBlockSize <= pEndBlock);
	assert(ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize >= pEndBlock);
	assert(ptInfo->iDirectIOOffset + (pEndBlock - ptInfo->pDirectIOBuffer)
			<= MAX_LOG_FILE_SIZE);
	assert(0 == (pBeginBlock - ptInfo->pDirectIOBuffer) % ptInfo->iBlockSize);
	assert(0 == (pEndBlock - ptInfo->pDirectIOBuffer) % ptInfo->iBlockSize);

	int iWriteSize = SafeDirectIOPWrite(
			ptInfo->iDirectFD, pBeginBlock, pEndBlock - pBeginBlock, 
			ptInfo->iDirectIOOffset + (pBeginBlock - ptInfo->pDirectIOBuffer));
	if (iWriteSize != (pEndBlock - pBeginBlock))
	{
		if (0 > iWriteSize)
		{
			logerr("SafeDirectIOPWrite ret %d", iWriteSize);
			return DB_WRITE_ERROR_BASIC;
		}

		return DB_WRITE_ERROR_POOR_WRITE;
	}

	return iWriteSize;
}

int CalculateWriteSize(
		const DirectIOWriteInfo* ptInfo, const char* pNewBuffer)
{
	if (ptInfo->iDirectIOOffset <= ptInfo->iOffset)
	{
		const char* pBuffer = GetCurrentPos(ptInfo);
		assert(pBuffer <= pNewBuffer);
		return pNewBuffer - pBuffer;
	}

	// else
	assert(pNewBuffer >= ptInfo->pDirectIOBuffer);
	assert(pNewBuffer <= ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize);
	return (ptInfo->iDirectIOOffset - ptInfo->iOffset)
			+ (pNewBuffer - ptInfo->pDirectIOBuffer);
}

int CalculateDumpValueLen(
		const int iBlockSize, const int iBlkTailingSpace)
{
	if (iBlockSize == iBlkTailingSpace)
	{
		// a full block
		return iBlockSize - kBlockMetaInfoRecordSize - leveldb::log::kHeaderSize;
	}

	assert(iBlockSize - iBlkTailingSpace 
			>= kBlockMetaInfoRecordSize + leveldb::log::kHeaderSize);
	return iBlkTailingSpace - leveldb::log::kHeaderSize;
}

const char* DumpValueImpl(
		BlockSeqGenerator& oSeqGen, 
		const int iBlockSize, 
		const int iMinTailRecordSize, 
		char* const pBuffer, 
		char* const pNextBlock, 
		const char* pValue, 
		const int iValLen, 
		const char* pValueIter, 
		char*& pNewBuffer)
{
	assert(pBuffer < pNextBlock);
	assert(pNextBlock - pBuffer <= iBlockSize);
//	assert(pNextBlock <= pDirectBufferEnd);
	int iDumpValueLen = 
		CalculateDumpValueLen(iBlockSize, pNextBlock - pBuffer);
	if (NULL == pValueIter)
	{
		if (iDumpValueLen >= iValLen)
		{
			// kFullType
			pNewBuffer = DumpLevelDBLogBlock(
					oSeqGen, iBlockSize, iMinTailRecordSize, 
					pBuffer, pNextBlock, kFullType, pValue, iValLen);
			assert(pNewBuffer <= pNextBlock);
			return pValue + iValLen; // success dump
		}

		assert(iDumpValueLen < iValLen);
		// kFirstType
		pNewBuffer = DumpLevelDBLogBlock(
				oSeqGen, iBlockSize, iMinTailRecordSize, 
				pBuffer, pNextBlock, kFirstType, pValue, iDumpValueLen);
		assert(pNewBuffer == pNextBlock);
		return pValue + iDumpValueLen;
	}

	// else
	assert(pValueIter > pValue);
	assert(pValueIter < pValue + iValLen);
	if (iDumpValueLen >= (iValLen - (pValueIter - pValue)))
	{
		// kLastType
		pNewBuffer = DumpLevelDBLogBlock(
				oSeqGen, iBlockSize, iMinTailRecordSize, 
				pBuffer, pNextBlock, kLastType, pValueIter, 
				iValLen - (pValueIter - pValue));
		assert(pNewBuffer > pBuffer);
		assert(pNewBuffer <= pNextBlock);
		return pValue + iValLen; // END
	}

	// kMiddleType
	assert(pBuffer + iBlockSize == pNextBlock);
	pNewBuffer = DumpLevelDBLogBlock(
			oSeqGen, iBlockSize, iMinTailRecordSize, 
			pBuffer, pNextBlock, kMiddleType, pValueIter, iDumpValueLen);
	assert(pNewBuffer == pNextBlock);
	return pValueIter + iDumpValueLen;
}

int DumpAndWrite(
		DirectIOWriteInfo* ptInfo, 
		const char* pValue, const int iValLen, 
		const int iDumpSize, const int iEstimateWriteSize)
{
	int ret = 0;
	const int iBlockSize = ptInfo->iBlockSize;

	char* pBuffer = GetCurrentPos(ptInfo);
	char* pBeginBlock = GetPrevBlockPos(ptInfo, pBuffer);
	char* pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	char* pNextBlock = pBeginBlock + iBlockSize;
	assert(pNextBlock <= pDirectBufferEnd);

	if (0 == ptInfo->iOffset)
	{
		// file meta info
		assert(0 == ptInfo->iDirectIOOffset);
		assert(pBuffer == pNextBlock - iBlockSize);
		LogFileMetaInfo tFileMetaInfo = {0};
		tFileMetaInfo.cMode = 0;
		tFileMetaInfo.llStartBlkSeq = ptInfo->llStartBlkSeq;
		tFileMetaInfo.iBlockSize = iBlockSize;
		DumpFileMetaInfo(tFileMetaInfo, pBuffer, pNextBlock);
		pBuffer = pNextBlock;
		pNextBlock = (pNextBlock == pDirectBufferEnd) 
			? pDirectBufferEnd : pNextBlock + iBlockSize;
		// adjust strage guarent this assert
		assert(pNextBlock != pDirectBufferEnd);
	}

	char* pNewBuffer = NULL;
	const char* pValueIter = NULL;
	while (true)
	{
		// assert(pBuffer != pNextBlock);
		if (pBuffer == pNextBlock)
		{
			assert(pDirectBufferEnd == pBuffer);
			// should be assert(..);
			return DB_WRITE_ERROR_LARGE_RECORD;
		}

		assert(pBuffer < pNextBlock);
		assert(pNextBlock <= pDirectBufferEnd);
		assert(0 == (pNextBlock - ptInfo->pDirectIOBuffer) % iBlockSize);
		pValueIter = DumpValueImpl(
				ptInfo->oSeqGen, iBlockSize, ptInfo->iMinTailRecordSize, 
				pBuffer, pNextBlock, pValue, iValLen, pValueIter, 
				pNewBuffer);
		if (pValue + iValLen == pValueIter)
		{
			break; // complete
		}

		assert(NULL != pValueIter);
		assert(pValueIter > pValue);
		assert(pValueIter < pValue + iValLen);

		pBuffer = pNextBlock;
		pNextBlock = (pNextBlock == pDirectBufferEnd) 
			?  pDirectBufferEnd : pNextBlock + iBlockSize;
	}

	// success dump
	assert(pNextBlock <= pDirectBufferEnd);
	assert(pValueIter == pValue + iValLen);
	
	ZeroTailingSpace(iBlockSize, pNewBuffer, pNextBlock);
	ret = SmallWriteImpl(ptInfo, pBeginBlock, pNextBlock);
	if (0 > ret)
	{
		return ret; // ERROR CASE
	}

	assert(ret == pNextBlock - pBeginBlock);
	assert(ret == iEstimateWriteSize);
	// success write
	int iWriteSize = CalculateWriteSize(ptInfo, pNewBuffer);
	assert(0 < iWriteSize);
	assert(iWriteSize == iDumpSize);
	return iWriteSize;
}

int BatchDumpAndWrite(
		DirectIOWriteInfo* ptInfo, 
		const std::vector<leveldb::Slice>& vecValue, 
		const int iBatchDumpSize, const int iEstimateWriteSize, 
		std::vector<uint32_t>& vecOffset)
{
	const int iBlockSize = ptInfo->iBlockSize;
	assert(0 < iBlockSize);

	char* pBuffer = GetCurrentPos(ptInfo);
	char* pBeginBlock = GetPrevBlockPos(ptInfo, pBuffer);
	assert(pBuffer - pBeginBlock >= 0);
	assert(pBuffer - pBeginBlock < iBlockSize);
	char* pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	char* pNextBlock = pBeginBlock + iBlockSize;
	assert(pBuffer < pNextBlock);
	assert(pNextBlock <=  pDirectBufferEnd);

	if (0 == ptInfo->iOffset)
	{
		// file meta info
		assert(0 == ptInfo->iDirectIOOffset);
		assert(pBuffer == pNextBlock - iBlockSize);
		LogFileMetaInfo tFileMetaInfo = {0};
		tFileMetaInfo.cMode = 0;
		tFileMetaInfo.llStartBlkSeq = ptInfo->llStartBlkSeq;
		tFileMetaInfo.iBlockSize = iBlockSize;
		DumpFileMetaInfo(tFileMetaInfo, pBuffer, pNextBlock);
		pBuffer = pNextBlock;
		pNextBlock = (pNextBlock == pDirectBufferEnd)
			? pDirectBufferEnd : pNextBlock + iBlockSize;
	}

	char* pNewBuffer = NULL;
	for (size_t i = 0; i < vecValue.size(); ++i)
	{
		const leveldb::Slice& tSlice = vecValue[i];
		assert(false == tSlice.empty());
		assert(NULL != tSlice.data());

		const char* pValueIter = NULL;

		// => adjust buffer before write
		if (pNextBlock - pBuffer <= ptInfo->iMinTailRecordSize)
		{
			pBuffer = pNextBlock;
			pNextBlock = (pNextBlock == pDirectBufferEnd)
				? pDirectBufferEnd : pNextBlock + iBlockSize;
		}
		
		uint32_t iOffset = 
			ptInfo->iDirectIOOffset + (pBuffer - ptInfo->pDirectIOBuffer);
		while (true)
		{
			if (pBuffer == pNextBlock)
			{
				assert(pDirectBufferEnd == pBuffer);
				return DB_WRTIE_ERROR_LARGE_BATCH;
			}

			assert(pBuffer < pNextBlock);
			assert(pNextBlock <= pDirectBufferEnd);
			assert(pNextBlock - pBuffer > ptInfo->iMinTailRecordSize);

			assert(0 == (pNextBlock - ptInfo->pDirectIOBuffer) % iBlockSize);
			pValueIter = DumpValueImpl(
					ptInfo->oSeqGen, iBlockSize, ptInfo->iMinTailRecordSize, 
					pBuffer, pNextBlock, tSlice.data(), tSlice.size(), pValueIter, 
					pNewBuffer);

			if (tSlice.data() + tSlice.size() == pValueIter)
			{
				break; // complete
			}

			assert(NULL != pValueIter);
			assert(pValueIter > tSlice.data());
			assert(pValueIter < tSlice.data() + tSlice.size());

			pBuffer = pNextBlock;
			pNextBlock = (pNextBlock == pDirectBufferEnd)
				? pDirectBufferEnd : pNextBlock + iBlockSize;
		}

		// complete one tSlice dump
		// => keep pNewBuffer unchanged!! 
		// => which will be used to calculate the write size
		assert(pBuffer < pNewBuffer);
		assert(pNewBuffer <= pNextBlock);
		pBuffer = pNewBuffer;

		// commit iOffset into vecOffset
		vecOffset.push_back(0 == iOffset ? iBlockSize : iOffset);
	}

	assert(NULL != pNewBuffer);
	assert(pNewBuffer <= pNextBlock);
	// success batch dump
	assert(pNextBlock <= pDirectBufferEnd);
	// NOTICE: 
	// even SafeDirectIOPWrite may split whole buffer into multiple write
	// => which may dump serverial valid record into disk, but
	//    this batch write will claim failed. !!!!
	
	ZeroTailingSpace(iBlockSize, pNewBuffer, pNextBlock);
	int ret = SmallWriteImpl(ptInfo, pBeginBlock, pNextBlock);
	if (0 > ret)
	{
		return ret;  // ERROR CASE
	}

	assert(ret == pNextBlock - pBeginBlock);
	assert(ret == iEstimateWriteSize);
	// success write
	int iWriteSize = CalculateWriteSize(ptInfo, pNewBuffer);
	assert(0 < iWriteSize);
	assert(iWriteSize == iBatchDumpSize);
	return iWriteSize;
}

int DumpToDirectIOBuffer(
		DirectIOWriteInfo* ptInfo, 
		const char* pValue, const int iValLen, 
		const int iDumpSize, const int iEstimateWriteSize)
{
	const int iBlockSize = ptInfo->iBlockSize;
	assert(0 < iBlockSize);
	assert(ptInfo->iOffset >= ptInfo->iDirectIOOffset);
	assert(ptInfo->iOffset <= ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize);

	char* pBuffer = GetCurrentPos(ptInfo);
	char* pBeginBlock = GetPrevBlockPos(ptInfo, pBuffer);
	char* pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	char* pNextBlock = (pBeginBlock == pDirectBufferEnd)
			? pDirectBufferEnd : pBeginBlock + iBlockSize;
	assert(pNextBlock <= pDirectBufferEnd);
	assert(pNextBlock - pBeginBlock <= iBlockSize);
	// assert(pNextBlock != pDirectBufferEnd);
	if (0 == ptInfo->iOffset)
	{
		// file meta info
		assert(0 == ptInfo->iDirectIOOffset);
		assert(pBuffer == pNextBlock - iBlockSize);
		assert(pNextBlock != pDirectBufferEnd);
		LogFileMetaInfo tFileMetaInfo = {0};
		tFileMetaInfo.cMode = 0;
		tFileMetaInfo.llStartBlkSeq = ptInfo->llStartBlkSeq;
		tFileMetaInfo.iBlockSize = iBlockSize;
		DumpFileMetaInfo(tFileMetaInfo, pBuffer, pNextBlock);
		pBuffer = pNextBlock;
		pNextBlock = (pNextBlock == pDirectBufferEnd)
			? pDirectBufferEnd : pNextBlock + iBlockSize;
		assert(pNextBlock - pBuffer <= iBlockSize);
		// assert(pNextBlock != pDirectBufferEnd);
	}

	char* pNewBuffer = NULL;
	const char* pValueIter = NULL;
	while (true)
	{
		// assert(pBuffer != pNextBlock);
		if (pBuffer == pNextBlock)
		{
			assert(pDirectBufferEnd == pBuffer);
			assert(pDirectBufferEnd == 
					ptInfo->pDirectIOBuffer + ptInfo->iMaxDirectIOBufSize);
			return DB_WRITE_ERROR_LARGE_RECORD;
		}

		assert(pBuffer < pNextBlock);
		assert(pNextBlock <= pDirectBufferEnd);
		assert(pNextBlock - pBuffer > ptInfo->iMinTailRecordSize);
		assert(pNextBlock - pBuffer <= iBlockSize);

		assert(0 == (pNextBlock - ptInfo->pDirectIOBuffer) % iBlockSize);
		pValueIter = DumpValueImpl(
				ptInfo->oSeqGen, iBlockSize, ptInfo->iMinTailRecordSize, 
				pBuffer, pNextBlock, pValue, iValLen, pValueIter, 
				pNewBuffer);
		if (pValue + iValLen == pValueIter)
		{
			break; // dump complete
		}

		assert(NULL != pValueIter);
		assert(pValueIter > pValue);
		assert(pValueIter < pValue + iValLen);

		pBuffer = pNextBlock;
		pNextBlock = (pNextBlock == pDirectBufferEnd)
			? pDirectBufferEnd : pNextBlock + iBlockSize;

		assert(pNextBlock - pBuffer <= iBlockSize);
	}

	assert(NULL != pNewBuffer);
	assert(pBuffer < pNewBuffer);
	assert(pNewBuffer <= pNextBlock);
	assert(pNextBlock - pBeginBlock == iEstimateWriteSize);
	assert(pNextBlock - ptInfo->pDirectIOBuffer + ptInfo->iDirectIOOffset
			<= MAX_LOG_FILE_SIZE);

	int iWriteSize = CalculateWriteSize(ptInfo, pNewBuffer);
	assert(0 < iWriteSize);
	assert(iWriteSize == iDumpSize);
	return iWriteSize;
}

int DirectIOWriteImpl(
		DirectIOWriteInfo* ptInfo, const char* pValue, int iValLen)
{
	assert(NULL != ptInfo);
	assert(NULL != pValue);
	assert(0 < iValLen);
	assert(0 == ptInfo->iWriteStatus);

	// iDumpSize : include FileMetaInfo if any 
	int iDumpSize = 0;
	int iEstimateWriteSize = 0;
	int iAdjustCpySize = 
		AdjustStrategy::AdjustDirectIOWriteBuffer(
			ptInfo, iValLen, iDumpSize, iEstimateWriteSize);
	if (0 > iAdjustCpySize)
	{
		return iAdjustCpySize;
	}
	assert(iAdjustCpySize < ptInfo->iBlockSize);
	assert(iValLen < iDumpSize);
	assert(iDumpSize <= iEstimateWriteSize);

	int iWriteSize = DumpAndWrite(
			ptInfo, pValue, iValLen, iDumpSize, iEstimateWriteSize);
	if (0 < iWriteSize)
	{
		assert(iWriteSize == iDumpSize);
		assert(iWriteSize <= iEstimateWriteSize);
		// success write
		// upload stat info
		++(ptInfo->llAccCnt);
		ptInfo->llAccValueLen += iValLen;
		ptInfo->llAccDumpSize += iDumpSize;
		ptInfo->llAccWriteSize += iEstimateWriteSize;
	}
	return iWriteSize;
}


int BatchDirectIOWriteImpl(
		DirectIOWriteInfo* ptInfo, 
		const std::vector<leveldb::Slice>& vecValue, 
		std::vector<uint32_t>& vecOffset)
{
	assert(NULL != ptInfo);
	assert(false == vecValue.empty());
	assert(0 == ptInfo->iWriteStatus);

	int iBatchDumpSize = 0;
	int iEstimateWriteSize = 0;
	int iAdjustCpySize = 
		AdjustStrategy::AdjustDirectIOWriteBuffer(
			ptInfo, vecValue, iBatchDumpSize, iEstimateWriteSize);
	if (0 > iAdjustCpySize)
	{
		// DB_WRITE_ERROR_LARGE_WRITE
		// or DB_WRITE_ERROR_NEXT_FILE
		return iAdjustCpySize; // ERROR CASE
	}
	assert(iAdjustCpySize < ptInfo->iBlockSize);
	assert(0 < iBatchDumpSize);
	assert(iBatchDumpSize <= iEstimateWriteSize);
	int iWriteSize = BatchDumpAndWrite(
			ptInfo, vecValue, iBatchDumpSize, iEstimateWriteSize, vecOffset);
	if (0 < iWriteSize)
	{
		assert(iWriteSize == iBatchDumpSize);
		assert(iWriteSize <= iEstimateWriteSize);
		assert(vecValue.size() == vecOffset.size());
		int iBatchValLen = 0;
		for (size_t i = 0; i < vecValue.size(); ++i)
		{
			iBatchValLen += vecValue[i].size();
		}

		++(ptInfo->llAccBatchCnt);
		ptInfo->llAccBatchSize += vecValue.size();
		ptInfo->llAccBatchValueLen += iBatchValLen;
		ptInfo->llAccBatchDumpSize += iBatchDumpSize;
		ptInfo->llAccBatchWriteSize += iEstimateWriteSize;
	}
	return iWriteSize;
}

int FlushDirectIOBufferImpl(DirectIOWriteInfo* ptInfo)
{
	assert(NULL != ptInfo);
	assert(INVALID_FD != ptInfo->iDirectFD);

	const int iBlockSize = ptInfo->iBlockSize;
	assert(0 < iBlockSize);
	if (ptInfo->iOffset == ptInfo->iDirectIOOffset)
	{
		return 0; // flush out nothing;
	}

	assert(ptInfo->iOffset > ptInfo->iDirectIOOffset);
	assert(ptInfo->iOffset <= ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize);
	assert(ptInfo->iOffset <= MAX_LOG_FILE_SIZE);

	char* pBuffer = GetCurrentPos(ptInfo);
	char* const pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	
	int iBlkOffset = (pBuffer - ptInfo->pDirectIOBuffer) % iBlockSize;
	char* const pEndBlock = 
		0 == iBlkOffset ? pBuffer : pBuffer - iBlkOffset + iBlockSize;

	assert(pEndBlock <= pDirectBufferEnd);
	assert((0 == iBlkOffset && pBuffer == pEndBlock)
			|| (0 != iBlkOffset && pBuffer < pEndBlock));
	assert(pEndBlock - ptInfo->pDirectIOBuffer + ptInfo->iDirectIOOffset
			>= ptInfo->iOffset);
	assert(pEndBlock - ptInfo->pDirectIOBuffer + ptInfo->iDirectIOOffset
			<= MAX_LOG_FILE_SIZE);

	assert(static_cast<uint32_t>(iBlockSize) <= ptInfo->iOffset);
	// redo append fake: prevent write succ, write error, flush case
	assert(0 == (pEndBlock - ptInfo->pDirectIOBuffer) % iBlockSize);
	AppendFakeLevelDBRecord(
			iBlockSize, ptInfo->iMinTailRecordSize, pBuffer, pEndBlock);
	assert(ptInfo->pDirectIOBuffer < pEndBlock);
	int iFlushWriteSize = SmallWriteImpl(ptInfo, ptInfo->pDirectIOBuffer, pEndBlock);
	if (0 > iFlushWriteSize)
	{
		return iFlushWriteSize; // ERROR CASE
	}

	assert(iFlushWriteSize == (pEndBlock - ptInfo->pDirectIOBuffer));
	assert(iFlushWriteSize <= ptInfo->iMaxDirectIOBufSize);

	// success write
	// shift: iDirectIOOffset
	assert(iBlkOffset == static_cast<int>(ptInfo->iOffset) % iBlockSize);
	assert(0 <= iBlkOffset);
	assert(iBlkOffset < iBlockSize);
	
	uint32_t iNewDirectIOOffset = ptInfo->iOffset - iBlkOffset;
	assert(iNewDirectIOOffset >= ptInfo->iDirectIOOffset);
	assert(iNewDirectIOOffset <= MAX_LOG_FILE_SIZE);
	ptInfo->iDirectIOOffset = iNewDirectIOOffset;
	if (0 == iBlkOffset)
	{
		assert(ptInfo->iDirectIOOffset == ptInfo->iOffset);
		return iFlushWriteSize;
	}

	if (ptInfo->pDirectIOBuffer != pEndBlock - iBlockSize)
	{
		assert(ptInfo->pDirectIOBuffer < pEndBlock - iBlockSize);
		// else => move buffer around !!
		memcpy(ptInfo->pDirectIOBuffer, pEndBlock - iBlockSize, iBlkOffset);
		AppendFakeLevelDBRecord(
				iBlockSize, ptInfo->iMinTailRecordSize, 
				ptInfo->pDirectIOBuffer + iBlkOffset, 
				ptInfo->pDirectIOBuffer + iBlockSize);
	}
	assert(ptInfo->iDirectIOOffset < ptInfo->iOffset);
	assert(ptInfo->iDirectIOOffset + iBlockSize > ptInfo->iOffset);
	assert(ptInfo->iOffset + ptInfo->iMinTailRecordSize < 
			ptInfo->iDirectIOOffset + iBlockSize);
	return iFlushWriteSize;
}


int DirectIOBufferWriteImpl(
		DirectIOWriteInfo* ptInfo, const char* pValue, int iValLen)
{
	assert(kMaxCompactStrategy == ptInfo->iAdjustStrategy);
	assert(NULL != ptInfo);
	assert(NULL != pValue);
	assert(0 < iValLen);
	assert(0 == ptInfo->iWriteStatus);

	int iDumpSize = CalculateDumpSize(ptInfo, iValLen);
	assert(0 < iDumpSize);
	int iEstimateWriteSize = GetEstimateWriteSize(ptInfo, iDumpSize); 
	assert(iDumpSize <= iEstimateWriteSize);
	if (iEstimateWriteSize > ptInfo->iMaxDirectIOBufSize)
	{
		return DB_WRITE_ERROR_LARGE_WRITE;
	}

	if (NeedIterWriteFile(ptInfo, iEstimateWriteSize))
	{
		return DB_WRITE_ERROR_NEXT_FILE;
	}

	assert(iEstimateWriteSize <= ptInfo->iMaxDirectIOBufSize);
	uint32_t iPrevAlignOffset = LowerAlignment(ptInfo->iOffset, ptInfo->iBlockSize);
	if (iPrevAlignOffset + iEstimateWriteSize
			> ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize)
	{
		// the rest of pDirectIOBuffer can't hold this write
		// => need flush the buffer into disk
		int iFlushWriteSize = FlushDirectIOBufferImpl(ptInfo);
		if (0 > iFlushWriteSize)
		{
			return iFlushWriteSize; // error case
		}

		assert(0 < iFlushWriteSize);
		assert(ptInfo->iOffset < ptInfo->iDirectIOOffset + ptInfo->iBlockSize);
		assert(iDumpSize == CalculateDumpSize(ptInfo, iValLen));
		assert(iEstimateWriteSize == GetEstimateWriteSize(ptInfo, iDumpSize));
	}

	int iWriteSize = DumpToDirectIOBuffer(
			ptInfo, pValue, iValLen, iDumpSize, iEstimateWriteSize);
	assert(0 < iWriteSize);
	assert(iWriteSize == iDumpSize);
	++(ptInfo->llAccCnt);
	ptInfo->llAccValueLen += iValLen;
	ptInfo->llAccDumpSize += iDumpSize;
	ptInfo->llAccWriteSize += iEstimateWriteSize;
	return iWriteSize;
}

void IncreaseOffset(DirectIOWriteInfo* ptInfo, const int iWriteSize)
{
	assert(NULL != ptInfo);
	assert(0 <= iWriteSize);
	uint32_t iNewOffset = ptInfo->iOffset + iWriteSize;
	int iBlkTailingSpace = ptInfo->iBlockSize - iNewOffset % ptInfo->iBlockSize;
	if (iBlkTailingSpace <= ptInfo->iMinTailRecordSize)
	{
		iNewOffset += iBlkTailingSpace; // skip iBlkTailingSpace
	}

	assert(iNewOffset >= ptInfo->iDirectIOOffset);
	assert(iNewOffset <= ptInfo->iDirectIOOffset + ptInfo->iMaxDirectIOBufSize);
	ptInfo->iOffset = iNewOffset;
	assert(ptInfo->iOffset <= MAX_LOG_FILE_SIZE);
	return ;
}

int DirectIOWrite(DirectIOWriteInfo* ptInfo, 
		const char* pValue, const int iValLen, uint32_t& iWriteOffset)
{
	uint64_t llBeginTime = dbcomm::GetTickMS();
	assert(NULL != ptInfo);
	if (false == IsValid(ptInfo))
	{
		FixDirectIOWriteInfo(ptInfo);
	}

	assert(true == IsValid(ptInfo));
	if (NULL == pValue || 0 >= iValLen)
	{
		return -1;
	}

	assert(NULL != pValue);
	assert(0 < iValLen);
	// ptInfo->iOffset may change due to AdjustStrategy
	int iWriteSize = DirectIOWriteImpl(ptInfo, pValue, iValLen);
	assert(0 != iWriteSize);
	if (0 > iWriteSize)
	{
		ptInfo->iWriteStatus = iWriteSize;
		return ptInfo->iWriteStatus;
	}

	ptInfo->iWriteStatus = 0;
	iWriteOffset = ptInfo->iOffset;
	iWriteOffset = 0 == iWriteOffset ? ptInfo->iBlockSize : iWriteOffset;
	IncreaseOffset(ptInfo, iWriteSize);
	assert(iWriteOffset < ptInfo->iOffset);
	// stat
	ptInfo->llAccCostTime += dbcomm::GetTickMS() - llBeginTime;
	return 0;
}

int BatchDirectIOWrite(
		DirectIOWriteInfo* ptInfo, 
		const std::vector<leveldb::Slice>& vecValue, 
		std::vector<uint32_t>& vecOffset)
{
	uint64_t llBeginTime = dbcomm::GetTickMS();
	assert(NULL != ptInfo);
	if (false == IsValid(ptInfo))
	{
		FixDirectIOWriteInfo(ptInfo);
	}

	assert(true == IsValid(ptInfo));
	if (vecValue.empty())
	{
		return -1;
	}

	assert(false == vecValue.empty());
	int iWriteSize = BatchDirectIOWriteImpl(ptInfo, vecValue, vecOffset);
	assert(0 != iWriteSize);
	if (0 > iWriteSize)
	{
		ptInfo->iWriteStatus = iWriteSize;
		return ptInfo->iWriteStatus;
	}

	ptInfo->iWriteStatus = 0;
	IncreaseOffset(ptInfo, iWriteSize);
	ptInfo->llAccBatchCostTime += dbcomm::GetTickMS() - llBeginTime;
	return 0;
}

int DirectIOBufferWrite(
		DirectIOWriteInfo* ptInfo, 
		const char* pValue, int iValLen, uint32_t& iWriteOffset)
{
	uint64_t llBeginTime = dbcomm::GetTickMS();
	assert(NULL != ptInfo);
	if (false == IsValid(ptInfo))
	{
		FixDirectIOWriteInfo(ptInfo);
	}

	assert(true == IsValid(ptInfo));
	if (NULL == pValue || 0 >= iValLen)
	{
		return -1;
	}

	assert(NULL != pValue);
	assert(0 < iValLen);
	int iWriteSize = DirectIOBufferWriteImpl(ptInfo, pValue, iValLen);
	assert(0 != iWriteSize);
	if (0 > iWriteSize)
	{
		ptInfo->iWriteStatus = iWriteSize;
		return ptInfo->iWriteStatus;
	}

	ptInfo->iWriteStatus = 0;
	iWriteOffset = ptInfo->iOffset;
	iWriteOffset = 0 == iWriteOffset ? ptInfo->iBlockSize : iWriteOffset;
	IncreaseOffset(ptInfo, iWriteSize);
	assert(iWriteOffset < ptInfo->iOffset);
	ptInfo->llAccCostTime += dbcomm::GetTickMS() - llBeginTime;
	return 0;
}

int GetDirectIOBufferUsedSize(
		const DirectIOWriteInfo* ptInfo, uint32_t& iUsedSize)
{
	if (false == IsValid(ptInfo))
	{
		return -1;
	}

	assert(true == IsValid(ptInfo));
	assert(ptInfo->iOffset >= ptInfo->iDirectIOOffset);
	iUsedSize = ptInfo->iOffset - ptInfo->iDirectIOOffset;
	return 0;
}

int GetDirectIOBufferUsedBlockSize(
		const DirectIOWriteInfo* ptInfo, uint32_t& iUsedBlockSize)
{
	uint32_t iUsedSize = 0;
	int ret = GetDirectIOBufferUsedSize(ptInfo, iUsedSize);
	if (0 != ret)
	{
		return ret;
	}

	iUsedBlockSize = UpperAlignment(iUsedSize, ptInfo->iBlockSize);
	assert(iUsedBlockSize >= iUsedSize);
	assert(iUsedBlockSize < iUsedSize + ptInfo->iBlockSize);
	return 0;
}

uint64_t GetDirectIOBufferAccWriteSize(const DirectIOWriteInfo* ptInfo)
{
	return ptInfo->llAccWriteSize + ptInfo->llAccBatchWriteSize;
}

int Flush(DirectIOWriteInfo* ptInfo)
{
	assert(NULL != ptInfo);
	if (INVALID_FD == ptInfo->iDirectFD)
	{
		return -1;
	}

	if (false == IsValid(ptInfo))
	{
		FixDirectIOWriteInfo(ptInfo);
	}

	assert(true == IsValid(ptInfo));
	int iFlushWriteSize = FlushDirectIOBufferImpl(ptInfo);
	if (0 > iFlushWriteSize)
	{
		ptInfo->iWriteStatus = iFlushWriteSize;
		return ptInfo->iWriteStatus;
	}

	assert(0 <= iFlushWriteSize);
	assert(ptInfo->iOffset <= MAX_LOG_FILE_SIZE);
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	return 0;
}


// DirectIOReadInfo

DirectIOReadInfo::DirectIOReadInfo(
		const int iBlockSizeI, const uint64_t llStartBlkSeqI, 
		const int iMaxDirectIOBufSizeI)
	: iMode(READINFO_MODE_NORMAL)
	, iBlockSize(iBlockSizeI)
	, llStartBlkSeq(llStartBlkSeqI)
	, iMaxDirectIOBufSize(iMaxDirectIOBufSizeI)
	, iDirectFD(INVALID_FD)
	, iOffset(0)
	, iDirectIOBufSize(0)
	, iDirectIOOffset(0)
	, pDirectIOBuffer(NULL)
	, iReadStatus(0)
{
	assert(0 < iBlockSize);
	assert(2 * iBlockSize <= iMaxDirectIOBufSize);
	assert(0 == iMaxDirectIOBufSize % iBlockSize);

	pDirectIOBuffer = AllocDirectIOBuffer(iMaxDirectIOBufSize);
	assert(NULL != pDirectIOBuffer);
}

DirectIOReadInfo::DirectIOReadInfo(
		const int iBlockSizeI, const uint64_t llStartBlkSeqI, 
		char* pDirectIOBufferI, const int iMaxDirectIOBufSizeI)
	: iMode(READINFO_MODE_DELEGATE)
	, iBlockSize(iBlockSizeI)
	, llStartBlkSeq(llStartBlkSeqI)
	, iMaxDirectIOBufSize(iMaxDirectIOBufSizeI)
	, iDirectFD(INVALID_FD)
	, iOffset(0)
	, iDirectIOBufSize(0)
	, iDirectIOOffset(0)
	, pDirectIOBuffer(pDirectIOBufferI)
	, iReadStatus(0)
{
	assert(0 < iBlockSize);
	assert(2 * iBlockSize <= iMaxDirectIOBufSize);
	assert(0 == iMaxDirectIOBufSize % iBlockSize);

	assert(NULL != pDirectIOBuffer);
}

DirectIOReadInfo::DirectIOReadInfo(
		const int iBlockSizeI, const uint64_t llStartBlkSeqI, 
		const char* pBegin, const char* pEnd)
	: iMode(READINFO_MODE_DELEGATE_READONLY)
	, iBlockSize(iBlockSizeI)
	, llStartBlkSeq(llStartBlkSeqI)
	, iMaxDirectIOBufSize(LowerAlignment(pEnd-pBegin, iBlockSizeI))
	, iDirectFD(INVALID_FD)
	, iOffset(0)
	, iDirectIOBufSize(iMaxDirectIOBufSize)
	, iDirectIOOffset(0)
	, pDirectIOBuffer(const_cast<char*>(pBegin))
	, iReadStatus(0)
{
	// const char* => never call reload on pDirectIOBuffer
	// => it's safe
	assert(0 < iBlockSize);
	assert(0 == iMaxDirectIOBufSize % iBlockSize);

	assert(NULL != pDirectIOBuffer);
}

DirectIOReadInfo::~DirectIOReadInfo()
{
	if (INVALID_FD != iDirectFD)
	{
		close(iDirectFD);
		iDirectFD = INVALID_FD;
	}

	if (NULL != pDirectIOBuffer && CheckMode(iMode, READINFO_MODE_NORMAL))
	{
		free(pDirectIOBuffer);
		pDirectIOBuffer = NULL;
	}
}


int CreateADirectIOReadInfo(
		const char* sLevelDBLogFile, 
		int iMaxDirectIOBufSize, DirectIOReadInfo*& ptInfo)
{
	assert(NULL == ptInfo);
	int iNewDirectFD = OpenForDirectIORead(sLevelDBLogFile);
	if (0 > iNewDirectFD)
	{
		logerr("OpenForDirectIORead %s ret %d", sLevelDBLogFile, iNewDirectFD);
		return -1;
	}

	CREATE_FD_MANAGER(iNewDirectFD);
	assert(0 <= iMaxDirectIOBufSize);
	int iPageSize = getpagesize();
	assert(0 < iPageSize);
	iMaxDirectIOBufSize = max(iPageSize, iMaxDirectIOBufSize);
	assert(0 == iMaxDirectIOBufSize % iPageSize);

	char* pBlockBuffer = AllocDirectIOBuffer(iMaxDirectIOBufSize);
	assert(NULL != pBlockBuffer);
	CREATE_MALLOC_MEM_MANAGER(pBlockBuffer);

	int iReadSize = SafePRead(iNewDirectFD, pBlockBuffer, iPageSize, 0);
	if (iPageSize != iReadSize)
	{
		logerr("SafePRead %s iPageSize %d iReadSize %d", 
				sLevelDBLogFile, iPageSize, iReadSize);
		// EOF => empty file 
		if (0 == iReadSize)
		{
			return 2;
		}

		assert(0 < iReadSize);
		return DB_READ_ERROR_POOR_READ;
	}

	LogFileMetaInfo tFileMetaInfo = {0};
	assert(iPageSize == iReadSize);
	int ret = TryPickleFileMetaInfo(
			pBlockBuffer, pBlockBuffer + iReadSize, tFileMetaInfo);
	if (0 != ret)
	{
		logerr("TryPickleFileMetaInfo %s ret %d", sLevelDBLogFile, ret);
		// ret == 1 <=> old bitcask file format;
		return 1 == ret ? 1 : -3;
	}

	// min blk size: page size
	if (iPageSize > tFileMetaInfo.iBlockSize
			// max blk size: 256 MB
			|| 256 * 1024 * 1024 < tFileMetaInfo.iBlockSize 
			|| INVALID_BLK_SEQ == tFileMetaInfo.llStartBlkSeq)
	{
		logerr("iPageSize %d iBlockSize %d llStartBlkSeq %ld", 
				iPageSize, tFileMetaInfo.iBlockSize, tFileMetaInfo.llStartBlkSeq);
		return -4;
	}

	int iNewMaxDirectIOBufSize = LowerAlignment(
			iMaxDirectIOBufSize, tFileMetaInfo.iBlockSize);
	iNewMaxDirectIOBufSize = max(iNewMaxDirectIOBufSize, 2 * tFileMetaInfo.iBlockSize);
	if (iNewMaxDirectIOBufSize > iMaxDirectIOBufSize)
	{
		// only if:
		assert(iNewMaxDirectIOBufSize == tFileMetaInfo.iBlockSize * 2);
		// drop pBlockBuffer
		ptInfo = new DirectIOReadInfo(
				tFileMetaInfo.iBlockSize, tFileMetaInfo.llStartBlkSeq, 
				iNewMaxDirectIOBufSize); // 
	}
	else
	{
		assert(iNewMaxDirectIOBufSize <= iMaxDirectIOBufSize);
		// sink pBlockBuffer into DirectIOReadInfo
		char* pSinkBlockBuffer = NULL;
		swap(pSinkBlockBuffer, pBlockBuffer);
		assert(NULL == pBlockBuffer);
		ptInfo = new DirectIOReadInfo(
				tFileMetaInfo.iBlockSize, tFileMetaInfo.llStartBlkSeq, 
				pSinkBlockBuffer, iNewMaxDirectIOBufSize);
		ptInfo->iMode = READINFO_MODE_NORMAL; // ptInfo own this buffer
	}
	assert(NULL != ptInfo);
	assert(0 == ptInfo->iDirectIOBufSize);

	swap(ptInfo->iDirectFD, iNewDirectFD);
	assert(INVALID_FD != ptInfo->iDirectFD);
	assert(INVALID_FD == iNewDirectFD);

	ptInfo->iOffset = ptInfo->iBlockSize;
	ptInfo->iDirectIOOffset = ptInfo->iOffset;
	return 0;
}

int CreateADirectIOReadInfoReadOnly(
		const char* pBegin, const char* pEnd, 
		const uint32_t iOffset , DirectIOReadInfo*& ptNewRInfo)
{
	assert(NULL != pBegin);
	assert(NULL != pEnd);

	LogFileMetaInfo tFileMetaInfo = {0};
	int ret = TryPickleFileMetaInfo(pBegin, pEnd, tFileMetaInfo);
	if (0 != ret)
	{
		return ret;
	}

	const int iPageSize = getpagesize();
	assert(0 <= iPageSize);
	if (iPageSize > tFileMetaInfo.iBlockSize
			// max blk size: 256 MB
			|| 256 * 1024 * 1024 < tFileMetaInfo.iBlockSize 
			|| INVALID_BLK_SEQ == tFileMetaInfo.llStartBlkSeq)
	{
		logerr("tFile: iBlockSize %d llStartBlkSeq %ld", 
				tFileMetaInfo.iBlockSize, tFileMetaInfo.llStartBlkSeq);
		return -4;
	}

	int iBufSize = pEnd - pBegin;
	assert(0 <= iBufSize);
	if (tFileMetaInfo.iBlockSize > iBufSize)
	{
		logerr("tFile: iBlockSize %d iBufSize %d %ld", 
				tFileMetaInfo.iBlockSize, iBufSize, pEnd - pBegin);
		return -5;
	}

	ptNewRInfo = new DirectIOReadInfo(
				tFileMetaInfo.iBlockSize, 
				tFileMetaInfo.llStartBlkSeq, 
				// skip the first block
				pBegin + tFileMetaInfo.iBlockSize, pEnd);
				// further operation on ptNewRInfo won't write [pBegin, pEnd]
//				iBufSize, const_cast<char*>(pBegin));
	uint32_t iNewOffset = max<uint32_t>(ptNewRInfo->iBlockSize, iOffset);
	iNewOffset = min<uint32_t>(iBufSize, iNewOffset);
	{
		const int iBlockSize = ptNewRInfo->iBlockSize;
		assert(0 < iBlockSize);
		iNewOffset = (iNewOffset + iBlockSize - 1) / iBlockSize * iBlockSize;
	}
	ptNewRInfo->iOffset = iNewOffset;
	assert(static_cast<uint32_t>(ptNewRInfo->iBlockSize) <= ptNewRInfo->iOffset);
	ptNewRInfo->iDirectIOOffset = ptNewRInfo->iBlockSize;
	return 0;
}

int CreateADirectIOReadInfo(
		const char* sLevelDBLogFile, 
		char* const pDirectIOBuffer, 
		const int iMaxDirectIOBufSize, DirectIOReadInfo*& ptInfo)
{
	assert(NULL == ptInfo);
	assert(NULL != pDirectIOBuffer);
	assert(0 <= iMaxDirectIOBufSize);
	int iPageSize = getpagesize();
	assert(0 < iPageSize);
	if (iMaxDirectIOBufSize < 2 * iPageSize)
	{
		return -1;
	}

	int iNewDirectFD = OpenForDirectIORead(sLevelDBLogFile);
	if (0 > iNewDirectFD)
	{
		logerr("OpenForDirectIORead %s ret %d", sLevelDBLogFile, iNewDirectFD);
		return -2;
	}

	// fd manager hold reference to iNewDirectFD
	// => close only if iNewDirectFD >= 0;
	CREATE_FD_MANAGER(iNewDirectFD);
	int iReadSize = SafePRead(iNewDirectFD, pDirectIOBuffer, iPageSize, 0);
	if (iPageSize != iReadSize)
	{
		logerr("SafePRead %s iPageSize %d iReadSize %d", 
				sLevelDBLogFile, iPageSize, iReadSize);
		// EOFO => empty file
		if (0 == iReadSize)
		{
			return 2;
		}

		return DB_READ_ERROR_POOR_READ;
	}

	LogFileMetaInfo tFileMetaInfo = {0};
	int ret = TryPickleFileMetaInfo(
			pDirectIOBuffer, pDirectIOBuffer + iReadSize, tFileMetaInfo);
	if (0 != ret)
	{
		logerr("TryPickleFileMetaInfo %s ret %d", sLevelDBLogFile, ret);
		return 1 == ret ? 1 : -3;
	}

	if (iPageSize > tFileMetaInfo.iBlockSize
			|| iMaxDirectIOBufSize < 2 * tFileMetaInfo.iBlockSize
			// max blk size: 256 MB
			|| 256 * 1024 * 1024 < tFileMetaInfo.iBlockSize 
			|| INVALID_BLK_SEQ == tFileMetaInfo.llStartBlkSeq)
	{
		logerr("iPageSize %d iBlockSize %d iMaxDirectIOBufSize %d "
				"llStartBlkSeq %ld", iPageSize, tFileMetaInfo.iBlockSize, 
				iMaxDirectIOBufSize, tFileMetaInfo.llStartBlkSeq);
		return -4;
	}

	int iNewMaxDirectIOBufSize = LowerAlignment(
			iMaxDirectIOBufSize, tFileMetaInfo.iBlockSize);
	assert(2 * tFileMetaInfo.iBlockSize <= iNewMaxDirectIOBufSize);
	ptInfo = new DirectIOReadInfo(
			tFileMetaInfo.iBlockSize, tFileMetaInfo.llStartBlkSeq, 
			pDirectIOBuffer, iNewMaxDirectIOBufSize);
	assert(NULL != ptInfo);
	assert(0 == ptInfo->iDirectIOBufSize);

	// shift the owner-ship of FD from iNewDirectFD to iDirectFD;
	// => no close attemp, and no fd leak!
	swap(ptInfo->iDirectFD, iNewDirectFD);
	assert(INVALID_FD != ptInfo->iDirectFD);
	assert(INVALID_FD == iNewDirectFD);

	ptInfo->iOffset = ptInfo->iBlockSize;
	ptInfo->iDirectIOOffset = ptInfo->iOffset;
	return 0;
}


inline bool IsValid(const DirectIOReadInfo* ptInfo)
{
	assert(NULL != ptInfo);
	return 0 == ptInfo->iReadStatus;
}

void CheckReloadDirectIOBuffer(
		const DirectIOReadInfo* ptInfo, 
		const char* pBegin, const char* pEnd)
{
	const int iBlockSize = ptInfo->iBlockSize;
	const char* pBuffer = pBegin;
	const char* pNextBlock = pBuffer + iBlockSize;
	assert(pNextBlock <= pEnd);

	if (0 == ptInfo->iDirectIOOffset)
	{
		pBuffer = pNextBlock;
		pNextBlock = pNextBlock == pEnd ? pEnd : pNextBlock + iBlockSize;
	}

	while (pBuffer < pEnd)
	{
		assert(pBuffer < pNextBlock);
		RLogLevelDBRecord tLogRecord = {0};
		PickleLevelDBRecord(
				pBuffer, pNextBlock, tLogRecord, true);
		bool bReadComplete = false;
		int ret = CheckLevelDBRecord(
				ptInfo->llStartBlkSeq, tLogRecord, 
				true, false, bReadComplete);
		if (0 != ret
				&& DB_FORMAT_ERROR_DEPRECATE_BLK != ret)
		{
			printf ( "iOffset %u iDirectIOOffset %u iDirectIOBufSize %u pDirectIOBuffer %p pBuffer %p pNextBlock %p "
					"tLogRecord.cRecordType %u tLogRecord.hLength %u\n", 
					ptInfo->iOffset, ptInfo->iDirectIOOffset, ptInfo->iDirectIOBufSize, ptInfo->pDirectIOBuffer, pBuffer, pNextBlock, 
					tLogRecord.cRecordType, tLogRecord.hLength );
			if (DB_FORMAT_ERROR_BLOCKMETA_MISSING == ret)
			{
				uint32_t iTestOffset = ptInfo->iDirectIOOffset
					+ (pBuffer - ptInfo->pDirectIOBuffer);
				char* pTestBuffer = AllocDirectIOBuffer(ptInfo->iBlockSize);
				CREATE_MALLOC_MEM_MANAGER(pTestBuffer);
				int iSmallReadSize = SafeDirectIOPRead(ptInfo->iDirectFD, 
						pTestBuffer, iBlockSize, iTestOffset);
				assert(iSmallReadSize == iBlockSize);
				RLogLevelDBRecord tTestLogRecord = {0};
				PickleLevelDBRecord(
						pTestBuffer, pTestBuffer + iBlockSize, tTestLogRecord, true);
				printf ( "iTestOffset %u tTestLogRecord.cRecordType %u tTestLogRecord.hLength %u\n", 
						iTestOffset, tTestLogRecord.cRecordType, tTestLogRecord.hLength);

			}
		}
		assert(0 == ret 
				|| DB_FORMAT_ERROR_DEPRECATE_BLK == ret);
		pBuffer = pNextBlock;
		pNextBlock = pNextBlock == pEnd ? pEnd : pNextBlock + iBlockSize;
	}
}

int ReloadDirectIOBuffer(
		DirectIOReadInfo* ptInfo, int iExpectedReadSize)
{
	assert(NULL != ptInfo);
	if (INVALID_FD == ptInfo->iDirectFD)
	{
		return DB_READ_ERROR_INVALID_FD;
	}

	assert(INVALID_FD != ptInfo->iDirectFD);
	assert(0 < ptInfo->iBlockSize);
	assert(NULL != ptInfo->pDirectIOBuffer);

	const int iBlockSize = ptInfo->iBlockSize;
	iExpectedReadSize = min(iExpectedReadSize, ptInfo->iMaxDirectIOBufSize);

	const int iPrevReadSize = ptInfo->iDirectIOBufSize;
	iExpectedReadSize = min(iExpectedReadSize, iPrevReadSize * 2);
	// limit max read in case deprecate blk:
	iExpectedReadSize = max(iExpectedReadSize, iBlockSize);

	assert(0 == ptInfo->iDirectIOOffset % iBlockSize);
	assert(iBlockSize <= iExpectedReadSize);
	ptInfo->iDirectIOBufSize = 0;
	int iReadSize = SafeDirectIOPRead(
			ptInfo->iDirectFD, ptInfo->pDirectIOBuffer, 
			iExpectedReadSize, ptInfo->iDirectIOOffset);
	// direct io read: at least one block
	if (iReadSize < iBlockSize)
	{
		if (0 == iReadSize)
		{
			return 0; // EOF
		}

		logerr("SafeDirectIOPRead iDirectIOOffset %u ret %d", 
				ptInfo->iDirectIOOffset, iReadSize);
		return 0 > iReadSize ? DB_READ_ERROR_BASIC : DB_READ_ERROR_POOR_READ;
	}

	// align
	assert(iReadSize <= iExpectedReadSize);
	ptInfo->iDirectIOBufSize = (iReadSize / iBlockSize) * iBlockSize;
	assert(ptInfo->iDirectIOBufSize <= iReadSize);
	assert(0 == ptInfo->iDirectIOBufSize % iBlockSize);
	
	// add for test
//	logdebug("READ STAT iDirectIOOffset %u iDirectIOBufSize %u iReadSize %u", 
//			ptInfo->iDirectIOOffset, ptInfo->iDirectIOBufSize, iReadSize);
	return ptInfo->iDirectIOBufSize;
}

void SkipOneBlock(DirectIOReadInfo* ptInfo)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);
	assert(ptInfo->iOffset >= ptInfo->iDirectIOOffset);
	uint32_t iMaxDirectIOOffset = ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize;
	assert(ptInfo->iOffset <= iMaxDirectIOOffset);
	uint32_t iBlkOffset = ptInfo->iOffset % ptInfo->iBlockSize;
	uint32_t iNewOffset = ptInfo->iOffset + ptInfo->iBlockSize;
	iNewOffset = 0 == iBlkOffset ? iNewOffset : iNewOffset - iBlkOffset;
	assert(iNewOffset > ptInfo->iOffset);
	ptInfo->iOffset = iNewOffset;
	// adjust
	if (ptInfo->iOffset >= iMaxDirectIOOffset)
	{
		// adjust iDirectIOOffset & iDirectIOBufSize
		ptInfo->iDirectIOOffset = iNewOffset;
		ptInfo->iDirectIOBufSize = 0;
	}

	assert(ptInfo->iOffset >= ptInfo->iDirectIOOffset);
	assert(ptInfo->iOffset <= ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize);
}

void SkipErrorBlock(DirectIOReadInfo* ptInfo)
{
	int iOldReadStatus = ptInfo->iReadStatus;
	if (false == IsLevelDBFormatError(iOldReadStatus))
	{
		return ; // can't skip not LevelDBLogFormatError
	}

	ptInfo->iReadStatus = 0;
	if (ptInfo->iDirectIOOffset > ptInfo->iOffset)
	{
		ptInfo->iOffset = ptInfo->iDirectIOOffset;
	}
	SkipOneBlock(ptInfo);
}

void EnableCheckBlockMode(DirectIOReadInfo* ptInfo)
{
	ptInfo->iMode |= READINFO_MODE_CHECK_BLOCK;
}

bool IsValidBlock(const char* pBlkIter, const char* pBlkEnd, int iBlockSize)
{
	assert(NULL != pBlkIter);
	assert(pBlkIter <= pBlkEnd);
	assert(pBlkIter + iBlockSize >= pBlkEnd);
	if (pBlkIter + iBlockSize != pBlkEnd)
	{
		return true; // default
	}

	while (pBlkIter < pBlkEnd)
	{
		RLogLevelDBRecord tLogRecord = {0};	
		const char* pNextBlkIter = 
			PickleLevelDBRecord(pBlkIter, pBlkEnd, tLogRecord, true);
		if (kBrokenType == tLogRecord.cRecordType)
		{
			return false;
		}
		assert(pNextBlkIter > pBlkIter);
		pBlkIter = pNextBlkIter;
	}

	return true;
}

int FixDirectIOReadInfo(DirectIOReadInfo* ptInfo)
{
	if (CheckMode(ptInfo->iMode, READINFO_MODE_DELEGATE_READONLY))
	{
		return 0; // fix nothing: readonly
	}

	// DB_FORMAT_ERROR_EOF_BLK isn't a error!!
	// adjust iDirectIOOffset to match ptInfo->iOffset begin blk;
	ptInfo->iDirectIOOffset = 
		LowerAlignment(ptInfo->iOffset, ptInfo->iBlockSize);
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	ptInfo->iDirectIOBufSize = 0;
	// reload anyway
	int ret = ReloadDirectIOBuffer(ptInfo, ptInfo->iBlockSize);
	if (0 >= ret)
	{
		assert(0 == ptInfo->iDirectIOBufSize);
		return ret;
	}

	assert(ret == ptInfo->iBlockSize);
	assert(ptInfo->iDirectIOBufSize == ptInfo->iBlockSize);
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	assert(ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize
			>= ptInfo->iOffset);
	if (CheckMode(ptInfo->iMode, READINFO_MODE_CHECK_BLOCK)
			&& !IsValidBlock(
				ptInfo->pDirectIOBuffer, 
				ptInfo->pDirectIOBuffer + ptInfo->iBlockSize, 
				ptInfo->iBlockSize))
	{
		return DB_FORMAT_ERROR_BROKEN_BLK;
	}

	return ret;
}

inline void AssertCheckValid(DirectIOReadInfo* ptInfo)
{
	assert(NULL != ptInfo);
	assert(NULL != ptInfo->pDirectIOBuffer);
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	assert(ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize >= ptInfo->iOffset);
}

inline const char* GetCurrentPos(const DirectIOReadInfo* ptInfo)
{
	assert(ptInfo->iDirectIOOffset <= ptInfo->iOffset);
	assert(ptInfo->iOffset <= ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize);
	return ptInfo->pDirectIOBuffer
		+ (ptInfo->iOffset - ptInfo->iDirectIOOffset);
}

inline const char* GetDirectBufferEnd(const DirectIOReadInfo* ptInfo)
{
	assert(0 <= ptInfo->iDirectIOBufSize);
	return ptInfo->pDirectIOBuffer + ptInfo->iDirectIOBufSize;
}

inline int CalculateReadSize(
		const DirectIOReadInfo* ptInfo, const char* pNewBuffer)
{
	if (ptInfo->iDirectIOOffset <= ptInfo->iOffset)
	{
		const char* pBuffer = GetCurrentPos(ptInfo);
		assert(pBuffer <= pNewBuffer);
		return pNewBuffer - pBuffer;
	}

	// else 
	assert(pNewBuffer >= ptInfo->pDirectIOBuffer);
	assert(pNewBuffer <= ptInfo->pDirectIOBuffer + ptInfo->iDirectIOBufSize);
	return (ptInfo->iDirectIOOffset - ptInfo->iOffset)
			+ (pNewBuffer - ptInfo->pDirectIOBuffer);
}

inline void IncreaseOffset(
		DirectIOReadInfo* ptInfo, const int iReadSize)
{
	assert(NULL != ptInfo);
	assert(0 <= iReadSize);
	assert(0 < ptInfo->iBlockSize);
	uint32_t iNewOffset = ptInfo->iOffset + iReadSize;
	assert(iNewOffset >= ptInfo->iDirectIOOffset);
	assert(iNewOffset <= ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize);
	ptInfo->iOffset = iNewOffset;
	return ;
}

inline bool IsOffsetInDirectIOBuffer(
		const DirectIOReadInfo* ptInfo, const uint32_t iOffset)
{
	assert(NULL != ptInfo);
	return ptInfo->iDirectIOOffset <= iOffset
		&& ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize >= iOffset;
}

inline const char* GetNextBlockPos(
		const DirectIOReadInfo* ptInfo, const char* pBuffer)
{
	assert(ptInfo->pDirectIOBuffer + ptInfo->iDirectIOBufSize >= pBuffer);

	return GetNextBlockPosImpl(ptInfo, pBuffer);
}

inline int AdjustAndReload(DirectIOReadInfo* ptInfo)
{
	ptInfo->iDirectIOOffset = LowerAlignment(ptInfo->iOffset, ptInfo->iBlockSize);
	ptInfo->iDirectIOBufSize = 0;
	return ReloadDirectIOBuffer(ptInfo, ptInfo->iBlockSize);
}


int DirectIOReadImpl(DirectIOReadInfo* ptInfo, std::string& sValue)
{
	assert(true == IsValid(ptInfo));
	if (MAX_LOG_FILE_SIZE <= ptInfo->iOffset)
	{
		return 0; // EOF
	}

	const int iBlockSize = ptInfo->iBlockSize;
	const char* pBuffer = GetCurrentPos(ptInfo);
	const char* pNextBlock = GetNextBlockPos(ptInfo, pBuffer);
	const char* pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	assert(pBuffer <= pNextBlock);
	assert(pNextBlock <= pDirectBufferEnd);

	// keep ptInfo->iOffset unchanged !!
	const char* pNewBuffer = NULL;
	int ret = 0;
	sValue.clear();

	bool bTailingFakeRecord = false;
	bool bCheckBlock = CheckMode(ptInfo->iMode, READINFO_MODE_CHECK_BLOCK);
	while (true)
	{
		if (pBuffer == pNextBlock)
		{
			assert(pDirectBufferEnd == pBuffer);
			if (CheckMode(ptInfo->iMode, READINFO_MODE_DELEGATE_READONLY))
			{
				return true == sValue.empty() ? 0 : DB_READ_ERROR_UNEXPECTED_EOF;
			}
			// reach the end of pDirectIOBuffer
			assert(ptInfo->iMaxDirectIOBufSize >= 2 * ptInfo->iBlockSize);
			const uint32_t iOldDirectIOOffset = ptInfo->iDirectIOOffset;
			assert(false == bTailingFakeRecord
					|| ptInfo->iBlockSize <= ptInfo->iDirectIOBufSize);
			ptInfo->iDirectIOOffset += ptInfo->iDirectIOBufSize;
			// backoff one blk if bTailingFakeRecord == true
			// => reduce the performace of read for efficent write;
			ptInfo->iDirectIOOffset = bTailingFakeRecord ?
				ptInfo->iDirectIOOffset - ptInfo->iBlockSize : ptInfo->iDirectIOOffset;
			assert(iOldDirectIOOffset <= ptInfo->iDirectIOOffset);

			ret = ReloadDirectIOBuffer(ptInfo, ptInfo->iMaxDirectIOBufSize);
			if (0 >= ret)
			{
				if (0 != ret)
				{
					return ret; // ERROR CASE
				}

				assert(0 == ret);
				return true == sValue.empty() ? 0 : DB_READ_ERROR_UNEXPECTED_EOF;
			}

			assert(iBlockSize <= ret);
			assert(ret == ptInfo->iDirectIOBufSize);
			// if true == bTailingFakeRecord
			// => ptInfo->iOffset must in ptInfo->pDirectIOBuffer
			pBuffer = false == bTailingFakeRecord 
				? ptInfo->pDirectIOBuffer : GetCurrentPos(ptInfo);
			pNextBlock = GetNextBlockPos(ptInfo, pBuffer);
			pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
			assert(pNextBlock <= pDirectBufferEnd);

			// peak the RLogLevelDBRecord: 
			if (bTailingFakeRecord && ptInfo->iBlockSize == ptInfo->iDirectIOBufSize)
			{
				// reload but buffer don't extend:
				// => recheck this buffer
				assert(pNextBlock == pDirectBufferEnd);
				assert(pBuffer != pNextBlock - iBlockSize);
				RLogLevelDBRecord tLogRecord = {0};
				const char* pTmpNewBuffer = 
					PickleLevelDBRecord(pBuffer, pNextBlock, tLogRecord, false);
				if (pTmpNewBuffer == pNextBlock 
						&& kFakeRecordType == tLogRecord.cRecordType)
				{
					return 0; // EOF on this case
				}
			}
		}

		assert(pBuffer < pNextBlock);
		assert(pNextBlock - pBuffer <= iBlockSize);
		// check only if (pBuffer + iBlockSize == pNextBlock)
		if (bCheckBlock && !IsValidBlock(pBuffer, pNextBlock, iBlockSize))
		{
			{
				const char* pTestBlkBegin = pNextBlock - iBlockSize;
				const char* pTestBlkEnd = pNextBlock;
				const char* pTestIter = pTestBlkBegin;
				uint32_t iTestBaseOffset = 
					pTestBlkBegin - ptInfo->pDirectIOBuffer + ptInfo->iDirectIOOffset;
				while (pTestIter < pTestBlkEnd)
				{
					RLogLevelDBRecord tLogRecord = {0};	
					const char* pTestNextIter = PickleLevelDBRecord(
							pTestIter, pTestBlkEnd, tLogRecord, true);

					logerr("INVALID pBlkEnd %p iTestOffset %u tLogRecord: type %u length %u tBlkMeta %d", 
							pNextBlock, 
							static_cast<uint32_t>(pTestIter - pTestBlkBegin) + iTestBaseOffset, 
							tLogRecord.cRecordType, tLogRecord.hLength, 
							pTestIter == pTestBlkBegin);
					pTestIter = pTestNextIter;
				}
			}

			return DB_FORMAT_ERROR_BROKEN_BLK;
		}

		ret = PickleLevelDBLogBlock(
				ptInfo->llStartBlkSeq, iBlockSize, 
				pBuffer, pNextBlock, sValue, pNewBuffer, bTailingFakeRecord);
		if (0 >= ret)
		{
			if (0 > ret)
			{
				return ret; // ERROR CASE
			}

			break; // success read
		}

		assert(1 == ret);
		assert(pNewBuffer == pNextBlock);
//		assert(false == bTailingFakeRecord
//				|| pBuffer == GetCurrentPos(ptInfo));
		pBuffer = pNextBlock;
		pNextBlock = 
			(pNextBlock == pDirectBufferEnd) 
			? pDirectBufferEnd : pNextBlock + iBlockSize;
	}

	assert(0 == ret);
	assert(NULL != pNewBuffer);
	assert(pBuffer < pNewBuffer);
	assert(pNewBuffer <= pNextBlock);
	return CalculateReadSize(ptInfo, pNewBuffer);
}


int DirectIOReadImpl(
		DirectIOReadInfo* ptInfo, 
		RLogLevelDBRecord& tLogRecord, std::string& sBuffer)
{
	assert(true == IsValid(ptInfo));
	if (MAX_LOG_FILE_SIZE <= ptInfo->iOffset)
	{
		return 0; // EOF
	}

	const int iBlockSize = ptInfo->iBlockSize;
	assert(static_cast<uint32_t>(iBlockSize) <= ptInfo->iOffset);
	const char* pBuffer = GetCurrentPos(ptInfo);
	const char* pNextBlock = GetNextBlockPos(ptInfo, pBuffer);
	const char* pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	assert(pBuffer <= pNextBlock);
	assert(pNextBlock <= pDirectBufferEnd);

	int ret = 0;
	if (pBuffer == pNextBlock)
	{
		assert(pDirectBufferEnd == pBuffer);
		if (CheckMode(ptInfo->iMode, READINFO_MODE_DELEGATE_READONLY))
		{
			return 0;
		}

		ptInfo->iDirectIOOffset += ptInfo->iDirectIOBufSize;
		ret = ReloadDirectIOBuffer(ptInfo, ptInfo->iMaxDirectIOBufSize);
		if (0 >= ret)
		{
			return ret;
		}

		assert(ret == ptInfo->iDirectIOBufSize);
		assert(iBlockSize <= ret);
		pBuffer = ptInfo->pDirectIOBuffer;
		pNextBlock = pBuffer + iBlockSize;
		pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
		assert(pNextBlock <= pDirectBufferEnd);
	}

	assert(pBuffer < pNextBlock);
	memset(&tLogRecord, 0, sizeof(tLogRecord));
	const char* pNewBuffer = PickleLevelDBRecord(pBuffer, pNextBlock, tLogRecord, true);
	sBuffer.clear();
	if (kFakeRecordType != tLogRecord.cRecordType
			&& kZeroType != tLogRecord.cRecordType
			&& kBrokenType != tLogRecord.cRecordType)
	{
		sBuffer.append(tLogRecord.pValue, pNewBuffer);
		assert(static_cast<size_t>(tLogRecord.hLength) == sBuffer.size());
		tLogRecord.pValue = sBuffer.data();
		assert(NULL != tLogRecord.pValue);
	}
	
	assert(0 < pNewBuffer - pBuffer);
	return pNewBuffer - pBuffer;
}

int DirectIORead(
		DirectIOReadInfo* ptInfo, std::string& sValue, uint32_t& iOffset)
{
	TRY_FIX_READINFO_IF_ERROR(ptInfo);

	assert(true == IsValid(ptInfo));
	AssertCheckValid(ptInfo);
	const uint32_t iOldOffset = ptInfo->iOffset;
	assert(static_cast<uint32_t>(ptInfo->iBlockSize) <= iOldOffset);
	int iReadSize = DirectIOReadImpl(ptInfo, sValue);
	assert(iOldOffset == ptInfo->iOffset);
	UpdateReadStatus(ptInfo, iReadSize);
	if (0 >= iReadSize)
	{
		if (0 == iReadSize 
				|| DB_FORMAT_ERROR_DEPRECATE_BLK == iReadSize)
		{
			// => case second read reload given block
			return 1; // EOF
		}

		// ERROR CASE: iOffset unchange if error occur;
		assert(false == IsValid(ptInfo));
		logdebug("iOffset %u iDirectIOOffset %u iDirectIOBufSize %u ret %d", 
				ptInfo->iOffset, ptInfo->iDirectIOOffset, 
				ptInfo->iDirectIOBufSize, iReadSize);
		return iReadSize;
	}

	iOffset = ptInfo->iOffset;
	assert(true == IsValid(ptInfo));
	assert(static_cast<size_t>(iReadSize) > sValue.size());
	IncreaseOffset(ptInfo, iReadSize);
	return 0; // a success read
}

bool CheckCurrentBlock(DirectIOReadInfo* ptInfo)
{
	if (static_cast<uint32_t>(ptInfo->iBlockSize) > ptInfo->iOffset)
	{
		return true; // not need check first block;
	}

	if (0 == ptInfo->iDirectIOBufSize
			|| false == IsOffsetInDirectIOBuffer(ptInfo, ptInfo->iOffset))
	{
		return true; // no need check
	}

	const char* pBuffer = GetCurrentPos(ptInfo);
	const char* pNextBlock = GetNextBlockPos(ptInfo, pBuffer);
	hassert(ptInfo->pDirectIOBuffer + ptInfo->iBlockSize <= pNextBlock, 
			"pDirectIOBuffer %p iOffset %u iDirectIOBufSize %d pNextBlock %p", 
			ptInfo->pDirectIOBuffer, ptInfo->iOffset, ptInfo->iDirectIOBufSize, 
			pNextBlock);
	return IsValidBlock(
			pNextBlock - ptInfo->iBlockSize, pNextBlock, ptInfo->iBlockSize);
}

int DirectIORead(
		DirectIOReadInfo* ptInfo, 
		uint32_t iOffset, std::string& sValue, uint32_t& iNextOffset)
{
	assert(NULL != ptInfo);
	iOffset = 0 == iOffset ?  ptInfo->iBlockSize : iOffset;
	assert(static_cast<uint32_t>(ptInfo->iBlockSize) <= iOffset);

	int ret = 0;
	bool bCheckBlock = CheckMode(ptInfo->iMode, READINFO_MODE_CHECK_BLOCK);
	uint32_t iOldOffset = ptInfo->iOffset;
	ptInfo->iOffset = iOffset;
	if (false == IsOffsetInDirectIOBuffer(ptInfo, iOffset))
	{
		ret = AdjustAndReload(ptInfo);
		UpdateReadStatus(ptInfo, ret);
		if (0 >= ret)
		{
			return 0 == ret ? 1 : ret;
		}

		assert(ret == ptInfo->iBlockSize);
		assert(true == IsValid(ptInfo));
	}

	bCheckBlock = bCheckBlock && 
		(0 == ptInfo->iOffset % ptInfo->iBlockSize) &&
		(LowerAlignment(ptInfo->iOffset, ptInfo->iBlockSize) > 
		 LowerAlignment(iOldOffset, ptInfo->iBlockSize));

	// IMPORTANT!!!  // may return
	// fix only if ptInfo->iReadStatus != 0
	if (false == IsValid(ptInfo))
	{
		TRY_FIX_READINFO_IF_ERROR(ptInfo);
	} 
	else if (bCheckBlock)
	{
		if (!CheckCurrentBlock(ptInfo))
		{
			return DB_FORMAT_ERROR_BROKEN_BLK;
		}
	}

	assert(true == IsValid(ptInfo));
	AssertCheckValid(ptInfo);
	int iReadSize = DirectIOReadImpl(ptInfo, sValue);
	assert(iOffset == ptInfo->iOffset);
	UpdateReadStatus(ptInfo, iReadSize);
	if (0 >= iReadSize)
	{
		if (0 == iReadSize 
				|| DB_FORMAT_ERROR_DEPRECATE_BLK == iReadSize)
		{
			// mark iReadStatus as error
			// => cause second read reload given block
			return 1; // EOF
		}

		assert(false == IsValid(ptInfo));
		logdebug("iOffset %u iDirectIOOffset %u iDirectIOBufSize %u ret %d", 
				ptInfo->iOffset, ptInfo->iDirectIOOffset, 
				ptInfo->iDirectIOBufSize, iReadSize);
		return iReadSize;
	}

	assert(true == IsValid(ptInfo));
	assert(static_cast<size_t>(iReadSize) > sValue.size());
	IncreaseOffset(ptInfo, iReadSize);
	iNextOffset = ptInfo->iOffset;
	return 0;
}


int DirectIORead(
		DirectIOReadInfo* ptInfo, 
		RLogLevelDBRecord& tLogRecord, std::string& sBuffer)
{
	assert(NULL != ptInfo);
	TRY_FIX_READINFO_IF_ERROR(ptInfo);

	assert(true == IsValid(ptInfo));
	AssertCheckValid(ptInfo);
	assert(static_cast<uint32_t>(ptInfo->iBlockSize) <= ptInfo->iOffset);
	int iReadSize = DirectIOReadImpl(ptInfo, tLogRecord, sBuffer);
	UpdateReadStatus(ptInfo, iReadSize);
	if (0 >= iReadSize)
	{
		return 0 == iReadSize ? 1 : iReadSize;
	}

	assert(true == IsValid(ptInfo));
	IncreaseOffset(ptInfo, iReadSize);
	return 0;
}

void PrintDirectIOBuffer(
		DirectIOReadInfo* ptInfo)
{
	assert(NULL != ptInfo);

	logerr("TEST ptInfo->iOffset %u ptInfo->iDirectIOOffset %u "
			"ptInfo->iDirectIOBufSize %u", 
			ptInfo->iOffset, ptInfo->iDirectIOOffset, ptInfo->iDirectIOBufSize);
	if (ptInfo->iOffset < ptInfo->iDirectIOOffset 
			|| ptInfo->iOffset > (ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize))
	{
		return ;
	}

	uint32_t iOldOffset = ptInfo->iOffset;
	int iOldReadStatus = ptInfo->iReadStatus;

	ptInfo->iOffset = ptInfo->iDirectIOOffset;
	ptInfo->iReadStatus = 0;
	while (ptInfo->iOffset < ptInfo->iDirectIOOffset + ptInfo->iDirectIOBufSize)
	{
		RLogLevelDBRecord tLogRecord = {0};
		string sBuffer;
		int iReadSize = DirectIOReadImpl(ptInfo, tLogRecord, sBuffer);
		if (0 >= iReadSize)
		{
			logerr("TEST ptInfo->iOffset %u DirectIOReadImpl ret %d", 
					ptInfo->iOffset, iReadSize);
			break;
		}

		logerr("TEST ptInfo->iOffset %u tLogRecord: type %u length %u tBlkMeta %d", 
				ptInfo->iOffset, tLogRecord.cRecordType, tLogRecord.hLength, 
				0 == (ptInfo->iOffset % ptInfo->iBlockSize));
		if (kBlockMetaType == tLogRecord.cRecordType)
		{
			LogBlockMetaInfo tBlkMetaInfo = {0};
			PickleBlockMetaInfo(tLogRecord.pValue, tLogRecord.hLength, tBlkMetaInfo);
			logerr("TEST ptInfo->iOffset %u llStartBlkSeq %lu blkseq %lu", 
					ptInfo->iOffset, ptInfo->llStartBlkSeq, tBlkMetaInfo.llBlkSeq);
		}
		IncreaseOffset(ptInfo, iReadSize);
	}

	ptInfo->iOffset = iOldOffset;
	ptInfo->iReadStatus = iOldReadStatus;
	return ;
}


int DirectIOReadSkipError(
		DirectIOReadInfo* ptInfo, 
		std::string& sValue, uint32_t& iOffset)
{
	int ret = 0;
	do 
	{
		uint32_t iOldOffset = ptInfo->iOffset;
		ret = DirectIORead(ptInfo, sValue, iOffset);
		if (0 <= ret
				|| false == IsLevelDBFormatError(ret))
		{
			// success read or EOF or ERROR CASE
			return ret;
		}

		// leveldb format error: skip into next 
		// skip by read one tLogRecord
		RLogLevelDBRecord tLogRecord = {0};
		string sDropValue;
		int iRet = DirectIORead(ptInfo, tLogRecord, sDropValue);
		if (0 != iRet)
		{
			logerr("DirectIORead ret %d iRet %d", ret, iRet);
			return iRet;
		}

		assert(iOldOffset != ptInfo->iOffset);
		logerr("ret %d skip iOffset %u skip %u recordtype %u", 
				ret, iOldOffset, ptInfo->iOffset - iOldOffset, 
				tLogRecord.cRecordType);
		assert(ptInfo->iOffset <= MAX_LOG_FILE_SIZE);
	} while (true);

	// never reach here
	assert(0);
	return -10;
}


int DirectIOReadSkipError(
		DirectIOReadInfo* ptInfo, 
		uint32_t iOffset, std::string& sValue, uint32_t& iNextOffset)
{
	int ret = 0;
	do 
	{
		ret = DirectIORead(ptInfo, iOffset, sValue, iNextOffset);
		if (0 <= ret
				|| false == IsLevelDBFormatError(ret))
		{
			return ret;
		}
	
		// leveldb format error: skip into next 
		// skip by read one tLogRecord
		RLogLevelDBRecord tLogRecord = {0};
		string sDropValue;
		int iRet = DirectIORead(ptInfo, tLogRecord, sDropValue);
		if (0 != iRet)
		{
			logerr("DirectIORead ret %d iRet %d", ret, iRet);
			return iRet;
		}

		assert(iOffset != ptInfo->iOffset);
		logerr("ret %d skip iOffset %u skip %u recordtype %u", 
				ret, iOffset, ptInfo->iOffset - iOffset, 
				tLogRecord.cRecordType);
		assert(ptInfo->iOffset <= MAX_LOG_FILE_SIZE);

		iOffset = ptInfo->iOffset; // reset iOffset: skip error
		assert(iOffset <= MAX_LOG_FILE_SIZE);
	} while (true);

	// never reach here!
	assert(0);
	return -10;
}


int CloseFile(DirectIOReadInfo* ptInfo)
{
	assert(NULL != ptInfo);

	int iDirectFD = INVALID_FD;
	swap(iDirectFD, ptInfo->iDirectFD);
	ptInfo->iOffset = 0;
	ptInfo->iDirectIOOffset = 0;
	ptInfo->iDirectIOBufSize = 0;
	ptInfo->iReadStatus = 0;

	int ret = 0;
	if (INVALID_FD != iDirectFD)
	{
		ret = close(iDirectFD);
		iDirectFD = INVALID_FD;
	}

	return ret;
}


clsLevelDBLogRawReader::clsLevelDBLogRawReader(const int iMaxDirectIOBufSize)
	: m_iMaxDirectIOBufSize(iMaxDirectIOBufSize)
	, m_pDirectIOBuffer(NULL)
	, m_ptRInfo(NULL)
{
	assert(0 <= iMaxDirectIOBufSize);
}

clsLevelDBLogRawReader::~clsLevelDBLogRawReader()
{
	if (NULL != m_ptRInfo)
	{
		assert(CheckMode(m_ptRInfo->iMode, READINFO_MODE_DELEGATE));
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	if (NULL != m_pDirectIOBuffer)
	{
		free(m_pDirectIOBuffer);
		m_pDirectIOBuffer = NULL;
	}
}

int clsLevelDBLogRawReader::OpenFile(const char* sLevelDBLogFile)
{
	if (NULL != m_ptRInfo)
	{
		assert(CheckMode(m_ptRInfo->iMode, READINFO_MODE_DELEGATE));
		delete m_ptRInfo;
		m_ptRInfo = NULL;
	}

	if (NULL == m_pDirectIOBuffer)
	{
		m_pDirectIOBuffer = AllocDirectIOBuffer(m_iMaxDirectIOBufSize);
	}

	assert(NULL == m_ptRInfo);
	assert(NULL != m_pDirectIOBuffer);
	DirectIOReadInfo* ptRInfo = NULL;
	int ret = CreateADirectIOReadInfo(
			sLevelDBLogFile, m_pDirectIOBuffer, m_iMaxDirectIOBufSize, ptRInfo);
	if (0 != ret)
	{
		assert(NULL == ptRInfo);
		return ret;
	}

	swap(m_ptRInfo, ptRInfo);
	assert(NULL != m_ptRInfo);
	assert(NULL == ptRInfo);
	return 0;
}

int clsLevelDBLogRawReader::Read(
		RLogLevelDBRecord& tLogRecord, std::string& sBuffer)
{
	if (NULL == m_ptRInfo)
	{
		return -1;
	}

	assert(NULL != m_ptRInfo);

	return DirectIORead(m_ptRInfo, tLogRecord, sBuffer);
}

uint32_t clsLevelDBLogRawReader::GetCurrentOffset() const
{
	assert(NULL != m_ptRInfo);
	return m_ptRInfo->iOffset;
}


int ScanBlockMetaInfo(
		DirectIOReadInfo* ptInfo, uint64_t& llBlkSeq, uint32_t& iOffset)
{
	assert(NULL != ptInfo);
	assert(0 < ptInfo->iBlockSize);
	
	iOffset = 0;
	assert(static_cast<uint32_t>(ptInfo->iBlockSize) == ptInfo->iOffset);
	assert(ptInfo->iOffset == ptInfo->iDirectIOOffset);
	assert(ptInfo->iBlockSize <= ptInfo->iMaxDirectIOBufSize);

	const char* pBuffer = GetCurrentPos(ptInfo);
	const char* pNextBlock = GetNextBlockPos(ptInfo, pBuffer);
	const char* pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
	assert(pNextBlock <= pDirectBufferEnd);
	while (true)
	{
		if (pBuffer == pNextBlock)
		{
			ptInfo->iDirectIOOffset += ptInfo->iDirectIOBufSize;
			const int iReadSize = 
				ReloadDirectIOBuffer(ptInfo, ptInfo->iMaxDirectIOBufSize);
			if (0 >= iReadSize)
			{
				if (0 == iReadSize)
				{
					break; // EOF
				}

				return iReadSize;
			}

			pBuffer = ptInfo->pDirectIOBuffer;
			pNextBlock = pBuffer + ptInfo->iBlockSize;
			pDirectBufferEnd = GetDirectBufferEnd(ptInfo);
			assert(pNextBlock <= pDirectBufferEnd);
		}
		// iReadSize == ptInfo->iBlockSize
		RLogLevelDBRecord tLogRecord = {0};
		const char* pNewBuffer = 
			PickleLevelDBRecord(pBuffer, pNextBlock, tLogRecord, true);	
		hassert(pNewBuffer > pBuffer && pNewBuffer < pNextBlock, 
				"iOffset %u iDirectIOOffset %u pDirectIOBuffer %p pNewBuffer %p pNextBlock %p pBuffer %p", 
				ptInfo->iOffset, ptInfo->iDirectIOOffset, ptInfo->pDirectIOBuffer, 
				pNewBuffer, pNextBlock, pBuffer);
		if (kBlockMetaType == tLogRecord.cRecordType)
		{
			// kBlockMetaType == tLogRecord.cRecordType
			LogBlockMetaInfo tBlkMetaInfo = {0};
			PickleBlockMetaInfo(tLogRecord.pValue, tLogRecord.hLength, tBlkMetaInfo);
			if (IsDeprecateBlock(ptInfo->llStartBlkSeq, tBlkMetaInfo.llBlkSeq))
			{
				// YES: DB_FORMAT_ERROR_DEPRECATE_BLK;
				break;
			}

			// else: valid blk => update llBlkSeq
			llBlkSeq = tBlkMetaInfo.llBlkSeq;
		}
		else
		{
			// ignore the error block
			logerr("iOffset %u UNEXPECTED tLogRecord.cRecordType %u", 
					ptInfo->iOffset, tLogRecord.cRecordType);
		}

		// else move the next block;
		ptInfo->iOffset += ptInfo->iBlockSize;
		pBuffer = pNextBlock;
		pNextBlock = pNextBlock == pDirectBufferEnd 
			? pDirectBufferEnd : pNextBlock + ptInfo->iBlockSize;
	}

	assert(ptInfo->iOffset >= ptInfo->iDirectIOOffset);
	iOffset = ptInfo->iOffset;
	return 0;
}

int ReadMetaInfo(
		const char* sFileName, const int iDirectIOBufSize, 
		int& iBlockSize, uint64_t& llBlkSeq, uint32_t& iOffset)
{
	assert(NULL != sFileName);
	assert(0 <= iDirectIOBufSize);

	DirectIOReadInfo* ptRInfo = NULL;
	int ret = CreateADirectIOReadInfo(sFileName, iDirectIOBufSize, ptRInfo);
	if (0 != ret)
	{
		assert(NULL == ptRInfo);
		// double check
		if (DB_READ_ERROR_POOR_READ == ret)
		{
			// check file size
			int iFileSize = dbcomm::GetFileSize(sFileName);
			assert(0 < iFileSize);
			int iPageSize = getpagesize();
			logerr("%s iFileSize %d iPageSize %d", 
					sFileName, iFileSize, iPageSize);
			// new format use direct io
			// => never write a file less then pagesize unless filesize == 0
			if (iFileSize < getpagesize())
			{
				return 1; // trut as old format
			}
		}
		return ret;
	}

	assert(NULL != ptRInfo);
	assert(static_cast<uint32_t>(ptRInfo->iBlockSize) == ptRInfo->iOffset);
	// no need to skip anymore
	CREATE_HEAP_MEM_MANAGER(DirectIOReadInfo, ptRInfo);
	
	iBlockSize = ptRInfo->iBlockSize;
	llBlkSeq = ptRInfo->llStartBlkSeq;
	return ScanBlockMetaInfo(ptRInfo, llBlkSeq, iOffset);
}

int ReadFileMetaInfo(
		const char* sFileName, int& iBlockSize, uint64_t& llStartBlkSeq)
{
	assert(NULL != sFileName);

	DirectIOReadInfo* ptRInfo = NULL;
	int ret = CreateADirectIOReadInfo(sFileName, 0, ptRInfo);
	if (0 != ret)
	{
		assert(NULL == ptRInfo);
		if (DB_READ_ERROR_POOR_READ == ret)
		{
			// check file size
			int iFileSize = dbcomm::GetFileSize(sFileName);
			assert(0 < iFileSize);
			int iPageSize = getpagesize();
			logerr("%s iFileSize %d iPageSize %d", 
					sFileName, iFileSize, iPageSize);
			if (iFileSize < getpagesize())
			{
				return 1; // treat as old format
			}
		}
		return ret;
	}

	assert(NULL != ptRInfo);
	CREATE_HEAP_MEM_MANAGER(DirectIOReadInfo, ptRInfo);	
	iBlockSize = ptRInfo->iBlockSize;
	llStartBlkSeq = ptRInfo->llStartBlkSeq;
	return 0;
}

int WriteFileMetaInfo(
		const char* sFileName, 
		const int iBlockSize, const uint64_t llStartBlkSeq)
{
	assert(NULL != sFileName);
	assert(0 < iBlockSize);

	DirectIOWriteInfo* ptWInfo = NULL;
	int ret = CreateADirectIOWriteInfo(
			sFileName, iBlockSize, iBlockSize, 0, llStartBlkSeq, 0, 0, ptWInfo);
	if (0 != ret)
	{
		assert(NULL == ptWInfo);
		return ret;
	}

	assert(NULL != ptWInfo);
	CREATE_HEAP_MEM_MANAGER(DirectIOWriteInfo, ptWInfo);
	assert(0 == ptWInfo->iOffset);
	assert(0 == ptWInfo->iDirectIOOffset);
	LogFileMetaInfo tFileMetaInfo = {0};
	tFileMetaInfo.cMode = 0;
	tFileMetaInfo.llStartBlkSeq = llStartBlkSeq;
	tFileMetaInfo.iBlockSize = iBlockSize;
	DumpFileMetaInfo(tFileMetaInfo, 
			ptWInfo->pDirectIOBuffer, ptWInfo->pDirectIOBuffer + iBlockSize);
	ret = SmallWriteImpl(ptWInfo, 
			ptWInfo->pDirectIOBuffer, ptWInfo->pDirectIOBuffer + iBlockSize);
	if (0 > ret)
	{
		return ret;
	}

	assert(ret == iBlockSize);
	return 0;
}

int DeprecateAllDataBlock(const char* sFileName)
{
	assert(NULL != sFileName);
	int iBlockSize = 0;
	uint64_t llStartBlkSeq = 0;
	int ret = ReadFileMetaInfo(sFileName, iBlockSize, llStartBlkSeq);
	if (0 != ret)
	{
		if (0 > ret)
		{
			return ret;
		}

		return 0; // do nothing is prev file isn't a leveldb log format
	}

	// leveldb log format
	if (0 == llStartBlkSeq
			|| 0 >= iBlockSize)
	{
		return -2; // invalid llStartBlkSeq;
	}

	// inc: MAX_BLK_SEQ_INC_PER_FILE => deprecate all blk in sFileName
	uint64_t llNewStartBlkSeq = llStartBlkSeq + MAX_BLK_SEQ_INC_PER_FILE;
	ret = WriteFileMetaInfo(sFileName, iBlockSize, llNewStartBlkSeq);
	logdebug("llStartBlkSeq %lu + %lu llNewStartBlkSeq %lu WriteFileMetaInfo ret %d", 
			llStartBlkSeq, MAX_BLK_SEQ_INC_PER_FILE, llNewStartBlkSeq, ret);
	return ret;
}

} // namespace KVDB



