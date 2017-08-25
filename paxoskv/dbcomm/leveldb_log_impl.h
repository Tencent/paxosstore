
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once 

#include <vector>
#include <string>
#include <cassert>
#include <stdint.h>


enum {
	// [-100, -1000]  level db record format error
	DB_FORMAT_ERROR_UNEXPECTED_TYPE_1 = -100, 
	DB_FORMAT_ERROR_UNEXPECTED_TYPE_2 = -101, 
	DB_FORMAT_ERROR_BROKEN_RECORD = -102, 
	
	DB_FORMAT_ERROR_BROKEN_BLK = -496, 
	DB_FORMAT_ERROR_BLOCKMETA_MISSING = -497, 
	DB_FORMAT_ERROR_UNEXPECTED_BLOCKMETA = -498, 
	DB_FORMAT_ERROR_BROKEN_BC_RECORD = -499, 
	DB_FORMAT_ERROR_BROKEN_FILEMETA = -500, 
	DB_FORMAT_ERROR_UNEXPECTED_EOF_BLK = -998, 
	DB_FORMAT_ERROR_DEPRECATE_BLK = -1000, 
	// (-1000, -2000] direct io read/write error
	// :  (-1000, -1500]  read error
	DB_READ_ERROR_BASIC = -1001, 
	DB_READ_ERROR_UNEXPECTED_EOF = -1002, 
	DB_READ_ERROR_POOR_READ = -1003, 

	DB_READ_ERROR_INVALID_FD = -1500, 
	// :  (-1500, -2000]  write error
	DB_WRITE_ERROR_BASIC = -1501, 
	DB_WRITE_ERROR_UNEXPECTED_ZERO = -1502, 
	DB_WRITE_ERROR_POOR_WRITE = -1503, 

	DB_WRITE_ERROR_LARGE_WRITE = -1898, 
	DB_WRITE_ERROR_SHORT_BUFFER = -1899, 
	DB_WRITE_ERROR_LARGE_RECORD = -1900, 
	DB_WRTIE_ERROR_LARGE_BATCH = -1901, 
	DB_WRITE_ERROR_INVALID_BATCH = -1902, 

	DB_WRITE_ERROR_BUFFER_FULL = -1998, 
	DB_WRITE_ERROR_INVALID_STATUS = -1999, 
	DB_WRITE_ERROR_NEXT_FILE = -2000, 
}; // error code


namespace leveldb {

class Slice;

} // namespace leveldb

namespace dbimpl {

extern const uint64_t MAX_BLK_SEQ_INC_PER_FILE;

#pragma pack(1)
typedef struct tagLogFileMetaInfo
{
	uint8_t cMode;
	uint64_t llStartBlkSeq;
	int32_t iBlockSize;
} LogFileMetaInfo;
#pragma pack()

#pragma pack(1)
typedef struct tagLogBlockMetaInfo
{
	// set cMode == 0xFF to indicate a EOF
	uint8_t cMode;
	uint64_t llBlkSeq;
} LogBlockMetaInfo;
#pragma pack()

// leveldb record format:
// + 4 byte crc
// + 2 byte length
// + 1 byte record type
typedef struct tagRLogLevelDBRecord
{
	// => indicate max block size: 64KB
	uint16_t hLength;
	uint8_t cRecordType;
	const char* pValue;
} RLogLevelDBRecord;


enum RecordType {
	kBrokenType = 0xFF, 
	kZeroType = 0, 
	kFullType = 1, 
	kFirstType = 2, 
	kMiddleType = 3, 
	kLastType = 4, 

	// extend level db log format:
	kBlockMetaType = 10, 
	kFakeRecordType = 11,  // deprecate

	kMaxRecordType, 
};

enum AdjustStrategyType {
	kMaxCompactStrategy = 0, 
	kDontMoveStrategy = 1, 
	kMinWriteStrategy = 2, 
};


extern const int kBlockMetaInfoRecordSize;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
// static const int kHeaderSize = 4 + 2 + 1;


class BlockSeqGenerator
{
public:
	BlockSeqGenerator(uint64_t llBlkSeq)
		: m_llBlkSeq(llBlkSeq)
	{

	}

	void Init(uint64_t llBlkSeq)
	{
		assert(0 != llBlkSeq);
		m_llBlkSeq = llBlkSeq;
	}

	void Adjust(uint64_t llBlkSeq)
	{
		m_llBlkSeq = std::max(m_llBlkSeq, llBlkSeq);
		++m_llBlkSeq;
	}

	uint64_t Next()
	{
		return ++m_llBlkSeq;
	}

	uint64_t GetBlkSeq() const
	{
		return m_llBlkSeq;
	}

private:
	uint64_t m_llBlkSeq;
};

uint32_t CalculateCRC(const RLogLevelDBRecord& tLogRecord);

bool CheckMode(int iMode, int iExpectedMode);

bool IsDeprecateBlock(uint64_t llStartBlkSeq, uint64_t llBlkSeq);

int OpenForDirectIOWrite(const char* sFileName);

int OpenForDirectIORead(const char* sFileName);

char* AllocDirectIOBuffer(const int iDirectIOBufSize);


char* DumpFileMetaInfo(
		const LogFileMetaInfo& tFileMetaInfo, 
		char* pBlkBegin, char* pBlkEnd);

int TryPickleFileMetaInfo(
		const char* pBlkBegin, 
		const char* pBlkEnd, 
		LogFileMetaInfo& tFileMetaInfo);

// NOTICE:
// using directio open read: may cause dirty write in system pagecache flush out
// >  0: true
// == 0: false
// <  0: error
int IsLevelDBLogFormat(const char* sFileName, int iBlockSize);

int IsLevelDBLogFormat(const char* sFileName);

int IsLevelDBLogFormatNormalIO(const char* sFileName);

const char* PickleBlockMetaInfo(
		const char* pBuffer, 
		int iBufLen, 
		LogBlockMetaInfo& tBlkMetaInfo);

char* DumpLevelDBRecord(
		const RecordType t, 
		const char* pRecValue, 
		int iRecValLen, 
		char* pBuffer, 
		char* pBlkEnd);

// when a invalid leveldb record income, PickleLevelDBRecord will
// return pNextBlockInBuffer: whole block contain invalid leveldb record
// will be ignore.
const char* PickleLevelDBRecord(
		const char* pBuffer, 
		const char* pBlkEnd, 
		RLogLevelDBRecord& tLogRecord, 
		bool bCheckCRC);

char* DumpBlockMetaInfoRecord(
		const LogBlockMetaInfo& tBlkMetaInfo, 
		char* pBlkBegin, char* pBlkEnd);


void AppendFakeLevelDBRecord(
		int iBlockSize, int iMinTailRecordSize, char* pBuffer, char* pBlkEnd);


char* DumpLevelDBLogBlock(
		BlockSeqGenerator& oSeqGen, int iBlockSize, 
		char* pBuffer, char* pBlkEnd, 
		const RecordType t, const char* pRecValue, int iRecValLen);


int PickleLevelDBLogBlock(
		int64_t llStartBlkSeq, int iBlockSize, 
		const char* pBuffer, const char* pBlkEnd, 
		std::string& sValue, const char*& pNewBuffer, bool& bTailingFakeRecord);


void AppendFakeLevelDBRecord(
		int iBlockSize, int iMinTailRecordSize, char* pBuffer, char* pBlkEnd);

void AdjustBufferPtr(
		char* const pVeryBegin, const int iBlockSize, 
		char*& pBlkBegin, char*& pBuffer, bool bMoveData);


// new format: leveldb log format
// => not include FileMetaInfo
int CalculateDumpSize(int iBlockSize, int iBlkOffset, int iValLen);


struct DirectIOWriteInfo
{
	const int iBlockSize;
	const int iMaxDirectIOBufSize;
	const int iMinTailRecordSize;

	uint64_t llStartBlkSeq;
	BlockSeqGenerator oSeqGen;
	int iDirectFD;
	uint32_t iOffset;
	uint32_t iDirectIOOffset;
	char* pDirectIOBuffer;

	AdjustStrategyType iAdjustStrategy;
	int iWriteStatus;

	// stat
	// single write
	uint64_t llAccCnt;
	uint64_t llAccValueLen;
	uint64_t llAccDumpSize;
	uint64_t llAccWriteSize;
	uint64_t llAccCostTime;
	// batch write
	uint64_t llAccBatchCnt;
	uint64_t llAccBatchSize;
	uint64_t llAccBatchValueLen;
	uint64_t llAccBatchDumpSize;
	uint64_t llAccBatchWriteSize;
	uint64_t llAccBatchCostTime;

	DirectIOWriteInfo(
			int iBlockSize, int iMaxDirectIOBufSize, 
			int iMinTailRecordSize, uint64_t llStartBlkSeq, 
			AdjustStrategyType iAdjustStrategyI);
	~DirectIOWriteInfo();
};

enum {
	READINFO_MODE_NORMAL = 1,
	READINFO_MODE_DELEGATE = 1 << 1, 
	READINFO_MODE_DELEGATE_READONLY = 1 << 2, 
	READINFO_MODE_CHECK_BLOCK = 1 << 3, 
};

struct DirectIOReadInfo
{
	int iMode;
	const int iBlockSize;
	const uint64_t llStartBlkSeq;
	const int iMaxDirectIOBufSize;

	int iDirectFD;
	uint32_t iOffset;
	int iDirectIOBufSize;
	uint32_t iDirectIOOffset;
	char* pDirectIOBuffer;

	int iReadStatus;

	DirectIOReadInfo(
			int iBlockSize, uint64_t llStartBlkSeq, 
			int iMaxDirectIOBufSize);

	DirectIOReadInfo(
		int iBlockSizeI, uint64_t llStartBlkSeqI, 
		char* pDirectIOBufferI, int iMaxDirectIOBufSizeI);

	DirectIOReadInfo(
			int iBlockSizeI, uint64_t llStartBlkSeqI, 
			const char* pBegin, const char* pEnd);

	~DirectIOReadInfo();
};

std::string GetStatInfo(const DirectIOWriteInfo* ptInfo);

int OpenFile(
		DirectIOWriteInfo* ptInfo, 
		const char* sLevelDBLogFile, 
		uint64_t llStartBlkSeq, int iAdjustStrategy);

int CloseFile(DirectIOWriteInfo* ptInfo);

int CreateADirectIOWriteInfo(
		const char* sLevelDBLogFile, 
		int iBlockSize, int iMaxDirectIOBufSize,
		int iMinTailRecordSize, uint64_t llStartBlkSeq, 
		int iAdjustStrategy, uint32_t iOffset, DirectIOWriteInfo*& ptInfo);

bool NeedIterWriteFile(
		const DirectIOWriteInfo* ptInfo, const int iEstimateWriteSize);

int IterWriteFile(
		DirectIOWriteInfo* ptInfo, 
		const std::string& sKvLogPath, 
		const std::string& sKvRecyclePath, 
		uint32_t iNextFileNo, char cPostfix);

int CalculateDumpSize(const DirectIOWriteInfo* ptInfo, int iValLen);

int GetEstimateWriteSize(const DirectIOWriteInfo* ptInfo, int iDumpSize);

int DirectIOWrite(
		DirectIOWriteInfo* ptInfo, 
		const char* pValue, int iValLen, uint32_t& iWriteOffset);

int BatchDirectIOWrite(
		DirectIOWriteInfo* ptInfo, 
		const std::vector<leveldb::Slice>& vecValue, 
		std::vector<uint32_t>& vecOffset);

int DirectIOBufferWrite(
		DirectIOWriteInfo* ptInfo, 
		const char* pValue, int iValLen, uint32_t& iWriteOffset);

int Flush(DirectIOWriteInfo* ptInfo);

int GetDirectIOBufferUsedSize(
		const DirectIOWriteInfo* ptInfo, uint32_t& iUsedSize);

int GetDirectIOBufferUsedBlockSize(
		const DirectIOWriteInfo* ptInfo, uint32_t& iUsedBlockSize);

uint64_t GetDirectIOBufferAccWriteSize(const DirectIOWriteInfo* ptInfo);

int CreateADirectIOReadInfo(
		const char* sLevelDBLogFile, 
		int iMaxDirectIOBufSize, DirectIOReadInfo*& ptInfo);

int CreateADirectIOReadInfo(
		const char* sLevelDBLogFile, 
		char* const pDirectIOBuffer, 
		const int iMaxDirectIOBufSize, DirectIOReadInfo*& ptInfo);

int CreateADirectIOReadInfoReadOnly(
		const char* pBegin, const char* pEnd, 
		uint32_t iOffset, DirectIOReadInfo*& ptInfo);


int CloseFile(DirectIOReadInfo* ptInfo);

void SkipErrorBlock(DirectIOReadInfo* ptInfo);
void EnableCheckBlockMode(DirectIOReadInfo* ptInfo);
bool CheckCurrentBlock(DirectIOReadInfo* ptInfo);

int DirectIORead(
		DirectIOReadInfo* ptInfo, std::string& sValue, uint32_t& iOffset);

int DirectIORead(
		DirectIOReadInfo* ptInfo, uint32_t iOffset, 
		std::string& sValue, uint32_t& iNextOffset);

int DirectIORead(
		DirectIOReadInfo* ptInfo, 
		RLogLevelDBRecord& tLogRecord, std::string& sBuffer);

int DirectIOReadSkipError(
		DirectIOReadInfo* ptInfo, std::string& sValue, uint32_t& iOffset);

int DirectIOReadSkipError(
		DirectIOReadInfo* ptInfo, uint32_t iOffset, 
		std::string& sValue, uint32_t& iNextOffset);


class clsLevelDBLogRawReader
{
public:
	clsLevelDBLogRawReader(const int iMaxDirectIOBufSize);
	~clsLevelDBLogRawReader();

	int OpenFile(const char* sLevelDBLogFile);

	int Read(RLogLevelDBRecord& tLogRecord, std::string& sBuffer);

	uint32_t GetCurrentOffset() const;

private:
	const int m_iMaxDirectIOBufSize;
	char* m_pDirectIOBuffer;

	dbimpl::DirectIOReadInfo* m_ptRInfo;
};


int ReadMetaInfo(
		const char* sFileName, int iDirectIOBufSize, 
		int& iBlockSize, uint64_t& llBlkSeq, uint32_t& iOffset);

int ReadFileMetaInfo(
		const char* sFileName, int& iBlockSize, uint64_t& llStartBlkSeq);


int WriteFileMetaInfo(
		const char* sFileName, 
		const int iBlockSize, const uint64_t llStartBlkSeq);

int DeprecateAllDataBlock(const char* sFileName);

void PrintDirectIOBuffer(DirectIOReadInfo* ptInfo);

void ZeroTailingSpace(
		const int iBlockSize, 
		char* const pNewBuffer, char* const pBlkEnd);


} // namespace dbimpl



