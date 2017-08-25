
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <arpa/inet.h>
#include <stdint.h>
#include <string>

#define RECORD_HEAD_SIZE ((sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t)))
#define RECORD_SIZE(cKeyLen, iValLen)\
   	((sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + (cKeyLen) + (iValLen) +\
	 sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t)))
#define FAKE_RECORD_META_SIZE (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + 8 + \
		sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t))
#define MIN_RECORD_SIZE (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + 0 + 0 + \
		sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t))


enum {
	// [-100, -1000] bitcast db record format error
	BDB_FORMAT_ERROR_BROKEN_START_FLAG = -100, 
	BDB_FORMAT_ERROR_BROKEN_HEAD = -101, 
	BDB_FORMAT_ERROR_BROKEN_RECORD = -102, 

	// (-1000, -2000] read/write error
	// :  (-1000, -1500]  read error
	BDB_READ_ERROR_BASIC = -1001, 
	BDB_READ_ERROR_UNEXPECTED_EOF = -1002, 
	BDB_READ_ERROR_POOR_READ = -1003, 

	// :  (-1500, -2000]  write error
	BDB_WRITE_ERROR_BASIC = -1501, 
	BDB_WRITE_ERROR_UNEXPECTED_ZERO = -1502, 
	BDB_WRITE_ERROR_POOR_WRITE = -1503, 
};


namespace dbimpl {

typedef struct s_Record
{
//	uint8_t cStartFlag;
	uint8_t cFlag;
	uint8_t cKeyLen;
	uint32_t iValLen;
	char sKey[8];
	char *pVal;
	uint32_t iVerA;
	uint32_t iVerB;
//	uint32_t iRecLen;
//	uint8_t cEndFlag;
} Record_t;

typedef struct RecordWithPos
{
	Record_t* pRecord;
	char* pBuf;
	uint32_t iLen;
	uint32_t iFileNo;
	uint32_t iOffset;
	uint32_t iHash;

	RecordWithPos()
	{
		pRecord = NULL;
		pBuf = NULL;
		iLen = 0;
		iFileNo = 0;
		iOffset = 0;
		iHash = 0;
	}
	
}RecordWithPos_t;


char* ToBufferImpl(
		const Record_t& tRecord, 
        int iRecordSize, char* pBuffer, int iBufLen);

int ToRecordImpl(
		const char* pBuf, 
        const char* pBufEnd, 
        Record_t& tRecord, int& iRecordSize);

inline int CalculateRecordSize(const Record_t& tRecord)
{
	return RECORD_SIZE(tRecord.cKeyLen, tRecord.iValLen);
}

char* AppendRecordHeader(
		const Record_t& tRecord, char* pBuffer, int iBufLen);

char* AppendRecordTail(
		const Record_t& tRecord, 
		const int iRecordSize, char* pBuffer, int iBufLen);

namespace BitCaskRecord {

struct block_head_t
{
	char c0x12;	
	char cFlag;
	char cKeyLen;
	uint32_t iValLen; //need ntohl
	char sKey[0];
	
	int GetValueLen() const 
	{
		return ntohl(iValLen);
	}

}__attribute__ ((packed));

struct block_tail_t
{
	uint32_t iVerA; //need ntohl
	uint32_t iVerB; //need ntohl
	uint32_t iRecLen; //need ntohl
	char c0x34;

	uint32_t GetVerA() const
	{
		return ntohl(iVerA);
	}

	uint32_t GetVerB() const
	{
		return ntohl(iVerB);
	}

	int GetRecordLen() const
	{
		return ntohl(iRecLen);
	}

}__attribute__ ((packed));


struct pointer
{
	char* ptr;
	block_head_t* head;
	char* body;
	block_tail_t* tail;
};

const struct block_head_t* MakeBitCaskRecordHeadPtr(const char* ptr);
struct pointer MakeBitCaskRecordPtr(char* ptr);
const struct pointer MakeBitCaskRecordPtr(const char* ptr);

bool IsAValidRecordHead(const char* const pBufBegin, const char* pBufEnd);
bool IsAValidRecord(const char* const pBufBegin, const char* pBufEnd);

} // namespace BitCaskRecord

bool IsBitCaskFormatError(int iRetCode);

int ReadARawRecord(
		int iFD, uint32_t iOffset, std::string& sRawRecord, uint32_t& iNextOffset);

int ReadARawRecordSkipError(
		int iFD, uint32_t iOffset, std::string& sRawRecord, uint32_t& iNextOffset);


} // namespace dbimpl
