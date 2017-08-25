
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <stdint.h>
#include <cassert>
#include "cutils/log_utils.h"
#include "bitcask_log_impl.h"
#include "db_comm.h"

#define START_FLAG 0x12
#define END_FLAG 0x34

using namespace std;
using dbcomm::SafePRead;
using dbcomm::SafePWrite;
using dbcomm::INVALID_FD;


namespace dbimpl {

char* ToBufferImpl(
		const Record_t& tRecord, 
		const int iRecordSize, 
		char* pBuf, int iBufLen)
{
	assert(NULL != pBuf);
	assert(0 < iRecordSize);
	assert(iRecordSize <= iBufLen);
	assert(static_cast<size_t>(iRecordSize) ==
			RECORD_SIZE(tRecord.cKeyLen, tRecord.iValLen));
	
	char* const pOldBuf = pBuf;

	*pBuf = START_FLAG;
	pBuf += sizeof(uint8_t);

	*pBuf = tRecord.cFlag;
	pBuf += sizeof(uint8_t);

	*pBuf = tRecord.cKeyLen;
	pBuf += sizeof(uint8_t);

	uint32_t iValLen = htonl(tRecord.iValLen);
	memcpy(pBuf, &iValLen, sizeof(uint32_t));
	pBuf += sizeof(uint32_t);

	memcpy(pBuf, tRecord.sKey, tRecord.cKeyLen);
	pBuf += tRecord.cKeyLen;

	memcpy(pBuf, tRecord.pVal, tRecord.iValLen);
	pBuf += tRecord.iValLen;

	uint32_t iVerA = htonl(tRecord.iVerA);
	memcpy(pBuf, &iVerA, sizeof(uint32_t));
	pBuf += sizeof(uint32_t);

	uint32_t iVerB = htonl(tRecord.iVerB);
	memcpy(pBuf, &iVerB, sizeof(uint32_t));
	pBuf += sizeof(uint32_t);

	uint32_t iEncodeRecSize = htonl(iRecordSize);
	memcpy(pBuf, &iEncodeRecSize, sizeof(uint32_t));
	pBuf += sizeof(uint32_t);

	*pBuf = END_FLAG;
	pBuf += sizeof(uint8_t);

	assert(pBuf - pOldBuf == iRecordSize);
	return pBuf;
}


int ToRecordImpl(
		const char* pBuf, const char* pBufEnd,  Record_t& tRecord, int& iRecordSize)
{
	assert(pBuf <= pBufEnd);
	size_t iBufLen = pBufEnd - pBuf;
	if (iBufLen < RECORD_HEAD_SIZE) return -1;
	if (static_cast<uint8_t>(*pBuf) != START_FLAG) return -2;
//	if (*(pBuf + iBufLen - 1) != END_FLAG) return -3;

	pBuf += (sizeof(uint8_t));

	tRecord.cFlag = *pBuf;
	pBuf += sizeof(uint8_t);

	tRecord.cKeyLen = *pBuf;
	pBuf += sizeof(uint8_t);

	tRecord.iValLen = ntohl(*(int32_t*)pBuf);
	pBuf += sizeof(int32_t);

	iRecordSize = RECORD_SIZE(tRecord.cKeyLen, tRecord.iValLen);
	if (static_cast<size_t>(iRecordSize) > iBufLen)
	{
		return -3;
	}

	memcpy(tRecord.sKey, pBuf, tRecord.cKeyLen);
	pBuf += tRecord.cKeyLen;

	tRecord.pVal = const_cast<char*>(pBuf);
	pBuf += tRecord.iValLen;

	tRecord.iVerA = ntohl(*(uint32_t*)pBuf);
	pBuf += sizeof(uint32_t);

	tRecord.iVerB = ntohl(*(uint32_t*)pBuf);
	pBuf += sizeof(uint32_t);

	uint32_t iDecodeRecSize = ntohl(*(uint32_t*)pBuf);
	pBuf += sizeof(uint32_t);

	if (static_cast<uint8_t>(*pBuf) != END_FLAG 
			|| iDecodeRecSize != static_cast<uint32_t>(iRecordSize))
	{
		return -4;
	}

	return 0;
}

inline char* EncodeFix8Bit(char* pIter, uint8_t cValue)
{
	*pIter = cValue;
	return pIter + sizeof(uint8_t);
}

char* AppendRecordHeader(
		const Record_t& tRecord, char* const pBuffer, int iBufLen)
{
	assert(NULL != pBuffer);
	assert(0 < iBufLen);
	assert(static_cast<int>(tRecord.cKeyLen + RECORD_HEAD_SIZE) <= iBufLen);

	char* pIter = pBuffer;	
	// 1. start flag
	pIter = EncodeFix8Bit(pIter, START_FLAG);

	// 2. cFlag
	pIter = EncodeFix8Bit(pIter, tRecord.cFlag);

	// 3. cKeyLen
	pIter = EncodeFix8Bit(pIter, tRecord.cKeyLen);

	// 4. iValLen
	pIter = dbcomm::EncodeNet32Bit(pIter, tRecord.iValLen);	

	// 5. sKey
	memcpy(pIter, tRecord.sKey, tRecord.cKeyLen);
	pIter += tRecord.cKeyLen;
	assert(pIter - pBuffer == 
			static_cast<int>(RECORD_HEAD_SIZE + tRecord.cKeyLen));
	return pIter;
}

char* AppendRecordTail(
		const Record_t& tRecord, 
		const int iRecordSize, char* const pBuffer, int iBufLen)
{
	assert(0 < iRecordSize);
	assert(CalculateRecordSize(tRecord) == iRecordSize);
	assert(static_cast<size_t>(iBufLen) 
			>= sizeof(uint32_t) * 3 + sizeof(uint8_t));

	char* pIter = pBuffer;
	// 1. iVerA
	pIter = dbcomm::EncodeNet32Bit(pIter, tRecord.iVerA);

	// 2. iVerB
	pIter = dbcomm::EncodeNet32Bit(pIter, tRecord.iVerB);

	// 3. iRecordSize
	pIter = dbcomm::EncodeNet32Bit(pIter, iRecordSize);
	
	// 4. end flag 
	pIter = EncodeFix8Bit(pIter, END_FLAG);
	assert(pIter - pBuffer == 
			sizeof(uint32_t) * 3 + sizeof(uint8_t));
	return pIter;
}

namespace BitCaskRecord {

const struct block_head_t* MakeBitCaskRecordHeadPtr(const char* ptr)
{
	return reinterpret_cast<const block_head_t*>(ptr);
}

struct pointer MakeBitCaskRecordPtr(char* ptr)
{
	assert(NULL != ptr);
	struct pointer p = {0};
	p.ptr = ptr;
	p.head = reinterpret_cast<block_head_t*>(ptr);
	p.body = ptr + sizeof(block_head_t) + p.head->cKeyLen;
	p.tail = reinterpret_cast<block_tail_t*>(p.body + p.head->GetValueLen());
	return p;
}

const struct pointer MakeBitCaskRecordPtr(const char* ptr)
{
	return MakeBitCaskRecordPtr(const_cast<char*>(ptr));
}

bool IsAValidRecordHead(const char* const pBufBegin, const char* pBufEnd)
{
	if (pBufEnd < pBufBegin + RECORD_HEAD_SIZE)
	{
		return false;
	}

	const block_head_t* pHead = reinterpret_cast<const block_head_t*>(pBufBegin);
	if (START_FLAG != static_cast<uint8_t>(pHead->c0x12))
	{
		logerr("START_FLAG %u pHead->c0x12 %u pBufBegin %u", 
				START_FLAG, 
				static_cast<uint8_t>(pHead->c0x12), 
				static_cast<uint8_t>(pBufBegin[0]));
		return false;
	}

	return true;
}

bool IsAValidRecord(const char* const pBufBegin, const char* pBufEnd)
{
	if (pBufEnd < pBufBegin + RECORD_HEAD_SIZE)
	{
		return false;
	}

	const block_head_t* pHead = reinterpret_cast<const block_head_t*>(pBufBegin);
	if (START_FLAG != static_cast<uint8_t>(pHead->c0x12))
	{
		return false;
	}

	if (0 > pHead->cKeyLen || 0 > pHead->GetValueLen())
	{
		logerr("ERR cKeyLen %d valuelen %d", 
				pHead->cKeyLen, pHead->GetValueLen());
		return false;
	}

	const int iRecLen = RECORD_SIZE(pHead->cKeyLen, pHead->GetValueLen());
	assert(0 < iRecLen);
	if (pBufEnd < pBufBegin + iRecLen)
	{
		return false;
	}

	const pointer tPtr = MakeBitCaskRecordPtr(pBufBegin);
	if (END_FLAG != static_cast<uint8_t>(tPtr.tail->c0x34))
	{
		return false;
	}
	
	if (iRecLen != tPtr.tail->GetRecordLen())
	{
		return false;
	}

	return true;
}


int GetRecordLen(const char* pBuffer, const char* pBufEnd)
{
	assert(NULL != pBufEnd);
	assert(NULL != pBuffer);
	assert(pBuffer <= pBufEnd);
	if (RECORD_HEAD_SIZE > static_cast<uint32_t>(pBufEnd - pBuffer))
	{
		return -1;
	}

	const block_head_t* pHead = reinterpret_cast<const block_head_t*>(pBuffer);
	if (START_FLAG != static_cast<uint8_t>(pHead->c0x12))
	{
		return BDB_FORMAT_ERROR_BROKEN_START_FLAG;
	}

	int iValLen = pHead->GetValueLen();
	if (0 > pHead->cKeyLen || 0 > iValLen)
	{
		return BDB_FORMAT_ERROR_BROKEN_HEAD;
	}

	assert(0 <= pHead->cKeyLen);
	assert(0 <= iValLen);
	return RECORD_SIZE(pHead->cKeyLen, iValLen);
}


} // namespace BitCaskRecord

bool IsBitCaskFormatError(int iRetCode)
{
	return -100 >= iRetCode && -1000 <= iRetCode;
}

int ReadARawRecord(
		int iFD, uint32_t iOffset, std::string& sRawRecord, uint32_t& iNextOffset)
{
	if (0 > iFD)
	{
		return -1;
	}

	int iSmallReadSize = 0;
	char sHeadBuf[RECORD_HEAD_SIZE] = {0};
	iSmallReadSize = pread(iFD, sHeadBuf, RECORD_HEAD_SIZE, iOffset);
	if (RECORD_HEAD_SIZE != iSmallReadSize)
	{
		if (0 == iSmallReadSize)
		{
			return 1; // EOF
		}

		if (0 > iSmallReadSize)
		{
			return BDB_READ_ERROR_BASIC;
		}

		assert(static_cast<int>(RECORD_HEAD_SIZE) > iSmallReadSize);
		return BDB_READ_ERROR_POOR_READ;
	}

	assert(RECORD_HEAD_SIZE == iSmallReadSize);
	{
		const int iRecLen = 
			BitCaskRecord::GetRecordLen(sHeadBuf, sHeadBuf + RECORD_HEAD_SIZE);
		if (0 > iRecLen)
		{
			assert(-1 != iRecLen);
			return iRecLen;
		}

		assert(0 < iRecLen);
		assert(RECORD_HEAD_SIZE < static_cast<uint32_t>(iRecLen));
		sRawRecord.clear();
		sRawRecord.resize(iRecLen);
	}

	iSmallReadSize = SafePRead(iFD, &sRawRecord[0], sRawRecord.size(), iOffset);
	if (static_cast<size_t>(iSmallReadSize) != sRawRecord.size())
	{
		if (0 >= iSmallReadSize)
		{
			logerr("SafePRead iOffset %u ret %d", iOffset, iSmallReadSize);
			return 0 == iSmallReadSize 
				? BDB_READ_ERROR_UNEXPECTED_EOF : BDB_READ_ERROR_BASIC;
		}

		return BDB_READ_ERROR_POOR_READ;
	}

	if (false == BitCaskRecord::IsAValidRecord(
				sRawRecord.data(), sRawRecord.data() + sRawRecord.size()))
	{
		return BDB_FORMAT_ERROR_BROKEN_RECORD;
	}

	iNextOffset = iOffset + sRawRecord.size();
	return 0; // OK
}

int ReadARawRecordSkipError(
		int iFD, uint32_t iOffset, std::string& sRawRecord, uint32_t& iNextOffset)
{
	if (0 > iFD)
	{
		return -1;
	}

	int ret = ReadARawRecord(iFD, iOffset, sRawRecord, iNextOffset);
	if (0 == ret)
	{
		return 0; // success read
	}

	if (false == IsBitCaskFormatError(ret))
	{
		return ret;
	}

	// else => IsBitCaskFormatError
	uint32_t iSkipCnt = 0;
	char sReadBuf[4096];
	do 
	{
		int iSmallReadSize = pread(iFD, sReadBuf, sizeof(sReadBuf), iOffset + iSkipCnt);
		if (0 >= iSmallReadSize)
		{
			if (0 == iSmallReadSize)
			{
				return 1;
			}

			return BDB_READ_ERROR_BASIC;
		}

		uint32_t iOldSkipCnt = iSkipCnt;
		for (int idx = 0; idx < iSmallReadSize; ++idx)
		{
			if (START_FLAG != static_cast<uint8_t>(sReadBuf[idx]))
			{
				++iSkipCnt;
			}

			ret = ReadARawRecord(iFD, iOffset + iSkipCnt, sRawRecord, iNextOffset);
			if (0 == ret)
			{
				return 0; // success read;
			}

			if (false == IsBitCaskFormatError(ret))
			{
				return 1 == ret ? BDB_READ_ERROR_UNEXPECTED_EOF : ret;
			}

			// else bit cask format error
			++iSkipCnt;
		}

		assert(iOldSkipCnt + iSmallReadSize == iSkipCnt);
	} while (true);

	// never reach here
	assert(0);
	return -1;
}


} // namespace dbimpl

