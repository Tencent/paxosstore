
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <string>
#include <algorithm>
#include <cassert>
#include "snappy.h"
#include "core/err_code.h"
#include "core/paxos.pb.h"
#include "cutils/log_utils.h"
#include "dbcomm/db_comm.h"
#include "mem_compressor.h"
#include "memcomm.h"


namespace memkv {

size_t MaxCompressedLength(const paxos::PaxosLog& oPLog) 
{
	size_t unCompressedLength = oPLog.ByteSize();
    if (MIN_COMPRESS_LEN <= unCompressedLength && 
            enable_mem_compresse()) { 
		size_t maxCompressedLength = 
            snappy::MaxCompressedLength(unCompressedLength);
		return std::max(unCompressedLength, maxCompressedLength);
	}

	return oPLog.ByteSize();
}

int PLogToNewHead(const paxos::PaxosLog& oPLog, HeadWrapper& oHead)
{
	assert(oHead.pData != NULL);

	char* pData = oHead.pData;
	uint8_t& cFlag = *(oHead.pFlag);
	uint32_t& iDataLen = *(oHead.pDataLen);

	size_t iPLogByte = oPLog.ByteSize();
    if (MIN_COMPRESS_LEN > iPLogByte || 
            false == enable_mem_compresse()) {
		if (oPLog.SerializeToArray(pData, iPLogByte) == false)
		{
			logerr("SERIALIZE_PAXOS_LOG_ERR");
			return SERIALIZE_PAXOS_LOG_ERR;
		}

		iDataLen = iPLogByte;
		cFlag = dbcomm::ClearFlag(cFlag, FLAG_COMPRESSE);
		return 0;
	}

    std::string sPLogBuffer;
	if (oPLog.SerializeToString(&sPLogBuffer) == false)
	{
		logerr("SERIALIZE_PAXOS_LOG_ERR");
		return SERIALIZE_PAXOS_LOG_ERR;
	}
	assert(sPLogBuffer.length() == iPLogByte);

	size_t compressDataLen = 0;
	snappy::RawCompress(sPLogBuffer.data(), iPLogByte, pData, &compressDataLen);
	if (compressDataLen >= iPLogByte) 
	{
		iDataLen = iPLogByte;
		memcpy(pData, sPLogBuffer.data(), iPLogByte);
		cFlag = dbcomm::ClearFlag(cFlag, FLAG_COMPRESSE);

		//printf("MemCompressor: %d => %d, not compressed\n", (int)iPLogByte, (int)iDataLen);
	}
	else
	{
		iDataLen = compressDataLen;
		cFlag = dbcomm::AddFlag(cFlag, FLAG_COMPRESSE);

		//printf("MemCompressor: %d => %d, compressed\n", (int)iPLogByte, (int)iDataLen);
	}

	return 0;
}

int NewHeadToPlog(const HeadWrapper& oHead, paxos::PaxosLog& oPLog) 
{
	assert(oHead.pData != NULL);

	char* pData = oHead.pData;
	uint8_t& cFlag = *(oHead.pFlag);
	uint32_t& iDataLen = *(oHead.pDataLen);

	if (dbcomm::TestFlag(cFlag, FLAG_COMPRESSE) == false)
	{
		if (oPLog.ParseFromArray(pData, iDataLen) == false) 
		{
			logerr("SERIALIZE_PAXOS_LOG_ERR");
			return SERIALIZE_PAXOS_LOG_ERR;
		}
		return 0;
	}

	size_t unCompressedLength = 0;
	if (snappy::GetUncompressedLength(
				pData, iDataLen, &unCompressedLength) == false)
	{
		logerr("snappy::GetUncompressedLength error.");
		return -1;
	}
	
	char* sPLogBuffer = (char*)malloc(unCompressedLength);
	if (sPLogBuffer == NULL)
	{
		logerr("malloc sPLogBuffer failed.");
		return -2;
	}

	if (snappy::RawUncompress(pData, iDataLen, sPLogBuffer) == false) 
	{
		logerr("snappy::RawUncompress error.");
		free(sPLogBuffer);
		return -3;
	}

	if (oPLog.ParseFromArray(sPLogBuffer, unCompressedLength) == false) 
	{
		logerr("SERIALIZE_PAXOS_LOG_ERR");
		free(sPLogBuffer);
		return SERIALIZE_PAXOS_LOG_ERR;
	}

	free(sPLogBuffer);
	return 0;
}

bool HasCompresseFlag(const HeadWrapper& oHead)
{
	uint8_t& cFlag = *(oHead.pFlag);
	return dbcomm::TestFlag(cFlag, FLAG_COMPRESSE);
}


int MayUnCompresse(
		const HeadWrapper& oHead, 
		char*& pValue, 
		uint32_t& iValLen)
{
	assert(oHead.pData != NULL);
	assert(0 == iValLen);

	char* pData = oHead.pData;
	uint32_t& iDataLen = *(oHead.pDataLen);

	if (0 == iDataLen) {
		return 0;
	}

	assert(0 < iDataLen);
	if (false == HasCompresseFlag(oHead)) {
		pValue = reinterpret_cast<char*>(malloc(iDataLen));
		assert(NULL != pValue);
		memcpy(pValue, pData, iDataLen);
		iValLen = iDataLen;
		return 0;
	}

	size_t iUnCompressedLength = 0;
	if (false == 
			snappy::GetUncompressedLength(
				pData, iDataLen, &iUnCompressedLength)) 
	{
		logerr("snappy::GetUncompressedLength ERROR");
		return -1;
	}

	assert(0 < iUnCompressedLength);
	pValue = reinterpret_cast<char*>(malloc(iUnCompressedLength));
	assert(NULL != pValue);

	if (false == snappy::RawUncompress(
				pData, iDataLen, pValue)) 
	{
		logerr("snappy::RawUncompress ERROR");
		free(pValue);
		pValue = NULL;
		return -2;
	}

	iValLen = iUnCompressedLength;
	assert(0 < iValLen);
	return 0;
}

} // namespace memkv
