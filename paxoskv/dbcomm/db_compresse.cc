
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cassert>
#include <string>
#include "snappy.h"
#include "db_comm.h"
#include "db_compresse.h"

namespace {


const static uint32_t MIN_COMPRESSE_LEN = 10;

} // namespace


namespace dbcomm {


namespace Compresse {


bool MayCompresse(
		const char* pRawValue, 
		uint32_t iRawValLen, const uint8_t cFlag, 
		std::string& sCompressBuffer)
{
	if (NULL == pRawValue || 
			MIN_COMPRESSE_LEN > iRawValLen || 
			0 != (cFlag & dbcomm::RECORD_COMPRESSE))
	{
		return false;
	}

	assert(NULL != pRawValue);
	assert(0 < iRawValLen);
	size_t iCompresseLen = snappy::MaxCompressedLength(iRawValLen);
	assert(0 < iCompresseLen);

	sCompressBuffer.resize(iCompresseLen);
	snappy::RawCompress(
			pRawValue, iRawValLen, &sCompressBuffer[0], &iCompresseLen);
	if (iCompresseLen >= static_cast<size_t>(iRawValLen))
	{
		sCompressBuffer.clear();
		return false;
	}

	assert(iCompresseLen < static_cast<size_t>(iRawValLen));
	sCompressBuffer.resize(iCompresseLen);
	assert(false == sCompressBuffer.empty());
	return true;
}


int MayUnCompresse(
		const char* pRawValue, 
		uint32_t iRawValLen, const uint8_t cFlag, 
		std::string& sUnCompresseBuffer)
{
	if (0 == (cFlag & dbcomm::RECORD_COMPRESSE))
	{
		return 0;
	}

	assert(0 != (cFlag & dbcomm::RECORD_COMPRESSE));
	if (NULL == pRawValue || 0 == iRawValLen)
	{
		return -10001;
	}

	assert(NULL != pRawValue);
	assert(0 < iRawValLen);

	size_t iUnCompresseLen = 0;
	snappy::GetUncompressedLength(
            pRawValue, iRawValLen, &iUnCompresseLen);
	if (0 == iUnCompresseLen) 
	{
		return -10002;
	}
	
	assert(0 < iUnCompresseLen);
	sUnCompresseBuffer.resize(iUnCompresseLen);	
	if (!snappy::RawUncompress(
                pRawValue, iRawValLen, &sUnCompresseBuffer[0]))
	{
		sUnCompresseBuffer.clear();
		return -10003;
	}

	assert(false == sUnCompresseBuffer.empty());
	return 1;
}



} // namespace Compresse


} // namespace dbcomm



