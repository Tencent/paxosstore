
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_IOCHANNEL_H_
#define CERTAIN_IOCHANNEL_H_

#include "network/EpollIO.h"

namespace Certain
{

#define IO_BUFFER_SIZE 			(42 << 20) // 42MB == 2 * MAX_WRITEBATCH_SIZE + 2000

class clsIOChannel : public clsFDBase
{
private:
	ConnInfo_t m_tConnInfo;

	// 0 <= iStart0 <= iEnd0 <= iStart1 <= iEnd1 <= iSize
	// The valid data is [iStart0, iEnd0) and [iStart1, iEnd1).
	struct CBuffer_t
	{
		char *pcData;
		uint32_t iSize;

		uint32_t iStart0;
		uint32_t iEnd0;

		uint32_t iStart1;
		uint32_t iEnd1;
	};

	void InitCBuffer(CBuffer_t *ptBuffer)
	{
		ptBuffer->pcData = (char *)malloc(IO_BUFFER_SIZE);
		ptBuffer->iSize = IO_BUFFER_SIZE;

		ptBuffer->iStart0 = 0;
		ptBuffer->iEnd0 = 0;

		ptBuffer->iStart1 = ptBuffer->iSize;
		ptBuffer->iEnd1 = ptBuffer->iSize;
	}

	void UpdateBuffer(CBuffer_t *ptBuffer)
	{
		AssertNotMore(ptBuffer->iStart0, ptBuffer->iEnd0);
		AssertNotMore(ptBuffer->iEnd0, ptBuffer->iStart1);
		AssertNotMore(ptBuffer->iStart1, ptBuffer->iEnd1);
		AssertNotMore(ptBuffer->iEnd1, ptBuffer->iSize);

		if (ptBuffer->iStart1 < ptBuffer->iEnd1)
		{
			AssertEqual(ptBuffer->iStart0, 0);
			return;
		}

		AssertEqual(ptBuffer->iStart1, ptBuffer->iEnd1);

		ptBuffer->iStart1 = ptBuffer->iSize;
		ptBuffer->iEnd1 = ptBuffer->iSize;

		if (ptBuffer->iStart0 == ptBuffer->iEnd0)
		{
			ptBuffer->iStart0 = 0;
			ptBuffer->iEnd0 = 0;
			return;
		}

		if (ptBuffer->iStart1 - ptBuffer->iEnd0 <= ptBuffer->iStart0)
		{
			ptBuffer->iStart1 = ptBuffer->iStart0;
			ptBuffer->iEnd1 = ptBuffer->iEnd0;

			ptBuffer->iStart0 = 0;
			ptBuffer->iEnd0 = 0;
		}
	}

	uint32_t GetSize(CBuffer_t *ptBuffer)
	{
		uint32_t iSize = (ptBuffer->iEnd1 - ptBuffer->iStart1) + (
				ptBuffer->iEnd0 - ptBuffer->iStart0);
		return iSize;
	}

	void DestroyCBuffer(CBuffer_t *ptBuffer)
	{
		free(ptBuffer->pcData), ptBuffer->pcData = NULL;
		memset(ptBuffer, 0, sizeof(CBuffer_t));
	}

	string m_strReadBytes;
	CBuffer_t m_tWriteBuffer;

	uint64_t m_iTotalReadCnt;
	uint64_t m_iTotalWriteCnt;

	uint64_t m_iTimestamp;

	uint32_t m_iServerID;

	bool m_bBroken;

public:
	class clsSerializeCBBase
	{
	public:
		virtual int Call(char *pcBuffer, uint32_t iSize) = 0;
		virtual ~clsSerializeCBBase()
		{
		}
	};

	clsIOChannel(clsIOHandlerBase *poHandler,
				 const ConnInfo_t &tConnInfo,
				 uint32_t iServerID = -1,
				 uint32_t iFDID = -1) :
		clsFDBase(tConnInfo.iFD, poHandler, iFDID),
		m_tConnInfo(tConnInfo),
		m_iTotalReadCnt(0),
		m_iTotalWriteCnt(0),
		m_iTimestamp(0),
		m_iServerID(iServerID),
		m_bBroken(false)
	{
		InitCBuffer(&m_tWriteBuffer);
	}

	virtual ~clsIOChannel()
	{
		PrintInfo();
		DestroyCBuffer(&m_tWriteBuffer);
	}

	int Read(char *pcBuffer, uint32_t iSize);
	int Write(const char *pcBuffer, uint32_t iSize);

	int FlushWriteBuffer();

	TYPE_GET_SET(uint64_t, Timestamp, iTimestamp);

	// Return 0 iff append successfully.
	int AppendReadBytes(const char *pcBuffer, uint32_t iSize);
	int AppendWriteBytes(const char *pcBuffer, uint32_t iSize);
	int AppendWriteBytes(clsSerializeCBBase *poSerializeCB);

	uint32_t GetWriteByteSize()
	{
		return GetSize(&m_tWriteBuffer);
	}

	uint32_t GetServerID() { return m_iServerID; }

	const ConnInfo_t &GetConnInfo() { return m_tConnInfo; }

	bool IsBroken() { return m_bBroken; }

	bool IsBufferBusy()
	{
		// Check if above 1/8 IO_BUFFER_SIZE.
		return (GetWriteByteSize() << 3) > IO_BUFFER_SIZE;
	}

	void PrintInfo();
};

} // namespace Certain

#endif
