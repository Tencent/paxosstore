
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UTILS_AOF_H_
#define CERTAIN_UTILS_AOF_H_

#include "utils/Assert.h"
#include "utils/Time.h"
#include "utils/Thread.h"

namespace Certain
{

// A single file appended only by multi-thread.
class clsAppendOnlyFile
{
private:
	static const uint32_t kBufferNum = 4;
	static const uint32_t kTryTime = 5;

	struct Buffer_t
	{
		char *pcData;
		uint64_t iMaxSize;
		uint64_t iSyncSize;

		volatile uint64_t iCurrPos;
		volatile uint64_t iWritenCnt;
		volatile uint64_t iTotalSyncCnt;
	};

	int m_iFD;
	string m_strPath;

	Buffer_t m_atBuffer[kBufferNum];
	Buffer_t *m_apBuffer[kTryTime];

	clsMutex m_oFlushMutex;

	uint64_t m_iFileSize;

	int TryAppend(Buffer_t *ptBuffer, const char *pcData,
			uint32_t iLen);

	void FlushInner(Buffer_t *ptBuffer, bool bAsync);

	void SyncBuffer(Buffer_t *ptBuffer);

public:
	clsAppendOnlyFile(const char * pcPath, uint64_t iSyncSize,
			uint64_t iBufferSize);

	~clsAppendOnlyFile();

	int Write(const char *pcData, uint32_t iLen);

	// Use default before false assertion.
	// Use bAsync(true) For the purpose of period disk sync.
	void Flush(bool bAsync = false);

	uint64_t GetFileSize();
};

int MakeCertainDir(const char *pcPath);

} // namespace Certain

#endif
