
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_ASYNCPIPEMNG_H_
#define CERTAIN_ASYNCPIPEMNG_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsAsyncPipeMng : public clsSingleton<clsAsyncPipeMng>
{
private:
	int m_aaAsyncPipe[MAX_ASYNC_PIPE_NUM][2];
	list<uint32_t> m_tIdleIdxList;

    // group_id -> num of concurr;
    uint32_t m_aiGroupCnt[MAX_CONTROL_GROUP_NUM];

    // pipe_id -> entity_id
    uint64_t m_aiEntityIDMap[MAX_ASYNC_PIPE_NUM];

    uint32_t m_iMaxGroupLimit;

	clsMutex m_oMutex;

public:
	clsAsyncPipeMng() { }

	int Init(clsConfigure *poConf);

	void Destroy();

	int GetIdlePipeIdx(uint32_t &iIdx, uint64_t iEntityID);

	void PutIdlePipeIdx(uint32_t iIdx);

	// iPtr is for check only.
	int SyncWriteByPipeIdx(uint32_t iIdx, uintptr_t iPtr);

	int SyncWaitByPipeIdx(uint32_t iIdx, uintptr_t iPtr);
};

} // namespace Certain

#endif
