
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_EXAMPLE_SIMPLE_SIMPLEDB_H_
#define CERTAIN_EXAMPLE_SIMPLE_SIMPLEDB_H_

#include "Command.h"
#include "Certain.h"
#include "simple/KVEngine.h"

namespace Certain
{

class clsDBImpl : public clsDBBase
{
private:
	static const uint32_t kBufferSize = (1 << 16);
	char m_pcBuffer[kBufferSize];

private:
	clsKVEngine *m_poKVEngine;

	int SubmitGet(clsSimpleCmd *poCmd);

	int SubmitSet(clsSimpleCmd *poCmd, string &strWriteBatch);

	void CheckMaxCommitedEntry(uint64_t iEntityID, uint64_t iEntry);

public:
	clsDBImpl(clsKVEngine *poKVEngine) : m_poKVEngine(poKVEngine) { }

	virtual ~clsDBImpl() { }

	virtual int Submit(clsClientCmd *poClientCmd, string &strWriteBatch);

	virtual int Commit(uint64_t iEntityID, uint64_t iEntry,
			const string &strWriteBatch);

	virtual int LoadMaxCommitedEntry(uint64_t iEntityID,
			uint64_t &iCommitedEntry, uint32_t &iFlag);
};

} // namespace Certain

#endif
