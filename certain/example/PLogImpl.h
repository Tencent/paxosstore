
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_EXAMPLE_SIMPLE_SIMPLEPLOG_H_
#define CERTAIN_EXAMPLE_SIMPLE_SIMPLEPLOG_H_

#include "Certain.h"
#include "simple/KVEngine.h"

namespace Certain
{

class clsPLogImpl : public clsPLogBase
{
private:
	struct EntryKey_t
	{
		uint64_t iEntityID;
		uint64_t iEntry;
		uint64_t iValueID;

		EntryKey_t() { }

		EntryKey_t(uint64_t iEntityID, uint64_t iEntry,
				uint64_t iValueID = 0)
		{
			this->iEntityID = iEntityID;
			this->iEntry = iEntry;
			this->iValueID = iValueID;
		}
	};

	string Serialize(const EntryKey_t &tKey)
	{
		return string((const char *)&tKey, sizeof(tKey));
	}

	EntryKey_t Parse(const string &strKey)
	{
		AssertEqual(strKey.size(), sizeof(EntryKey_t));
		return *(EntryKey_t *)(strKey.data());
	}

	clsKVEngine *m_poKVEngine;

public:
	clsPLogImpl(clsKVEngine *poKVEngine)
			: m_poKVEngine(poKVEngine) { }

	virtual ~clsPLogImpl() { }

	virtual int PutValue(uint64_t iEntityID, uint64_t iEntry,
			uint64_t iValueID, const string &strValue);

	virtual int GetValue(uint64_t iEntityID, uint64_t iEntry,
			uint64_t iValueID, string &strValue);

	virtual int Put(uint64_t iEntityID, uint64_t iEntry,
			const string &strRecord);

	virtual int Get(uint64_t iEntityID, uint64_t iEntry,
			string &strRecord);

	virtual int LoadUncommitedEntrys(uint64_t iEntityID,
			uint64_t iMaxCommitedEntry, uint64_t iMaxLoadingEntry,
			vector< pair<uint64_t, string> > &vecRecord, bool &bHasMore);
};

} // namespace Certain

#endif
