
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UUIDGROUPMNG_H_
#define CERTAIN_UUIDGROUPMNG_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsUUIDGroup
{
private:
	// iUUID --> iTimeout
	clsLRUTable<uint64_t, uint32_t> *m_poLRUTable;

	clsRWLock m_oRWLock;

	bool CheckTimeout(uint64_t iCheckUUID = 0);

public:
	clsUUIDGroup()
	{
		m_poLRUTable = new clsLRUTable<uint64_t, uint32_t>(MAX_UUID_NUM);
	}

	// compatible with gperftools
#if 0
	~clsUUIDGroup()
	{
		delete m_poLRUTable;
	}
#endif

	size_t Size();

	bool IsUUIDExist(uint64_t iUUID);
	bool AddUUID(uint64_t iUUID);
};

class clsUUIDGroupMng : public clsSingleton<clsUUIDGroupMng>
{
private:
	clsUUIDGroup aoGroup[UUID_GROUP_NUM];

	// compatible with gperftools
	friend class clsSingleton<clsUUIDGroupMng>;
	clsUUIDGroupMng() { }

public:
	int Init(clsConfigure *poConf) { return 0; }
	void Destroy() { }

	bool IsUUIDExist(uint64_t iEntityID, uint64_t iUUID);

	bool AddUUID(uint64_t iEntityID, uint64_t iUUID);
};

} // namespace Certain

#endif
