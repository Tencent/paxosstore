
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "UUIDGroupMng.h"

namespace Certain
{

bool clsUUIDGroup::CheckTimeout(uint64_t iCheckUUID)
{
	uint64_t iCurrTime = GetCurrTime();

	uint64_t iUUID;
	uint32_t iTimeout;

	bool bCheckUUIDRemoved = false;

	while (m_poLRUTable->PeekOldest(iUUID, iTimeout))
	{
		if (iTimeout > iCurrTime)
		{
			break;
		}

		Assert(m_poLRUTable->Remove(iUUID));

		if (iUUID == iCheckUUID)
		{
			bCheckUUIDRemoved = true;
		}
	}

	return bCheckUUIDRemoved;
}

size_t clsUUIDGroup::Size()
{
	clsThreadReadLock oReadLock(&m_oRWLock);

	return m_poLRUTable->Size();
}

bool clsUUIDGroup::IsUUIDExist(uint64_t iUUID)
{
	{
		clsThreadReadLock oReadLock(&m_oRWLock);

		if (!m_poLRUTable->Find(iUUID))
		{
			return false;
		}
	}

	{
		clsThreadWriteLock oWriteLock(&m_oRWLock);

		return !CheckTimeout(iUUID);
	}
}

bool clsUUIDGroup::AddUUID(uint64_t iUUID)
{
	clsThreadWriteLock oWriteLock(&m_oRWLock);

	CheckTimeout();

	uint32_t iTimeout = GetCurrTime() + 3600; // 1hour

	return m_poLRUTable->Add(iUUID, iTimeout);
}

bool clsUUIDGroupMng::IsUUIDExist(uint64_t iEntityID, uint64_t iUUID)
{
	uint32_t iGroupID = Hash(iEntityID) % UUID_GROUP_NUM;
	return aoGroup[iGroupID].IsUUIDExist(iUUID);
}

bool clsUUIDGroupMng::AddUUID(uint64_t iEntityID, uint64_t iUUID)
{
	uint32_t iGroupID = Hash(iEntityID) % UUID_GROUP_NUM;
	return aoGroup[iGroupID].AddUUID(iUUID);
}

} // namespace Certain


