
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "KVEngine.h"

int clsKVEngine::Put(const string &strKey, const string &strValue)
{
	Certain::clsThreadWriteLock oWLock(&m_oRWLock);
	return PutInner(strKey, strValue);
}

int clsKVEngine::MultiPut(const vector< pair<string, string> > &vecKeyValue)
{
	Certain::clsThreadWriteLock oWLock(&m_oRWLock);
	for (vector< pair<string, string> >::const_iterator iter
			= vecKeyValue.begin(); iter != vecKeyValue.end(); ++iter)
	{
		PutInner(iter->first, iter->second);
	}
	return Certain::eRetCodeOK;
}

int clsKVEngine::Get(const string &strKey, string &strValue)
{
	Certain::clsThreadReadLock oRLock(&m_oRWLock);

	if (m_tKVEngine.find(strKey) == m_tKVEngine.end())
	{
		return Certain::eRetCodeNotFound;
	}

	strValue = m_tKVEngine[strKey];

	return Certain::eRetCodeOK;
}

bool clsKVEngine::Find(const string &strKey)
{
	Certain::clsThreadReadLock oRLock(&m_oRWLock);
	return m_tKVEngine.find(strKey) != m_tKVEngine.end();
}

int clsKVEngine::RangeGet(const string &strStart, const string &strEnd,
		vector< pair<string, string> > &vecKeyValue)
{
	vecKeyValue.clear();
	Certain::clsThreadReadLock oRLock(&m_oRWLock);

	KVEngine_t::iterator iter = m_tKVEngine.upper_bound(strStart);
	while (iter != m_tKVEngine.end() && iter->first < strEnd)
	{
		vecKeyValue.push_back(*iter);
		++iter;
	}

	return Certain::eRetCodeOK;
}
