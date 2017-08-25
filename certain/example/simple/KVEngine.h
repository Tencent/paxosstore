
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_EXAMPLE_SIMPLE_KVEngine_H_
#define CERTAIN_EXAMPLE_SIMPLE_KVEngine_H_

#include "Certain.h"

class clsKVEngine
{
private:
	typedef map<string, string> KVEngine_t;
	KVEngine_t m_tKVEngine;

	Certain::clsRWLock m_oRWLock;

	int PutInner(const string &strKey, const string &strValue)
	{
		m_tKVEngine[strKey] = strValue;

		return Certain::eRetCodeOK;
	}

public:
	int Put(const string &strKey, const string &strValue);

	int MultiPut(const vector< pair<string, string> > &vecKeyValue);

	int Get(const string &strKey, string &strValue);

	bool Find(const string &strKey);

	int RangeGet(const string &strStart, const string &strEnd,
			vector< pair<string, string> > &vecKeyValue);
};

#endif
