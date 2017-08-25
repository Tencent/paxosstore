
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_WAKEUPPIPEMNG_H_
#define CERTAIN_WAKEUPPIPEMNG_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsWakeUpPipeMng : public clsSingleton<clsWakeUpPipeMng>
{
private:
	typedef pair<int, int> Pipe_t;
	vector< Pipe_t > vecPipe;

	clsMutex m_oMutex;

public:
	clsWakeUpPipeMng() { }

	void NewPipe(int &iInFD)
	{
		iInFD = 0;
		int iOutFD = 0;

		AssertEqual(MakeNonBlockPipe(iInFD, iOutFD), 0);

		Pipe_t tPipe;
		tPipe.first = iInFD;
		tPipe.second = iOutFD;

		printf("NewPipe iInFD %d\n", iInFD);

		clsThreadLock oLock(&m_oMutex);
		vecPipe.push_back(tPipe);
	}

	void WakeupAll()
	{
		for (uint32_t i = 0; i < vecPipe.size(); ++i)
		{
			int iOutFD = vecPipe[i].second;

			write(iOutFD, "x", 1);
		}
	}
};

} // namespace Certain

#endif
