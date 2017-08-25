
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "utils/Thread.h"
#include "utils/Time.h"

namespace Certain
{

bool clsThreadBase::IsExited()
{
	if (m_bExited)
	{
		return true;
	}

	if (!m_bExiting)
	{
		return false;
	}

	int iRet = pthread_kill(m_tID, 0);
	if (iRet == ESRCH)
	{
		m_bExited = true;
		assert(pthread_join(m_tID, NULL) == 0);
	}

	return m_bExited;
}

bool clsThreadBase::CheckIfExiting(uint64_t iStopWaitTimeMS)
{
	if (!m_bStopFlag)
	{
		return false;
	}

	if (m_bExiting)
	{
		return true;
	}

	uint64_t iCurrTimeMS = GetCurrTimeMS();

	if (m_iStopStartTimeMS == 0)
	{
		m_iStopStartTimeMS = iCurrTimeMS;
	}
	else if (m_iStopStartTimeMS + iStopWaitTimeMS <= iCurrTimeMS)
	{
		m_bExiting = true;
	}

	return m_bExiting;
}

} // namespace Certain


