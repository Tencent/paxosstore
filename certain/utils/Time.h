
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UTILS_TIME_H_
#define CERTAIN_UTILS_TIME_H_

#include "utils/Logger.h"

#include "utils/FooHook.h"

namespace Certain
{

#define TIMERUS_START(x) uint64_t x = _GetTickCount()
#define TIMERUS_STOP(x)  x = (_GetTickCount() - x) / (_GetCpuHz() / 1000000)

#define TIMERMS_START(x) uint64_t x = _GetTickCount()
#define TIMERMS_STOP(x)  x = (_GetTickCount() - x) / (_GetCpuHz() / 1000)

inline uint64_t GetCurrTimeUS()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

inline uint64_t GetCurrTimeMS()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

inline uint32_t GetCurrTime()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	return (uint32_t)tv.tv_sec;
}

inline uint32_t GetCurrentHour()
{
	time_t iNow = time(NULL);
	struct tm stm;
	if (localtime_r(&iNow, &stm) == NULL)
	{
		return uint32_t(-1);
	}
	return stm.tm_hour;
}

class clsSmartSleepCtrl
{
private:
	uint64_t m_iInitTimeoutUS;
	uint64_t m_iTimeoutUS;
	uint64_t m_iMaxTimeoutUS;

public:
	clsSmartSleepCtrl(uint64_t iInitTimeoutUS, uint64_t iMaxTimeoutUS)
	{   
		m_iInitTimeoutUS = iInitTimeoutUS;
		m_iTimeoutUS = iInitTimeoutUS;
		m_iMaxTimeoutUS = iMaxTimeoutUS;
	}

	~clsSmartSleepCtrl() { }

	void Reset()
	{   
		m_iTimeoutUS = m_iInitTimeoutUS;
	}

	void Sleep()
	{   
		usleep(m_iTimeoutUS);

		if (m_iTimeoutUS < m_iMaxTimeoutUS)
		{   
			m_iTimeoutUS <<= 1;

			if (m_iTimeoutUS > m_iMaxTimeoutUS)
			{   
				m_iTimeoutUS = m_iMaxTimeoutUS;
			}
		}
	}
};

} // namespace Certain

#endif // CERTAIN_UTIL_TIME_H_
