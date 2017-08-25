
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <cstring>
#include <cassert>
#include "cutils/log_utils.h"
#include "pipe_alloc.h"

namespace dbcomm {

namespace {

inline void AssertEmptyPipePair(
        const clsPipeAllocator::PipePair& tPipePair)
{
	assert(-1 == tPipePair.iPipe[0]);
	assert(-1 == tPipePair.iPipe[1]);
}

inline void AssertValidPipePair(
        const clsPipeAllocator::PipePair& tPipePair)
{
	assert(0 <= tPipePair.iPipe[0]);
	assert(0 <= tPipePair.iPipe[1]);
}

void swap(clsPipeAllocator::PipePair& a, clsPipeAllocator::PipePair& b)
{
	std::swap(a.iPipe[0], b.iPipe[0]);
	std::swap(a.iPipe[1], b.iPipe[1]);
}

} // namespace


clsPipeAllocator::clsPipeAllocator(
		size_t iPrevAllocSize, size_t iReserveSize)
	: m_iVecPipeUsed(0)
{
	m_vecPipe.reserve(iReserveSize);
	if (0 < iPrevAllocSize)
	{
		PipePair defPipePair = {{-1, -1}};
		m_vecPipe.resize(iPrevAllocSize, defPipePair);
		for (size_t i = 0; i < iPrevAllocSize; ++i) 
		{
			int ret = pipe(m_vecPipe[i].iPipe);
			if (0 != ret) 
			{
				logerr("pipe ret %d strerror %s", 
                        ret, strerror(errno));
				assert(false);
			}
			assert(0 == ret);
		}
	}
}

clsPipeAllocator::~clsPipeAllocator()
{
	for (size_t i = 0; i < m_vecPipe.size(); ++i) 
	{
		PipePair& tPair = m_vecPipe[i];
		close(tPair.iPipe[0]);
		close(tPair.iPipe[1]);
	}
}

bool clsPipeAllocator::AllocPipe(PipePair& tPipePair)
{
	AssertEmptyPipePair(tPipePair);
	if (true == m_vecPipe.empty())
	{
		int ret = pipe(tPipePair.iPipe);
		if (0 != ret) 
		{
			logerr("pipe ret %d strerror %s", ret, strerror(errno));
			m_vecPipe.resize(m_vecPipe.size() - 2);
			return false;
		}

		++m_iVecPipeUsed;
		assert(0 == ret);
		return true;
	}

	++m_iVecPipeUsed;
	assert(false == m_vecPipe.empty());
	swap(tPipePair, m_vecPipe.back());
	AssertValidPipePair(tPipePair);
	m_vecPipe.pop_back();
	return true;
}

void clsPipeAllocator::FreePipe(PipePair* ptPipePair, size_t iSize)
{
	if (0 == iSize) 
	{
		return ;
	}

	size_t iPrevSize = m_vecPipe.size();
	m_vecPipe.resize(iPrevSize + iSize);
	memcpy(&m_vecPipe[iPrevSize], ptPipePair, iSize * sizeof(PipePair));
	m_iVecPipeUsed -= iSize;
}

} // namespace dbcomm


