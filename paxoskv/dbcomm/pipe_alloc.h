
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <vector>

namespace dbcomm {

class clsPipeAllocator
{
public:
	struct PipePair 
	{
		int iPipe[2];
	};

	clsPipeAllocator(size_t iPrevAllocSize, size_t iReserveSize);
	~clsPipeAllocator();

	bool AllocPipe(PipePair& tPipePair);
	void FreePipe(PipePair* ptPipePair, size_t iSize);

	size_t GetPipeUsed() const {
		return m_iVecPipeUsed;
	}

	size_t GetPipeSize() const {
		return m_vecPipe.size();
	}

	const std::vector<PipePair>& GetPipeVector() const {
		return m_vecPipe;
	}

private:
	size_t m_iVecPipeUsed;
	std::vector<PipePair> m_vecPipe;
};


} // namespace dbcomm
