
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UTILS_FIXSIZEPOLL_H_
#define CERTAIN_UTILS_FIXSIZEPOLL_H_

#include "utils/Logger.h"

namespace Certain
{

class clsFixSizePool
{
	public:
		clsFixSizePool(int iItemCnt, int iItemSize);
		~clsFixSizePool();
		char * Alloc(int iItemSize, bool bLock);
		void Free(char * pItem, bool bLock);

		int GetAllocCnt(){return m_iAllocCnt;}
		int GetItemCnt(){ return m_iItemCnt;}

		//for test
		char * GetBegin(){return  m_pItem;}
		char * GetEnd(){return  m_pItem + (unsigned long long)m_iItemSize * m_iItemCnt;}

		void PrintStat();

		void StartPrintStatWorker();
		static void * PrintStatWork(void * args);

	private:

		int * GetNext(int iIndex)
		{
			return (int*)(m_pItem + iIndex * (unsigned long long)m_iItemSize); 
		}

	private:
		struct PoolStat
		{
			int item_cnt;
			int item_size;
			uint64_t pool_alloc_cnt;
			uint64_t pool_alloc_size;
			uint64_t os_alloc_cnt;
			uint64_t os_alloc_size;
		};
		
	private:
		PoolStat _stat;
		int m_iItemCnt;
		int m_iItemSize;
		char * m_pItem;
		volatile int m_iHead;
		volatile int m_iAllocCnt;
		pthread_mutex_t m_tMutex;
};

}

#endif
