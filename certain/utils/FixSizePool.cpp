
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "utils/FixSizePool.h"

namespace Certain
{

#define ASSERT(bTrue)  {if(!(bTrue)) abort();}

clsFixSizePool::clsFixSizePool(int iItemCnt, int iItemSize)
{
	ASSERT(iItemSize > int(sizeof(int)));
	ASSERT(iItemCnt > 1);

	bzero(&_stat, sizeof(PoolStat));
	_stat.item_cnt = iItemCnt;
	_stat.item_size = iItemSize;
	
	m_iItemSize = iItemSize;
	m_iItemCnt = iItemCnt;
	m_pItem = (char*)calloc(iItemCnt, iItemSize);
	ASSERT(m_pItem!=NULL);

	for(int i=0; i<iItemCnt-1; i++)
	{
		*GetNext(i) = i+1;
	}
	*GetNext(iItemCnt-1) = -1;
	m_iHead = 0;
	m_iAllocCnt = 0;
	ASSERT(pthread_mutex_init(&m_tMutex, NULL)==0);
}

clsFixSizePool::~clsFixSizePool()
{
	free(m_pItem);
	ASSERT(pthread_mutex_destroy(&m_tMutex)==0);
}

char * clsFixSizePool::Alloc(int iItemSize, bool bLock)
{
	char * tmp = NULL;
	if(iItemSize <= m_iItemSize && m_iHead != -1)
	{
		if(bLock)
		{
			ASSERT(pthread_mutex_lock(&m_tMutex)==0);
		}
		ASSERT(-1<=m_iHead  && m_iHead<m_iItemCnt);

		if(m_iHead != -1)
		{
			tmp = m_pItem + m_iHead * (unsigned long long)m_iItemSize;		
			m_iHead = *GetNext(m_iHead);
			m_iAllocCnt++;

			__sync_fetch_and_add(&_stat.pool_alloc_cnt, 1);
			__sync_fetch_and_add(&_stat.pool_alloc_size, iItemSize);
		}
		else
		{
			tmp = (char*)malloc(iItemSize);

			__sync_fetch_and_add(&_stat.os_alloc_cnt, 1);
			__sync_fetch_and_add(&_stat.os_alloc_size, iItemSize);
		}

		if(bLock)
		{
			ASSERT(pthread_mutex_unlock(&m_tMutex)==0);
		}
	}
	else
	{
		tmp = (char*)malloc(iItemSize);

		__sync_fetch_and_add(&_stat.os_alloc_cnt, 1);
		__sync_fetch_and_add(&_stat.os_alloc_size, iItemSize);
	}

	ASSERT(tmp!=NULL);
	return tmp;
}

void clsFixSizePool::Free(char* pItem, bool bLock)
{
	ASSERT(pItem!=NULL);
	if(pItem>=m_pItem  && pItem< m_pItem + m_iItemCnt * (unsigned long long)m_iItemSize)
	{
		if(bLock)
		{
			ASSERT(pthread_mutex_lock(&m_tMutex)==0);
		}
		ASSERT(-1<=m_iHead  && m_iHead<m_iItemCnt);

		int iIndex = (unsigned long long)(pItem - m_pItem)/m_iItemSize;
		//printf("Free iIndex %d pItem %p m_pItme %p m_iItemSize %d m_iHead %d\n", iIndex, pItem, m_pItem, m_iItemSize, m_iHead);
		*GetNext(iIndex) = m_iHead; 
		m_iHead  = iIndex;
		m_iAllocCnt--;

		if(bLock)
		{
			ASSERT(pthread_mutex_unlock(&m_tMutex)==0);
		}
	}
	else
	{
		free(pItem);
	}
}

void clsFixSizePool::PrintStat()
{
	CertainLogInfo("INFO: %s[%d] PoolStat.item_cnt %d item_size %d pool_alloc_cnt %lu pool_alloc_size %lu os_alloc_cnt %lu os_alloc_size %lu\n", __FILE__, __LINE__,
			_stat.item_cnt, _stat.item_size, _stat.pool_alloc_cnt, _stat.pool_alloc_size,
			_stat.os_alloc_cnt, _stat.os_alloc_size);
}

void * clsFixSizePool::PrintStatWork(void * args)
{
	clsFixSizePool * pool = (clsFixSizePool *)args;
	while(true)
	{
		pool->PrintStat();
		sleep(1);
	}
	return NULL;
}

void clsFixSizePool::StartPrintStatWorker()
{
	pthread_t tid;
	assert(0 == pthread_create(&tid, NULL, PrintStatWork, this));
	printf("StartPrintStatWorker done\n");
}

};


