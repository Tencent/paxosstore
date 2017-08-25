
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UTILS_OBJREUSEPOOL_H_
#define CERTAIN_UTILS_OBJREUSEPOOL_H_

#include <utils/CircleQueue.h>
#include <utils/ObjReusedPool.h>

namespace Certain
{

template<typename T>
class clsObjReusedPool
{
private:
	clsCircleQueue<T *> *m_poObjPool;

	// For test.
	uint64_t m_iNewCount = 0;
	uint64_t m_iDeleteCount = 0;

public:
	clsObjReusedPool(uint32_t iPoolSize)
	{
		assert(iPoolSize > 0);
		m_poObjPool = new clsCircleQueue<T *>(iPoolSize);
	}

	virtual ~clsObjReusedPool()
	{
		delete m_poObjPool, m_poObjPool = NULL;
	}

	virtual void Construct(T *) { }

	virtual void Destruct(T *) { }

	T *NewObjPtr()
	{
		T *poObj;

		if (m_poObjPool->Take(&poObj) != 0)
		{
			OSS::ReportNewPaxosCmd();
			m_iNewCount++;
			poObj = new T;
		}

		Construct(poObj);

		return poObj;
	}

	void FreeObjPtr(T *poObj)
	{
		if (poObj == NULL)
		{
			return;
		}

		Destruct(poObj);

		if (m_poObjPool->Push(poObj) != 0)
		{
			OSS::ReportFreePaxosCmd();
			m_iDeleteCount++;
			delete poObj, poObj = NULL;
		}
	}

	uint32_t Size()
	{
		return m_poObjPool->Size();
	}

	uint64_t GetNewCount()
	{
		return m_iNewCount;
	}

	uint64_t GetDeleteCount()
	{
		return m_iDeleteCount;
	}
};

} // namespace Certain

#endif
