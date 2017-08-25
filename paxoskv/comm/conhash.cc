
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <algorithm>
#include <sys/time.h>
#include <string.h>
#include "conhash.h"
#include "hash_header.h"

// #include "hash_header.h"

#define MAXKEYLEN (8)

static unsigned int UinHashFunc(uint32_t key)
{
	key += ~(key << 15);
	key ^=  (key >> 10);
	key +=  (key << 3);
	key ^=  (key >> 6);
	key += ~(key << 11);
	key ^=  (key >> 16);
	return key;
}
static uint64_t BKDRHash_uint64(const char *str)
{
	uint64_t seed= 1313; // 31 131 1313 13131 131313 etc..
	uint64_t hash = 0;

	while (*str)
	{
		hash = hash * seed + (*str++);
	}

	return hash;
}

clsConHash::clsConHash()
{
	m_iSect = 0;
	m_aiHashValues = NULL;
	m_aiValue2Node = NULL;
	MAX_NODE = g_vnodes_count / g_groups_count;
}

clsConHash::~clsConHash()
{
	if(m_aiHashValues)
	{
		delete [] m_aiHashValues;
	}
	if(m_aiValue2Node)
	{
		delete [] m_aiValue2Node;
	}

}

int clsConHash::Init(int iConSect)
{
	m_iSect = iConSect;

	m_aiHashValues = new uint32_t[iConSect * MAX_NODE]();
	m_aiValue2Node = new uint16_t[iConSect * MAX_NODE]();
	for(int index = 0, cursor = 0; index < g_vnodes_count; ++index)
	{
		//iConSect -- sect count
		//index -- sect ID, start from 0
		if(g_vnodes_node[index] < iConSect)
		{
			m_aiHashValues[cursor] = g_vnodes_values[index];
			m_aiValue2Node[cursor] = g_vnodes_node[index];
			cursor++;
		}
	}
	return 0;

}


int clsConHash::FindConSect(uint32_t iHash)
{
	int vnodeCount = m_iSect * MAX_NODE;
	if(iHash <= m_aiHashValues[0] || iHash > m_aiHashValues[vnodeCount - 1])
	{
		return m_aiValue2Node[0];
	}

	uint32_t * target = 
        std::lower_bound(m_aiHashValues, m_aiHashValues + m_iSect * MAX_NODE, iHash);
	return m_aiValue2Node[target - m_aiHashValues];
}

int clsConHash::_GetConHashSectByVirtualUin(uint32_t iUin, int &iConSect)
{
	iConSect = FindConSect(iUin);
	return 0;
	
}

int clsConHash::GetConHashSectByUin(uint32_t iUin, int &iConSect)
{
	uint32_t iHash = UinHashFunc(iUin);
	iConSect = FindConSect(iHash);
	return 0;
}

int clsConHash::GetConHashSectByStrKey(const char *sStrKey, int &iConSect)
{
	uint64_t ullKey=BKDRHash_uint64(sStrKey);
	return GetConHashSectByKvKey((char *) &ullKey, sizeof(uint64_t), iConSect);
}

int clsConHash::GetVirtualUinByKey(const char *pKey, int iKeyLen, uint32_t & iUin)
{
	iUin = 0;

	if( iKeyLen > MAXKEYLEN || pKey == NULL )
	{
		return -40000;
	}

	if(iKeyLen == MAXKEYLEN)
	{
		uint32_t int1, int2;
		memcpy(&int1, pKey, sizeof(uint32_t));
		memcpy(&int2, pKey + sizeof(uint32_t), sizeof(uint32_t));
		iUin = (int1 ^ int2);
	}                       
	else if (iKeyLen >= 5)  
	{                       
		
		iUin = (*(uint32_t *)pKey); 
		iUin = UinHashFunc(iUin);
	}                      
	else
	{
		return -40001;
	}   

	return 0;

}

int clsConHash::GetSplitIndex(const char * key, uint8_t len)
{
	uint32_t uin = 0;
	int ret = GetVirtualUinByKey(key, len, uin);
	if(ret != 0)
	{
		return -40002;
	}

	int vnodeCount = m_iSect * MAX_NODE;
	if(uin <= m_aiHashValues[0] || uin > m_aiHashValues[vnodeCount - 1])
	{
		return 0;
	}

	uint32_t * target = 
        std::lower_bound(m_aiHashValues, m_aiHashValues + m_iSect * MAX_NODE, uin);
	return target - m_aiHashValues;
}

int clsConHash::GetSplitCount()
{
	return m_iSect * MAX_NODE;
}

int clsConHash::GetConHashSectByKvKey(const char *sKey, int iKeyLen, int &iConSect)
{
	uint32_t iUin = 0;
	iConSect = -1;
	int iRet = GetVirtualUinByKey(sKey, iKeyLen, iUin);
	if( iRet != 0 )
	{
		return -40002;
	}
	else
	{
		_GetConHashSectByVirtualUin(iUin, iConSect);	
	}
	return 0;
}

