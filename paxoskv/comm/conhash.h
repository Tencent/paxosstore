
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#define TEST_MODE 0 

#include <stdint.h>


class clsConHash
{
	public:
	clsConHash();
	~clsConHash();
	
	int Init(int iConSect);
	int GetConHashSectByUin(uint32_t iUin, int &iConSect);
	int GetConHashSectByStrKey(const char *sStrKey, int &iConSect);
	int GetConHashSectByKvKey(const char *sKey, int iKeyLen, int &iConSect);
	int GetVirtualUinByKey(const char *pKey, int iKeyLen, uint32_t & iUin);
	int FindConSect(uint32_t iHash);
	int GetSectCount(){return m_iSect;}

	int GetSplitCount();
	int GetSplitNode(int index);
	int GetSplitIndex(const char * key, uint8_t len);

	private:
	int _GetConHashSectByVirtualUin(uint32_t iUin, int &iConSect);
	
	int MAX_NODE;
	int m_iSect;
	//HashSect *m_astHashSect;

	uint32_t * m_aiHashValues;
	uint16_t * m_aiValue2Node;
};

