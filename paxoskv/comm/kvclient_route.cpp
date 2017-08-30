
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cstring>

#include "cutils/hash_utils.h"
#include "kvclient_route.h"
#include "kvsvrcomm.h"


int clsKvClientRoute::GetVirtualUinByKey(const char *pKey, uint32_t iKeyLen, uint32_t & iUin)
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
	 }
	 else
	 {
		 return -40001;
	 }

	 return 0;

}


int clsKvClientRoute::GetDistributeKeyByKey(const char *pKey, uint32_t iKeyLen, uint32_t & iDistributeKey)
{
	uint32_t iUin = 0;
	int iRet = GetVirtualUinByKey(pKey, iKeyLen, iUin);
	if( iRet != 0 )
	{
		return -40002;
	}
	else
	{
		iDistributeKey = iUin / 10000;
	}

	return 0;
}

int clsKvClientRoute::GetSvrIDByDistributeKey(uint32_t iKvCount, uint32_t iGroupCount,
		uint32_t iDistributeKey, uint32_t * piMasterSvrID, uint32_t * piSlaveSvrID )
{

	 if( iGroupCount > iKvCount
		 || iGroupCount < 2 
		 || iKvCount <= 0  
		 || iGroupCount <= 0
		 || iKvCount % iGroupCount != 0 )
	 {
		 return -40003;
	 }

	 uint32_t iKvBakCount = iKvCount  - (iKvCount / iGroupCount);
	 uint32_t iBaseSect = iKvCount * iKvBakCount;
	 uint32_t iOffSet = (iDistributeKey % iBaseSect) / iKvCount;

	 uint32_t iMasterSvrID = iDistributeKey % iKvCount;

	 uint32_t iSlaveSvrID = ( 
			 				 iMasterSvrID
			 			  	 + iOffSet 
							 + iOffSet / (iGroupCount - 1) + 1 
							) % iKvCount; 

	 if(piMasterSvrID)
	 {
		 *piMasterSvrID = iMasterSvrID + 1; //svrid from 1 to iKvCount
	 }

	 if(piSlaveSvrID)
	 {
		 *piSlaveSvrID = iSlaveSvrID + 1;
	 }

	 return 0;
}

int clsKvClientRoute::GetCSvrIDByDistributeKey(uint32_t iKvCount, uint32_t iGroupCount,
		uint32_t iDistributeKey, uint32_t * piCSvrID )
{
	 if( iGroupCount > iKvCount 
		 || iGroupCount < 2 
		 || iKvCount <= 0  
		 || iGroupCount <= 0
		 || iKvCount % iGroupCount != 0 )
	 {
		 return -40003;
	 }

	 uint32_t iKvBakCount = iKvCount  - (iKvCount / iGroupCount);
	 uint32_t iBaseSect = iKvCount * iKvBakCount;
	 uint32_t iOffSet = (iDistributeKey % iBaseSect) / iKvCount;

	 uint32_t iMasterSvrID = iDistributeKey % iKvCount;

	 uint32_t iCSvrID = ( 
			 				 iMasterSvrID
			 			  	 + iOffSet + 2 
							 + (iOffSet + 1) 
							 / (iGroupCount - 1)  
							) % iKvCount; 

	 if(piCSvrID)
	 {
		 *piCSvrID = iCSvrID + 1; //svrid from 1 to iKvCount
	 }
	 
	 return 0;
}

int clsKvClientRoute::GetDistributeKeyByStrKey(const char *pKey, uint32_t & iDistributeKey)
{
	int iRet=0;

	uint64_t ullKey = cutils::bkdr_64hash(pKey);

	iRet=GetDistributeKeyByKey( (char *) &ullKey, sizeof(uint64_t), iDistributeKey);

	return iRet;
}

int clsKvClientRoute::GetDistributeKeyByUin(uint32_t iUin, uint32_t & iDistributeKey)
{
	iDistributeKey=iUin/10000;
	return 0;
}

static const int role_index[6][6] = 
{
	{-1,  0,  1, -1,  2,  3},
	
	{ 4, -1,  5,  6, -1,  7},

	{ 8,  9, -1, 10, 11, -1},

	{-1, 12, 13, -1, 14, 15},

	{16, -1, 17, 18, -1, 19},

	{20, 21, -1, 22, 23, -1}
};


int clsKvClientRoute::GetRoleIndexForKV64(const char * key, uint8_t len)
{
	uint32_t sect = 0;
	int ret = GetDistributeKeyByKey(key, len, sect);
	if(ret < 0)
	{
		return ret;
	}

	uint32_t masterID = 0, slaveID = 0;
	ret = GetSvrIDByDistributeKey(6, 3, sect, &masterID, &slaveID);
	if(ret < 0)
	{
		return ret;
	}

	return role_index[masterID - 1][slaveID - 1];
}

int clsKvClientRoute::GetRoleCountForKV64()
{
	return 24;
}

