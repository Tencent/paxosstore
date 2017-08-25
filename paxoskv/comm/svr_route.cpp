
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <stdio.h>
#include <string.h>
#include <memory>
#include <cassert>
#include <netinet/in.h> 
#include "cutils/log_utils.h"
#include "kvclient_route.h"
#include "svrlist_config_base.h"
#include "svr_route.h"
#include "conhash.h"
#include "kvsvrcomm.h"

using namespace Comm;

int clsSvrRoute::GetSvrAddrByKey4All(
        clsSvrListConfigBase * poSvrConfig, 
        const char * key, uint8_t len, 
        SvrAddr_t * addrA, SvrAddr_t * addrB, SvrAddr_t * addrC)
{
    if (nullptr == poSvrConfig) {
        return -1;
    }

	uint32_t sect = 0;
	int ret = clsKvClientRoute::GetDistributeKeyByKey( key, len, sect );
	if(ret < 0)
	{
		logerr("ERR: %lx %d ret %d\n", *(uint64_t*)key, len, ret);
		return -40020;
	}

	uint32_t iTmpSect = sect;
	if (poSvrConfig->GetConsistentHash())	
	{
		int se = -1;
		ret = poSvrConfig->GetConHash()->GetConHashSectByKvKey( key, len, se);
		if( ret ) return -1;

		iTmpSect = se;
	}

	SvrGroup_t * group = poSvrConfig->GetBySect( iTmpSect );
	if( addrA || addrB )
	{

		uint32_t iMasterSvrID = 0, iSlaveSvrID = 0;
		ret = clsKvClientRoute::GetSvrIDByDistributeKey( group->iCountAB, group->iGroupCount, sect, 
															&iMasterSvrID, &iSlaveSvrID);
		if( ret < 0 )
		{
			logerr("ERR clsKvClientRoute::GetSvrIDByDistributeKey "
                    "kvcount %u groupcount %u ret %d\n",
				   	group->iCountAB, group->iGroupCount, ret);
			return -40022;
		}
		if( addrA )
		{
			memcpy( addrA, &group->tAddrAB[iMasterSvrID - 1], sizeof(SvrAddr_t) );
		}
		if( addrB )
		{
			memcpy( addrB, &group->tAddrAB[iSlaveSvrID - 1], sizeof(SvrAddr_t) );
		}
	}
	if( addrC )
	{
		if( 6 == group->iCountAB && 6 == group->iGroupCount )
		{
			//*addrC = group.tAddrAB[sect % group.iCountC];
			memcpy(addrC, &group->tAddrC[sect % group->iCountC], sizeof(SvrAddr_t));
			return 0;
		}

		uint32_t iSvrID = 0;
		ret = clsKvClientRoute::GetCSvrIDByDistributeKey(group->iCountC, group->iGroupCount, sect, &iSvrID);
		if( ret < 0 )
		{
			logerr("ERR: clsKvClientRoute::GetCSvrIDByDistributeKey "
                    "ret %d\n", ret);
			return -42026;
		}

		memcpy( addrC, &group->tAddrC[iSvrID -1], sizeof(SvrAddr_t) );
	}

	return 0;
}

int clsSvrRoute::GetRoleByKey4All(
		clsSvrListConfigBase* poSvrConfig, const SvrAddr_t& self, 
		const char* key, uint8_t len)
{
	SvrAddr_t addrA, addrB, addrC;
	int ret = GetSvrAddrByKey4All(
            poSvrConfig, key, len, &addrA, &addrB, &addrC);
	if (0 != ret)
	{
		return ret;
	}

	if (self == addrA)
	{
		return MACHINE_A;
	}
	else if (self == addrB)
	{
		return MACHINE_B;
	}
	else if (self == addrC)
	{
		return MACHINE_C;
	}

	return -42010;
}


int clsSvrRoute::GetRemoteAddrByKey4All(
		clsSvrListConfigBase* poSvrConfig, 
		const SvrAddr_t& self, 
		const char* key, 
		int len, 
		SvrAddr_t& addr)
{
	SvrAddr_t arrAddr[2];
	int ret = GetSvrAddrByKey4All(
			poSvrConfig, key, len, &arrAddr[0], &arrAddr[1], NULL);
	if (0 != ret)
	{
		return ret;
	}

	if (self == arrAddr[0]) // A
	{
		addr = arrAddr[1];
	}
	else if (self == arrAddr[1]) // B
	{
		addr = arrAddr[0];
	}
	else
	{
		return -40025;
	}

	return 0;
}

int clsSvrRoute::GetAddrCByKey4All(
		clsSvrListConfigBase* poSvrConfig, 
		const char* key, 
		int len, 
		SvrAddr_t& addr)
{
	return GetSvrAddrByKey4All(poSvrConfig, key, len, NULL, NULL, &addr);
}

int clsSvrRoute::CheckRouteByKey4All(
		clsSvrListConfigBase* poSvrConfig, 
		const SvrAddr_t& self, 
		const char* key, 
		int len)
{
	SvrAddr_t arrAddr[3];
	int ret = GetSvrAddrByKey4All(
			poSvrConfig, key, len, &arrAddr[0], &arrAddr[1], &arrAddr[2]);
	if (0 != ret)
	{
		return ret;
	}

	for (size_t idx = 0; idx < 3; ++idx)
	{
		if (self.iIP == arrAddr[idx].iIP)
		{
			return 0;
		}
	}

	logerr("ERR: CheckRouteByKey4All failed");
	return -40029;
}

int clsSvrRoute::GetSvrSetByKey4All(
		clsSvrListConfigBase* poSvrConfig, 
		const char* sKey, uint8_t cKeyLen)
{
    if (nullptr == poSvrConfig) {
		return -1;
	}

	if (!poSvrConfig->GetConsistentHash())
	{
		return -2;
	}

	int iConSect = -1;
	int ret = poSvrConfig->GetConHash(
            )->GetConHashSectByKvKey(sKey, cKeyLen, iConSect);
	if (0 != ret)
	{
		return -3;
	}

	ret = poSvrConfig->GetSvrSetIdx(iConSect);
	if (0 > ret)
	{
		return -4;
	}

	return ret;
}

int clsSvrRoute::GetSvrMemberIDByKey4All(
		clsSvrListConfigBase* poSvrConfig, 
		const char* key, uint8_t len, 
		int& iMemberIDA, int& iMemberIDB, int& iMemberIDC)
{
    if (nullptr == poSvrConfig) {
        return -1;
    }

	uint32_t sect = 0;
	int ret = clsKvClientRoute::GetDistributeKeyByKey(key, len, sect);
	if (0 > ret) {
		logerr("ERR: key %lx %d GetDistributeKeyByKey ret %d", 
                *(uint64_t*)key, len, ret);
		return -40020;
	}

	uint32_t iTmpSect = sect;

	if (poSvrConfig->GetConsistentHash()) {
		int se = -1;
		ret = poSvrConfig->GetConHash()->GetConHashSectByKvKey(key, len, se);
		if (0 != ret) {
			logerr("ERR: GetConHashSectByKvKey ret %d", ret);
			return -1;
		}

		iTmpSect = se;
	}


	SvrGroup_t* group = poSvrConfig->GetBySect(iTmpSect);
	if (NULL == group) {
		logerr("ERR: GetBySect iTmpSect %u ret %d", iTmpSect, ret);
		return -1;
	}

	assert(NULL != group);
	uint32_t iMasterSvrID = 0;
	uint32_t iSlaveSvrID = 0;
	ret = clsKvClientRoute::GetSvrIDByDistributeKey(
			group->iCountAB, group->iGroupCount, sect, &iMasterSvrID, &iSlaveSvrID);
	if (0 > ret) {
		logerr("ERR: GetSvrIDByDistributeKey kvcount %u groupcount %u ret %d", 
				group->iCountAB, group->iGroupCount, ret);
		return -40022;
	}

	iMemberIDA = 0;
	iMemberIDB = 0;
	iMemberIDC = 0;

	iMemberIDA = iMasterSvrID;
	iMemberIDB = iSlaveSvrID;
	if (6 == group->iCountAB && 6 == group->iGroupCount) {
		iMemberIDC = sect % group->iCountC + 1;
		return 0;
	}

	// else;
	uint32_t iSvrID = 0;
	ret = clsKvClientRoute::GetCSvrIDByDistributeKey(
			group->iCountC, group->iGroupCount, sect, &iSvrID);
	if (0 > ret) {
		logerr("ERR: GetCSvrIDByDistributeKey ret %d", ret);
		return -42026;
	}

	iMemberIDC = iSvrID;
	return 0;
}


