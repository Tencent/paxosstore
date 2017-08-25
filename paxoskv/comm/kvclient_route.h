
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef  KVSVR_CLSKVCLIENTROUTE_HPP
#define  KVSVR_CLSKVCLIENTROUTE_HPP
#include <inttypes.h>   
#include <stdint.h>

class clsKvClientRoute
{
	public:
		static int GetVirtualUinByKey(const char *pKey, uint32_t iKeyLen, uint32_t & iUin);

		static int GetDistributeKeyByKey(const char *pKey, uint32_t iKeyLen, uint32_t & iDistributeKey);


		static int GetSvrIDByDistributeKey(uint32_t iKvCount, uint32_t iGroupCount,
				uint32_t iDistributeKey, uint32_t * piMasterSvrID, uint32_t * piSlaveSvrID );

		static int GetCSvrIDByDistributeKey(uint32_t iKvCount, uint32_t iGroupCount,
				 uint32_t iDistributeKey, uint32_t * piCSvrID );

		static int GetDistributeKeyByStrKey(const char *pKey, uint32_t & iDistributeKey);

		static int GetDistributeKeyByUin(uint32_t iUin, uint32_t & iDistributeKey);





		static int GetRoleIndexForKV64(const char * key, uint8_t len); //only work for kv64
		static int GetRoleCountForKV64();
};

#endif
