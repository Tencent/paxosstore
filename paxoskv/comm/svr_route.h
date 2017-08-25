
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once
#include <stdint.h>

#include <vector>


struct SvrAddr_t;
struct SvrGroup_t;
struct SvrGroupList_t;

class clsSvrListConfigBase;
class clsSvrRoute
{
	public:
		static int GetSvrAddrByKey4All( 
				clsSvrListConfigBase* poSvrConfig, 
				const char * key, uint8_t len, 
				SvrAddr_t * addrA, SvrAddr_t * addrB, SvrAddr_t * addrC);

		static int GetRoleByKey4All(
				clsSvrListConfigBase* poSvrConfig, const SvrAddr_t& self, 
				const char* key, uint8_t len);

		static int GetRemoteAddrByKey4All(
				clsSvrListConfigBase* poSvrConfig, 
				const SvrAddr_t& self, 
				const char* key, 
				int len, 
				SvrAddr_t& addr);

		static int GetAddrCByKey4All(
				clsSvrListConfigBase* poSvrConfig, 
				const char* key, 
				int len, 
				SvrAddr_t& addr);

		static int CheckRouteByKey4All(
				clsSvrListConfigBase* poSvrConfig, 
				const SvrAddr_t& self, 
				const char* key, 
				int len);

		static int GetSvrSetByKey4All(
				clsSvrListConfigBase* poSvrConfig, 
				const char* sKey, uint8_t cKeyLen);

		static int GetSvrMemberIDByKey4All(
				clsSvrListConfigBase* poSvrConfig, 
				const char* key, uint8_t len, 
				int& iMemberIDA, int& iMemberIDB, int& iMemberIDC);

};

