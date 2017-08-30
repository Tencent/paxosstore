
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <strings.h>

enum{
	COMMON_OK=0,
	COMMON_ERR=-1,
	LOCAL_OUT=-2000,
	REMOTE_OUT=-2001,
	OUTBOUND_ERR=-3000,
	USE_NEW_IP=-4000,

	USE_NEW_SET=-5000,
	NEED_TOKEN=-5001,
	NO_IMMIINFO=-5002,

	HASH_NOT_MATCH=-5100,
	SYNCED=1,

	VERSION_OUT=6000,
	CONCUR_OUT=-6000,
    	TLV_SELECT_ERR = -7000,
	LIMIT_OUT=-60001,
	ROUTE_ERR=-60002,
	RESOURCE_LIMIT=-60003,
	LEVELDB_L0_LIMIT=-60004,
	NET_FLOW_LIMIT=-60005, 

	RECV_TIMEOUT = -8000,
	HANDLE_TIMEOUT = -8001,
	SET_CONCUR_LIMIT = -9000,
};

typedef enum
{
	Role_Min = 0,
	Role_A = 1,
	Role_B = 2,
	Role_C = 3,
	Role_Max = 4
}Role_t;

#define MERGE_FAILED         -1000
#define COLD_DATA_UNACCESS -7000
#define COLD_DATA_BROKEN -7000
#define COLD_DATA_FINAL_FAIL -7001
#define BAD_SNSOBJ_KEY -30001

#define BATCHGET_NO_VALUE    1
#define GET_NO_VALUE    1
#define GET_COLD_LINK    2


#define MACHINE_MIN 0
#define MACHINE_A 1
#define MACHINE_B 2
#define MACHINE_C 3
#define MACHINE_MAX 4

#define ITERATE_TYPE_KEY_ONLY 0x1
#define ITERATE_TYPE_ROLE_A 0x2
#define ITERATE_TYPE_TRANSFER 0x4
#define ITERATE_TYPE_NO_TRANSFER 0x8

#define SWAP_VER(x,y) \
uint32_t  tmp = 0;\
tmp = (x);\
(x) = (y);\
(y) = tmp;\

#define MAXKEYLEN 8
#define HDBVER  0x02

#define MAX_SVR_COUNT_AB_PER_GROUP 6
#define MAX_SVR_COUNT_C_PER_GROUP 6
#define MAX_C_SHARE_GROUP 4 

struct SvrAddr_t
{
	public:
		int iIP;
		uint16_t iPort;

		SvrAddr_t(): iIP(0), iPort(0)
		{
			bzero(_buffer, sizeof(_buffer));
		}
		SvrAddr_t(const SvrAddr_t & inst): iIP(inst.iIP), iPort(inst.iPort)
		{
			bzero(_buffer, sizeof(_buffer));
		}
		SvrAddr_t & operator=(const SvrAddr_t & inst)
		{
			iIP = inst.iIP;
			iPort = inst.iPort;
			return *this;
		}
		bool operator==(const SvrAddr_t & inst) const
		{
			return iIP == inst.iIP && iPort == inst.iPort;
		}
		//for map compare
		bool operator<(const SvrAddr_t & inst) const
		{
			return iIP < inst.iIP;
		}
		//only for print use
		const char * GetIP() const
		{
			uint8_t * bytes = (uint8_t *)&iIP;
			snprintf(_buffer, sizeof(_buffer), "%u.%u.%u.%u", bytes[0], bytes[1], bytes[2], bytes[3]);
			return _buffer;
		}
		//only for print use
		int GetPort() const
		{
			return ntohs(iPort);
		}
	private:
		mutable char _buffer[16];
}__attribute__ ((packed));

struct SvrGroup_t
{
	uint32_t iCountAB;
	uint32_t iCountC;
	uint32_t iBegin;
	uint32_t iEnd;
	uint32_t iGroupCount;
	SvrAddr_t tAddrAB[MAX_SVR_COUNT_AB_PER_GROUP];
	SvrAddr_t tAddrC[MAX_SVR_COUNT_C_PER_GROUP];
}__attribute__ ((packed));

struct SvrGroupList_t
{
	uint32_t iGroupCnt;
	uint32_t iMachineC;
	SvrGroup_t tSvrGroup[MAX_C_SHARE_GROUP];
}__attribute__ ((packed));
//NOTE: iRecordListVersion >= 1 代表新结构，后面是
//iRecordListBufferSize + RecordListBuffer + { cmdsize + type(1)  + base_version + cmdbuffer_size + cmdbuffer }

/*  
 *  data format update
 *  1. nothing
 *  2. BufferHeader_old { iRecordListVersion =  0  -> body = recordlistbuffer }
 *  3. BufferHeader_new { iRecordListVersion >= 1  -> body = tlv }
 *
 *  A.read
 *  B.create  用配置决定创建成新/旧格式  kvsvr_create_new
 *  C.update  用原来的格式，如果有配置，则用新格式 kvsvr_update_new
 *
 *  push:GetCmdInfo -> sync
 *  get:if( sync.mode == new ) return reject
 *  1.全部上线支持读新格式
 *    回写依然是旧格式
 *  2.改配置，写成新格式
 *  3.改配置，启用流水push
 *  4.改配置，校验流水push
 *  5.改配置，接受流水push
 *
*/
#define MAX_UINT32_VALUE 0xffffffff
#define MAX_UINT64_VALUE 0xffffffffffffffffLL


#define MAX_STR_KEY   (1024*10) // 10K

uint32_t GenerateSect(uint64_t iHashKey);
uint32_t GenerateSectKV6(uint64_t iHashKey);


bool is_little_endian(void);
//uint64_t ntohll(uint64_t i);
//uint64_t htonll(uint64_t i);

//unsigned long long ntohll(unsigned long long val)

//unsigned long long htonll(unsigned long long val)

#define  DEFAUTL_UIN  0


int GetCpuCount();

void BindToCpu(int begin, int end);

void BindWorkerCpu();

void BindToLastCpu();


