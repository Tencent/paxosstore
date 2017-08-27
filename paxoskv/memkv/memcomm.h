
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stddef.h>
#include <stdint.h>

#define FLAG_DELETE 0x01
#define FLAG_COMPRESSE 0x02

#define HBAK_MARK 0x20
#define CHOSEN_MARK 0x40

#define PENDING 0x10

#define MAX_KEY_LEN 8

#define MAX_MEM_VALUE_LEN (20 * 1024 * 1024)

#define START_FLAG 0x12
#define END_FLAG 0x34

#if defined(SMALL_MEM)
#define BLOCK_SIZE (4 * 1024 * 1024)
#else
#define BLOCK_SIZE (512 * 1024 * 1024)
#endif

#define NEW_IDX_SHM_KEY 0x20160405
#define NEW_DATA_BLOCK_SHM_KEY 0x30160405


namespace paxos {

class PaxosLog;

} // namespace paxos


namespace memkv {
namespace PEER_STATUS {

enum {
    UNKOWN = 0,

    D_UNKOWN = 0,
    D_OUT = 0x01,
    D_PENDING = 0x02,
    D_CHOSEN = 0x03,

    C_UNKOWN = 0,
    C_OUT = 0x10,
    C_PENDING = 0x20,
    C_CHOSEN = 0x30,

    IGNORE = 0xFF,
};

};

void *KVGetShm(int key, size_t size, int flag);
int KVGetShm2(void **pshm, int shmid, size_t size, int flag);
int KVGetShm_NoMemSet(void **pshm, int shmid, size_t size, int flag);
int KVDelShm(int iShmId, size_t iSize, void *pShm);
int KVDelShm(int iShmId);

typedef struct s_MemKey {
    char sKey[8];
    uint8_t cKeyLen;
    uint64_t llLogID;
    uint16_t iBlockID;
    uint32_t iBlockOffset;
} __attribute__((packed)) MemKey_t;


class clsMemBaseVisitor
{
public:
	virtual int OnMemKey( const MemKey_t & tMemKey ) = 0;
	virtual ~clsMemBaseVisitor(){}
};


typedef struct s_NewBasic {
    uint64_t llMaxIndex;
    uint64_t llChosenIndex;
    uint32_t iFileNo;
    uint32_t iOffset;

    uint64_t llReqID;   // chosen info: TODO
    uint32_t iVersion;  // chosen info
    uint8_t cFlag;      // chosen info TAG_TRANS, TAG_COMPRESSE

    uint8_t cState;  // has pending index ?
} __attribute__((packed)) NewBasic_t;

typedef struct s_NewHead {
    uint64_t llLogID;
    NewBasic_t tBasicInfo;
    uint8_t cFlag;  // FLAG_DELETE, FLAG_COMPRESSE
    uint8_t cReserve;
    uint32_t iVisitTime;
    uint32_t iDataLen;
    char pData[0];

} __attribute__((packed)) NewHead_t;

typedef struct s_NewHeadLite
{
	uint64_t llLogID;
	uint8_t cFlag; // FLAG_DELETE, FLAG_COMPRESSE
	uint32_t iFileNo;
	uint32_t iDataLen;
	char pData[0];
} __attribute__ ((packed)) NewHeadLite_t;

class HeadWrapper
{
public:
	explicit HeadWrapper(NewHead_t* pHead);
	explicit HeadWrapper(NewHeadLite_t* pHead);

	void SetBasicInfo(const NewBasic_t& tBasicInfo);
	int GetBasicInfo(NewBasic_t& tBasicInfo) const;

	bool IsNull() const;
	size_t HeadSize() const;
	size_t RecordSize() const;
	bool IsLiteHead() const;
	const void* Ptr() const;
	void* Ptr();

private:
	HeadWrapper(); // for HeadWrapper::NULL

public:
	const static HeadWrapper Null;

	uint64_t* pLogID;
	uint8_t* pFlag;
	uint32_t* pFileNo;
	uint32_t* pDataLen;
	char* pData;

private:
	void* m_pHead;
	bool m_bLiteHead;
};


}  // namespace memkv
