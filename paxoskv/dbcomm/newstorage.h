
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once 

#include <string>
#include <stdint.h>
#include "stor.h"

typedef struct stor_cursor_s stor_cursor_t;

namespace dbimpl {


typedef struct s_Record Record_t;
typedef struct RecordWithPos RecordWithPos_t;

} // namespace dbimpl

namespace dbcomm {

class HashBaseLock;
class clsLevelDBLogIterWriter;
class clsUniversalBatch;




class NewStorage
{
public:
	NewStorage();
	~NewStorage();

public:
	int Init(
			const char* sPath, 
            const char* sRecyclePath, 
			int iBlockSize, 
            int iMaxDirectIOBufSize, 
            int iAdjustStrategy, 
            int iWaitTime);

	int Add(const dbimpl::Record_t* pstRecord, 
            uint32_t* piFileNo, uint32_t* piOffset);

	int BatchAdd(dbimpl::RecordWithPos_t* pRecords, int iCount);

    int Get(uint32_t iFileNo, uint32_t iOffset, std::string& sRawRecord);
//	int Get(uint32_t iFileNo, 
//            uint32_t iOffset, 
//            dbimpl::Record_t* pstRecord);

//	clsUniversalBatch* GetBatchWriteHandle() { 
//        return m_poBatchWriteHandler; 
//    }

    HashBaseLock* GetHaseBaseLock() {
        return m_poAddBaseLock;
    }

private:
	int AddImpl(
			const dbimpl::Record_t& tRecord, 
			uint32_t& iFileNo, uint32_t& iOffset);

private:
	std::string m_sKvLogPath;

	HashBaseLock* m_poAddBaseLock;
	stor_cursor_t* m_pstMaxWritePos;
	clsLevelDBLogIterWriter* m_poWriter;
	// clsUniversalBatch* m_poBatchWriteHandler;
};

} // namespace dbcomm



