
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <mutex>
#include <vector>
#include <string>
#include <stdint.h>


namespace dbimpl {

struct DirectIOWriteInfo;
struct DirectIOReadInfo;

} // namespace dbimpl


namespace leveldb {

class Slice;

} // namespace leveldb

namespace dbcomm {

int IsLevelDBLogFormat(const char* sFileName);

// comm help function
int ReadMetaInfo(
		const char* sLevelDBLogFile, int iDirectIOBufSize, 
		int& iBlockSize, uint64_t& llBlkSeq, uint32_t& iOffset);

int ReadFileMetaInfo(
		const char* sFileName, int& iBlockSize, uint64_t& llStartBlkSeq);

int WriteFileMetaInfo(
		const char* sFileName, 
		int iBlockSize, uint64_t llStartBlkSeq);

int DeprecateAllDataBlock(const char* sFileName);


// auto iter into next write file
class clsLevelDBLogIterWriter
{
public:
	clsLevelDBLogIterWriter(
			const char* sKvLogPath, 
			const char* sKvRecyclePath, 
			bool bIsMergeWrite);
	~clsLevelDBLogIterWriter();

	int Init(
			int iBlockSize, int iMaxDirectIOBufSize, 
			int iMinTailRecordSize, uint64_t llStartBlkSeq, 
			int iAdjustStrategy, uint32_t iFileNo, uint32_t iOffset);

	int Write(
			const char* pValue, int iValLen, 
			uint32_t& iFileNo, uint32_t& iOffset);

	int WriteNoLock(
			const char* pValue, int iValLen, 
			uint32_t& iFileNo, uint32_t& iOffset);

	int BatchWrite(
			const std::vector<leveldb::Slice>& vecValue, 
			uint32_t& iFileNo, std::vector<uint32_t>& vecOffset);

	int BatchWriteNoLock(
			const std::vector<leveldb::Slice>& vecValue, 
			uint32_t& iFileNo, std::vector<uint32_t>& vecOffset);

	int IterIntoNextFileNoLock();

	uint32_t GetFileNo() 
	{
        std::lock_guard<std::mutex> lock(m_tLogLock);
		return GetFileNoNoLock();
	}

	uint32_t GetFileNoNoLock() const
	{
		return m_iFileNo;
	}

	uint32_t GetCurrentOffset();

	uint32_t GetCurrentOffsetNoLock() const;

	uint64_t GetCurrentBlkSeqNoLock() const;

	std::string GetStatInfo() const;

private:
	const std::string m_sKvLogPath;
	const std::string m_sKvRecyclePath;
	const bool m_bIsMergeWrite;

    std::mutex m_tLogLock;
	uint32_t m_iFileNo;
	dbimpl::DirectIOWriteInfo* m_ptWInfo;
};


class clsLevelDBLogWriter
{
public:
	clsLevelDBLogWriter();
	~clsLevelDBLogWriter();

	int OpenFile(
			const char* sLevelDBLogFile, 
			int iBlockSize, int iMaxDirectIOBufSize, 
			int iMinTailRecordSize, uint64_t llStartBlkSeq, 
			int iAdjustStrategy);

	int Write(const char* pValue, int iValLen, uint32_t& iOffset);

	int BatchWrite(
			const std::vector<leveldb::Slice>& vecValue, 
			std::vector<uint32_t>& vecOffset);

private:
	std::string m_sFileName;
	dbimpl::DirectIOWriteInfo* m_ptWInfo;
};


class clsLevelDBLogBufferIterWriter
{
public:
	clsLevelDBLogBufferIterWriter(
			const char* sKvLogPath, 
			const char* sKvRecyclePath, 
			bool bIsMergeWrite);

	~clsLevelDBLogBufferIterWriter();

	int Init(
			int iBlockSize, int iMaxDirectIOBufSize, 
			int iMinTailRecordSize, uint64_t llStartBlkSeq, 
			int iAdjustStrategy, uint32_t iFileNo, uint32_t iOffset);

	// write => add pValue into buffer
	int Write(
			const char* pValue, int iValLen, 
			uint32_t& iFileNo, uint32_t& iOffset);

	int Flush();

	int IterIntoNextFile();

	uint32_t GetBufferUsedSize() const;
	uint32_t GetBufferUsedBlockSize() const;
	uint64_t GetAccWriteSize() const;

	uint32_t GetFileNo() const;

	uint32_t GetCurrentOffset() const;

	int GetBlockSize() const;

	uint64_t GetCurrentBlkSeq() const;

private:
	const std::string m_sKvLogPath;
	const std::string m_sKvRecyclePath;
	const bool m_bIsMergeWrite;

	uint32_t m_iFileNo;

	dbimpl::DirectIOWriteInfo* m_ptWInfo;
};

// sequen
class clsLevelDBLogReader
{
public:
	clsLevelDBLogReader();
	~clsLevelDBLogReader();

	int OpenFile(
			const char* sLevelDBLogFile, int iMaxDirectIOBufSize);

	int Read(std::string& sValue);

	int ReadSkipError(std::string& sValue, uint32_t& iOffset);

private:
	std::string m_sFileName;
	dbimpl::DirectIOReadInfo* m_ptRInfo;
};

class clsLevelDBLogPReader
{
public:
	clsLevelDBLogPReader();
	~clsLevelDBLogPReader();

	int OpenFile(const char* sLevelDBLogFile, int iMaxDirectIOBufSize);

	int Read(uint32_t iOffset, std::string& sValue, uint32_t& iNextOffset);

	int ReadSkipError(uint32_t iOffset, std::string& sValue, uint32_t& iNextOffset);

private:
	std::string m_sFileName;
	dbimpl::DirectIOReadInfo* m_ptRInfo;
};

class clsLevelDBLogAttachReader
{
public:
	clsLevelDBLogAttachReader();
	~clsLevelDBLogAttachReader();

	// pBegin must be iOffset == 0
	int Attach(const char* pBegin, const char* pEnd, uint32_t iOffset);

	int Read(std::string& sValue, uint32_t& iOffset);
	int ReadSkipError(std::string& sValue, uint32_t& iOffset);

	int GetBlockSize() const;
	uint64_t GetStartBlkSeq() const;

private:
	dbimpl::DirectIOReadInfo* m_ptRInfo;
};

class clsLevelDBLogBufferPReader
{
public:
	clsLevelDBLogBufferPReader(const int iMaxDirectIOBufSize);
	~clsLevelDBLogBufferPReader();

	int OpenFile(const char* sLevelDBLogFile, bool bCheckBlock);

	int CloseFile();

	int Read(uint32_t iOffset, std::string& sValue, uint32_t& iNextOffset);

	int SkipOneLevelDBRecord(uint32_t& iNextOffset);
//	int SkipErrorRecord(uint32_t& iNextOffset);
//	int SkipErrorBlock(uint32_t& iNextOffset);
	
	int GetBlockSize() const;

	void PrintDirectIOBuffer();

private:
	const int m_iMaxDirectIOBufSize;
	char* m_pDirectIOBuffer;

	dbimpl::DirectIOReadInfo* m_ptRInfo;
};


} // namespace dbcomm


