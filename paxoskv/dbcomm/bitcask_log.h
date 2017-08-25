
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

namespace dbcomm {



class clsBitCaskLogReader
{
public:
	clsBitCaskLogReader();
	~clsBitCaskLogReader();

	int OpenFile(const char* sBitCaskLogFile);

	int Read(std::string& sValue);

	int ReadSkipError(std::string& sValue, uint32_t& iOffset);

	uint32_t GetCurrentOffset() const 
	{
		return m_iOffset;
	}


private:
	std::string m_sFileName;
	int m_iFD;
	uint32_t m_iOffset;
};


class clsBitCaskLogBufferIterWriter
{
public:
	clsBitCaskLogBufferIterWriter(const char* sKvLogPath, bool bIsMergeWrite);

	~clsBitCaskLogBufferIterWriter();

	int Init(int iMaxBufSize, uint32_t iFileNo);

	int Write(
			const char* pValue, int iValLen, 
			uint32_t& iFileNo, uint32_t& iOffset);

	int Flush();

	int IterIntoNextFile();


	bool HasPending() const {
		return !m_sWriteBuffer.empty();
	}

private:
	const std::string m_sKvLogPath;
	const bool m_bIsMergeWrite;

	int m_iMaxBufSize;

	uint32_t m_iFileNo;
	uint32_t m_iOffset;
	int m_iFD;

	std::string m_sWriteBuffer;
};


class clsBitCaskIterWriter {

public:
	
	clsBitCaskIterWriter(const char* sKvLogPath);

	~clsBitCaskIterWriter();


	int Init(int iFileNo, uint32_t iOffset);

	
	int Write(
			const char* pValue, int iValLen, 
			int& iFileNo, uint32_t& iOffset);

	int IterIntoNextFile();

	std::string GetFileName(int iFileNo);

private:
	const std::string m_sKvLogPath;

	uint32_t m_iFileNo;
	uint32_t m_iOffset;

	int m_iFD;
};


int ReadRecord(
        const std::string& sFileName, 
        uint32_t iOffset, std::string& value);

} // namespace dbcomm



