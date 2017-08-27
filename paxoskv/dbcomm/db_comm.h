
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <list>
#include <cstring>


namespace dbcomm {

enum {
    INVALID_FD = -1, 
    INVALID_FILENO = 0, 
};

enum {
    RECORD_COMPRESSE = 2, 
};

int GetFileSize(const char* filename);

int DiskRatio(const char* sPath, int iRatio);

bool IsFileExist(const char *sFileName);

int CalcSleepTime(int iNeedTime);

int CheckAndFixDirPath(const std::string& sDirPath);

uint64_t GetTickMS();

uint32_t GetTickSecond();

std::string ConcatePath(
        const std::string& sPath, const std::string& sTail);

int DumpToFileName(
		int iFileNo, const std::string& sPostFix, 
		std::string& sFileName);

int GetFirstRegularFile(const std::string& sPath, std::string& sFileName);

bool IsAMergeFileNo(int iFileNo);

bool IsAWriteFileNo(int iFileNo);

int ToMergeFileNo(int iFileNo);

int ParseFromDBFileName(
		const std::string& sFileName, int& iFileNo, char& cType);

int DumpToDBFileName(
		const int iFileNo, const char cType, std::string& sFileName);

int ParseFromDBDataFileName(const std::string& sFileName, int& iFileNo);

int DumpToDBDataFileName(const int iFileNo, std::string& sFileName);


int ScanFiles(
    const std::string& sPath,
    const std::list<std::string>& tExcludeFilePattern,
    const std::list<std::string>& tIncludeFilePattern,
    std::list<std::string>& tFileList);

void GatherMergeFilesFromDataPath(
		const std::string& sDataPath, 
		std::vector<int>& tVecMergeFile);

void GatherWriteFilesFromDataPath(
		const std::string& sDataPath, 
		std::vector<int>& vecWriteFile);

// end of TODO

int SafeWrite(
		int iFD, const char* pWriteBuf, int iWriteBufLen);

int SafePWrite(
		int iFD, const char* pWriteBuf, int iWriteBufLen, off_t offset);

int SafePRead(
		int iFD, char* pReadBuf, int iReadBufSize, off_t offset);

int SafeDirectIOPRead(
		int iFD, char* pReadBuf, int iReadBufSize, off_t offset);

int SafeDirectIOPWrite(
		int iFD, const char* pWriteBuf, int iWriteBufLen, off_t offset);

template <typename C, typename T>
inline C* DecodeTemplate(C* pBuffer, T& iValue)
{
	memcpy(&iValue, pBuffer, sizeof(T));
	return pBuffer + sizeof(T);
}

template <typename C, typename T>
inline C* EncodeTemplate(C* pBuffer, T iValue)
{
	memcpy(pBuffer, &iValue, sizeof(T));
	return pBuffer + sizeof(T);
}

template <typename T>
void AppendEncodeTemplate(std::string& sBuffer, T iValue)
{
	sBuffer.resize(sBuffer.size() + sizeof(T));
	char* const pEnd = &sBuffer[0] + sBuffer.size();
	char* pIter = pEnd - sizeof(T);
	pIter = EncodeTemplate(pIter, iValue);
	assert(pEnd == pIter);
}

inline char* Decode64Bit(char* pBuffer, int64_t& iValue)
{
	memcpy(&iValue, pBuffer, sizeof(int64_t));
	return pBuffer + sizeof(int64_t);
}

inline const char* Decode64Bit(const char* pBuffer, int64_t& iValue)
{
	return Decode64Bit(const_cast<char*>(pBuffer), iValue);
}

inline char* Encode64Bit(char* pBuffer, const int64_t iValue)
{
	memcpy(pBuffer, &iValue, sizeof(int64_t));
	return pBuffer + sizeof(int64_t);
}

inline char* Decode32Bit(char* pBuffer, int& iValue)
{
	memcpy(&iValue, pBuffer, sizeof(int));
	return pBuffer + sizeof(int);
}

inline const char* Decode32Bit(const char* pBuffer, int& iValue)
{
	return Decode32Bit(const_cast<char*>(pBuffer), iValue);
}

inline char* Encode32Bit(char* pBuffer, const int iValue)
{
	memcpy(pBuffer, &iValue, sizeof(int));
	return pBuffer + sizeof(int);
}

inline char* DecodeNet32Bit(char* pBuffer, int& iValue)
{
	memcpy(&iValue, pBuffer, sizeof(int));
	iValue = ntohl(iValue);
	return pBuffer + sizeof(int);
}

inline const char* DecodeNet32Bit(const char* pBuffer, int& iValLen)
{
	return DecodeNet32Bit(const_cast<char*>(pBuffer), iValLen);
}

inline char* EncodeNet32Bit(char* pBuffer, int iValue)
{
	iValue = htonl(iValue);
	memcpy(pBuffer, &iValue, sizeof(int));
	return pBuffer + sizeof(int);
}

template <typename IntType, typename FlagIntType>
inline IntType AddFlag(const IntType flags, const FlagIntType add_flag)
{
	return flags | add_flag;
}


template <typename IntType, typename FlagIntType>
inline IntType ClearFlag(const IntType flags, const FlagIntType clear_flag)
{
	return flags & (~clear_flag);
}

template <typename IntType, typename FlagIntType>
inline bool TestFlag(const IntType flags, const FlagIntType some_flag)
{
	return flags & some_flag;
}


// resource manange helper

#define CREATE_HEAP_MEM_MANAGER(type, p) \
	dbcomm::HeapMemManager<type> p##__LINE__(p)

#define CREATE_MALLOC_MEM_MANAGER(p) \
	dbcomm::MallocMemManager p##__LINE__(p)

#define CREATE_FD_MANAGER(fd) dbcomm::clsFDManager fd##__LINE__(fd);

template <typename ObjectType>
class HeapMemManager {
public:
	HeapMemManager(ObjectType*& pObj)
		: m_pObj(pObj)
	{}

	~HeapMemManager()
	{
		delete m_pObj;
		m_pObj = NULL;
	}

	ObjectType* Release()
	{
		ObjectType* pObj = m_pObj;
		m_pObj = NULL;
		return pObj;
	}

private:
	ObjectType*& m_pObj;
};


class clsFDManager {
public:
	clsFDManager(int& fd)
		: m_iFD(fd)
	{ }

	~clsFDManager()
	{
		if (0 <= m_iFD) {
			close(m_iFD);
		}
	}

private:
	int& m_iFD;
};


class MallocMemManager {
public:
	MallocMemManager(char*& pValue)
		: m_pValue(pValue)
	{

	}

	~MallocMemManager()
	{
		free(m_pValue);
		m_pValue = NULL;
	}

private:
	char*& m_pValue;
};



} // namespace dbcomm
