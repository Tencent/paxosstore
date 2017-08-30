
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/param.h>
#include <sys/mount.h>
#if !defined(__APPLE__)
#include <sys/statfs.h>
#endif
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <list>
#include <cassert>
#include "cutils/log_utils.h"
#include "db_comm.h"


namespace {

typedef void (*ScanMethod)(
		const std::string& sSource, 
        char cFileType, std::list<std::string>& tFileList);



void 
ScanDBFilesFromDataPath(
		const std::string& sDataPath, 
        const char cFileType, 
		std::list<std::string>& tFileList)
{
    std::list<std::string> tExcludeFilePattern;
    std::list<std::string> tIncludeFilePattern;
	{
		tIncludeFilePattern.push_back(std::string(".") + cFileType);
	}
	
    dbcomm::ScanFiles(sDataPath, 
            tExcludeFilePattern, tIncludeFilePattern, tFileList);
}



void 
GatherDBFiles(
        const std::string& sSource,
		ScanMethod pfnScan, 
        char cFileType, 
        std::vector<int>& tVecFile)
{
    std::list<std::string> tFileList;
	pfnScan(sSource, cFileType, tFileList);
	for (std::list<std::string>::const_iterator iter = tFileList.begin();
			iter != tFileList.end(); ++iter)
	{
		int iFile = 0;
		int ret = dbcomm::ParseFromDBDataFileName(*iter, iFile);
		if (0 != ret)
		{
			logerr("ParseFromDBDataFileName %s iter %s ret %d", 
                    sSource.c_str(), iter->c_str(), ret);
			continue;
		}

		assert(0 == ret);
		tVecFile.push_back(iFile);
	}
}

#if defined( __LIBCO_RDTSCP__) 
static unsigned long long counter(void)
{
	register uint32_t lo, hi;
	register unsigned long long o;
	__asm__ __volatile__ (
			"rdtscp" : "=a"(lo), "=d"(hi)
			);
	o = hi;
	o <<= 32;
	return (o | lo);

}
static unsigned long long getCpuKhz()
{
	FILE *fp = fopen("/proc/cpuinfo","r");
	if(!fp) return 1;
	char buf[4096] = {0};
	fread(buf,1,sizeof(buf),fp);
	fclose(fp);

	char *lp = strstr(buf,"cpu MHz");
	if(!lp) return 1;
	lp += strlen("cpu MHz");
	while(*lp == ' ' || *lp == '\t' || *lp == ':')
	{
		++lp;
	}

	double mhz = atof(lp);
	unsigned long long u = (unsigned long long)(mhz * 1000);
	return u;
}
#endif



} // namespace 

namespace dbcomm {


int GetFileSize(const char* filename) 
{
    struct stat filestat;
    if (0 != stat(filename, &filestat)) {
        return -1;
    }

    return filestat.st_size;
}

int DiskRatio(const char* sPath, int iRatio)
{
	int ret = 0;
	struct statfs stStatfs;
	ret = statfs(sPath, &stStatfs);
	if (ret < 0) {
		logerr("statfs(%s) %s\n", sPath, strerror(errno));
		return 0;
	}

	if ((stStatfs.f_blocks - stStatfs.f_bfree) * 100 > 
            stStatfs.f_blocks * iRatio) { 
        return 1;
    }
	if ((stStatfs.f_blocks - stStatfs.f_bfree) * 100 == 
            stStatfs.f_blocks * iRatio) { 
        return 0;
    }

	if ((stStatfs.f_blocks - stStatfs.f_bfree) * 100 < 
            stStatfs.f_blocks * iRatio) { 
        return -1;
    }

	return 0;
}

bool IsFileExist(const char *sFileName)
{
	int fd = open(sFileName, O_RDONLY);
	if (fd > 0) { 
		close(fd);
		return true;
	}
	//No such file or directory
	if (fd < 0 && errno == 2)
		return false;
	if (fd < 0) {
		logerr("open(%s) %s", sFileName, strerror(errno));
    }

	return false;
}

int CalcSleepTime(int iNeedTime)
{
	time_t iCurTime = 0;
	time_t iTmpTime = 0;
	int iSleepTime = 0;
	int iHour = iNeedTime;
	struct tm stTm;
	iCurTime = time(NULL);
	localtime_r(&iCurTime, &stTm);
	if (stTm.tm_hour < iHour) {
		stTm.tm_hour = iHour;
		stTm.tm_min = 0;
		stTm.tm_sec  = 0;
		iSleepTime = mktime(&stTm) - iCurTime;
	} 
    else if (stTm.tm_hour > iHour) {
		iTmpTime = iCurTime + (3600 * 24);
		localtime_r(&iTmpTime, &stTm);
		stTm.tm_hour = iHour;
		stTm.tm_min = 0;
		stTm.tm_sec  = 0;
		iSleepTime = mktime(&stTm) - iCurTime;
	} 
    else if (stTm.tm_hour == iHour) {
		iSleepTime = 0;
	}

	return iSleepTime;
}


int CheckAndFixDirPath(const std::string& sDirPath)
{
	if (0 == sDirPath.size())
	{
		return -1;
	}

	if (0 > access(sDirPath.c_str(), F_OK))
	{
		// 1. non-exist => mkdir
		int ret = mkdir(sDirPath.c_str(), 0755);
		return 0 == ret ? 0 : -2;
	}

	// sDirPath exist, check whether is dir
	struct stat info = { 0 };
	int ret = stat(sDirPath.c_str(), &info);
	if (0 != ret)
	{
		return -3;
	}

	if (!S_ISDIR(info.st_mode))
	{
		unlink(sDirPath.c_str());
		if (0 > mkdir(sDirPath.c_str(), 0755))
		{
			return -4;
		}
	}
	return 0;
}



uint64_t GetTickMS() 
{
#if defined( __LIBCO_RDTSCP__) 
	static uint32_t khz = getCpuKhz();
	return counter() / khz;
#else
	struct timeval now = { 0 };
	gettimeofday( &now,NULL );
	unsigned long long u = now.tv_sec;
	u *= 1000;
	u += now.tv_usec / 1000;
	return u;
#endif
}

uint32_t GetTickSecond()
{
    return GetTickMS() / 1000;
}


std::string ConcatePath(
        const std::string& sPath, const std::string& sTail)
{
    if ('/' == sPath[sPath.size()-1])
    {
        return sPath + sTail;
    }
    return sPath + "/" + sTail;
}

bool IsAMergeFileNo(const int iFileNo)
{
	return iFileNo < INVALID_FILENO ? true : false;
}

bool IsAWriteFileNo(const int iFileNo)
{
	return iFileNo > INVALID_FILENO ? true : false;
}

int ToMergeFileNo(const int iFileNo)
{
	return iFileNo < INVALID_FILENO ? iFileNo : -iFileNo;
}

int DumpToFileName(
		const int iFileNo, 
        const std::string& sPostFix, std::string& sFileName)
{
	char buf[32] = { 0 };		
	int len = snprintf(buf, sizeof(buf), "%d", iFileNo);
	if (0 >= len)
	{
		return -1;
	}
	sFileName = std::string(buf, len) + sPostFix;
	return 0;
}

int DumpToDBFileName(
		const int iFileNo, const char cType, std::string& sFileName)
{
	char buf[32] = { 0 };
	int len = snprintf(buf, sizeof(buf), "%d.%c", iFileNo, cType);
	if (0 >= len)
	{
		return -1;
	}
	sFileName = std::string(buf, len);
	return 0;
}

int ParseFromDBFileName(
        const std::string& sFileName, int& iFileNo, char& cType)
{
	char buf[3] = { 0 };
	int ret = sscanf(sFileName.c_str(), "%d.%s", &iFileNo, buf);	
	if (2 != ret)
	{
		return -1; // invalid filename;
	}

	if ('\0' != buf[1])
	{
		return -2; // too many postfix
	}

	cType = buf[0];
	return 'w' == cType || 'm' == cType || 'h' == cType ? 0 : -2; // invalid type
}


int ParseFromDBDataFileName(
        const std::string& sFileName, int& iFileNo)
{
	char cType = 0;
	int iRawFileNo = INVALID_FILENO;
	int ret = ParseFromDBFileName(sFileName, iRawFileNo, cType);
	if (0 != ret)
	{
		return -1;
	}

	if ('m' == cType)
	{
		iFileNo = -iRawFileNo;
		return 0;
	}
	else if ('w' == cType)
	{
		iFileNo = iRawFileNo;
		return 0;
	}
	return -2;
}

int DumpToDBDataFileName(
        const int iFileNo, std::string& sFileName)
{
	// iFileNo > 0 => *.w
	// iFileNo < 0 => *.m
	char cType = iFileNo >= 0 ? 'w' : 'm';
	return DumpToDBFileName(abs(iFileNo), cType, sFileName);
}

int ScanFiles(
    const std::string& sPath,
    const std::list<std::string>& tExcludeFilePattern,
    const std::list<std::string>& tIncludeFilePattern,
    std::list<std::string>& tFileList)
{
    DIR* pDir = NULL;
    struct dirent* pDirEnt = NULL;
    if (NULL == (pDir = opendir(sPath.c_str())))
    {
        return -1;
    }

    while (NULL != (pDirEnt = readdir(pDir)))
    {
        assert(NULL != pDirEnt);
        const char* sFileName = pDirEnt->d_name;
		
		bool bMatchExclude = false;
        for (std::list<std::string>::const_iterator iter
                = tExcludeFilePattern.begin();
                iter != tExcludeFilePattern.end(); ++iter)
        {
            if (strstr(sFileName, iter->c_str()))
            {
				bMatchExclude = true;
				break;
            }
        }

		if (bMatchExclude)
		{
			continue;
		}

        for (std::list<std::string>::const_iterator iter
                = tIncludeFilePattern.begin();
                iter != tIncludeFilePattern.end(); ++iter)
        {
            if (strstr(sFileName, iter->c_str()))
            {
				tFileList.push_back(sFileName);
				break;
            }
        }
    }

    closedir(pDir);
    return 0;
}

int GetFirstRegularFile(const std::string& sPath, std::string& sFileName)
{
	DIR* pDir = NULL;
	struct dirent* pDirEnt = NULL;
	if (NULL == (pDir = opendir(sPath.c_str())))
	{
		return -1;
	}

	while (NULL != (pDirEnt = readdir(pDir)))
	{
		assert(NULL != pDirEnt);
		if (DT_REG == pDirEnt->d_type)
		{	
			sFileName = pDirEnt->d_name;
			closedir(pDir);
			return 0;
		}
		// else => iter to next one;
	}

	closedir(pDir);
	return 1; // indicate empty dir
}

void GatherMergeFilesFromDataPath(
		const std::string& sDataPath, 
		std::vector<int>& tVecMergeFile)
{
	GatherDBFiles(
            sDataPath, ScanDBFilesFromDataPath, 'm', tVecMergeFile);
}

void GatherWriteFilesFromDataPath(
		const std::string& sDataPath, 
		std::vector<int>& vecWriteFile)
{
	GatherDBFiles(
            sDataPath, ScanDBFilesFromDataPath, 'w', vecWriteFile);
}





int SafeWrite(
		int iFD, const char* pWriteBuf, int iWriteBufLen)
{
	assert(0 <= iFD);
	int iWriteSize = 0;
	while (iWriteSize < iWriteBufLen)
	{
		int iSmallWriteSize = write(
				iFD, pWriteBuf + iWriteSize, iWriteBufLen - iWriteSize);
		if (0 >= iSmallWriteSize)
		{
			if (0 == iSmallWriteSize)
			{
				break;
			}

			return iSmallWriteSize;
		}

		iWriteSize += iSmallWriteSize;
	}

	return iWriteSize;
}

int SafePWrite(
		int iFD, const char* pWriteBuf, int iWriteBufLen, off_t offset)
{
	assert(0 <= iFD);
	int iWriteSize = 0;
	while (iWriteSize < iWriteBufLen)
	{
		int iSmallWriteSize = pwrite(
				iFD, pWriteBuf + iWriteSize, 
				iWriteBufLen - iWriteSize, offset + iWriteSize);
		if (0 >= iSmallWriteSize)
		{
			if (0 == iSmallWriteSize)
			{
				break;
			}

			return iSmallWriteSize;
		}

		iWriteSize += iSmallWriteSize;
	}

	return iWriteSize;
}

int SafePRead(
		int iFD, char* pReadBuf, int iReadBufSize, off_t offset)
{
	assert(0 <= iFD);
	int iReadSize = 0;
	while (iReadSize < iReadBufSize)
	{
		int iSmallReadSize = pread(
				iFD, pReadBuf + iReadSize, 
				iReadBufSize - iReadSize, offset + iReadSize);
		if (0 >= iSmallReadSize)
		{
			if (0 == iSmallReadSize)
			{
				break;
			}

			return iSmallReadSize;
		}

		iReadSize += iSmallReadSize;
	}

	return iReadSize;
}


int SafeDirectIOPRead(
		int iFD, char* pReadBuf, int iReadBufSize, off_t offset)
{
	assert(0 <= iFD);
	// it's not safe that read by while loop pread:
	//    => prev read block may not be fresh in read/write chasing mode;
	return pread(iFD, pReadBuf, iReadBufSize, offset);
}

int SafeDirectIOPWrite(
		int iFD, const char* pWriteBuf, int iWriteBufLen, off_t offset)
{
	return SafePWrite(iFD, pWriteBuf, iWriteBufLen, offset);
}



} // namespace dbcomm

