
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <sys/syscall.h>
//#include <linux/ioprio.h>
#include <algorithm>
#include <unordered_set>
#include <sstream>
#include <cstdlib>
#include <cstdio>
#include "dbcomm/hashlock.h"
#include "dbcomm/stor.h"
#include "dbcomm/leveldb_log_impl.h"
#include "dbcomm/leveldb_log.h"
#include "dbcomm/bitcask_log_impl.h"
#include "dbcomm/bitcask_log.h"
#include "dbcomm/db_compresse.h"
#include "dbcomm/db_comm.h"
#include "comm/kvsvrcomm.h"
#include "cutils/log_utils.h"
#include "cutils/hash_utils.h"
#include "pmergetor.h"
#include "memkv.h"
#include "memcomm.h"
#include "mem_compressor.h"
#include "merge_comm.h"




static __thread char m_szMergeName[ 1024 ];

#define DEFAULT_UNLINK_SLEEP_TIME 30

using dbimpl::Record_t;

namespace {

using namespace dbcomm;
using namespace memkv;


void SplitVector(
		const std::vector<int> &a, 
        size_t size, 
        std::vector< std::vector<int> > & result)
{
	result.clear();
	for( size_t i=0;i<a.size();i += size )
	{
        std::vector<int> v;
		for( size_t j=0;j + i <a.size() && j < size;j++ )
		{
			v.push_back( a[ j + i ] );
		}

		result.push_back( v );
	}
}

std::string GetMergeFileName(const char* sPath, uint32_t iFileNo)
{
    std::string sFileName;
	assert(0 < static_cast<int>(iFileNo));
	int ret = dbcomm::DumpToDBDataFileName(
			-(static_cast<int>(iFileNo)), sFileName);
	assert(0 == ret);
	return dbcomm::ConcatePath(sPath, sFileName);
}

std::string GetFileName(const char* sPath, int iFileNo)
{
	assert(0 != iFileNo);
    std::string sFileName;
	int ret = dbcomm::DumpToDBDataFileName(iFileNo, sFileName);
	assert(0 == ret);
	return dbcomm::ConcatePath(sPath, sFileName);
}

int ReadMergeMetaInfo(
		const std::string& sKvPath, 
		const int iBlockSize, 
		uint32_t& iFileNo, uint32_t& iOffset, uint64_t& llBlkSeq)
{
    std::vector<int> vecMergeFile;
	dbcomm::GatherMergeFilesFromDataPath(sKvPath.c_str(), vecMergeFile);
	if (vecMergeFile.empty())
	{
		// init metainfo for merge files
		iFileNo = 1;
		iOffset = 0;
		llBlkSeq = 1;
		return 0;
	}

	// else
    std::sort(vecMergeFile.begin(), vecMergeFile.end());
	assert(0 > vecMergeFile.front());
	logerr("INFO vecMergeFile.size %lu front %d back %d", 
			vecMergeFile.size(), vecMergeFile.front(), vecMergeFile.back());
	iFileNo = abs(vecMergeFile.front());	
	assert(0 != iFileNo);
    std::string sCurrentFile = GetMergeFileName(sKvPath.c_str(), iFileNo);
	int ret = dbcomm::IsLevelDBLogFormat(sCurrentFile.c_str());
	if (0 >= ret)
	{
		logerr("IsLevelDBLogFormat %s ret %d", sCurrentFile.c_str(), ret);
		if (0 > ret)
		{
			return ret;
		}

		iOffset = 0;
		llBlkSeq = 1;
		// check is empty ?
		int iFileSize = dbcomm::GetFileSize(sCurrentFile.c_str());
		if (0 != iFileSize)
		{
			++iFileNo;
		}

		return 0; // done
	}

	assert(1 == ret); // leveldb log format;
	int iFileBlockSize = 0;
	uint64_t llFileBlkSeq = 0;
	ret = ReadMetaInfo(
			sCurrentFile.c_str(), 
            1024 * iBlockSize, iFileBlockSize, llFileBlkSeq, iOffset);
	logerr("INFO: ReadMetaInfo %s ret %d iFileBlockSize %d "
			"llFileBlkSeq %lu iOffset %u", sCurrentFile.c_str(), ret, 
			iFileBlockSize, llFileBlkSeq, iOffset);
	if (0 != ret)
	{
		assert(0 > ret);
		return ret;
	}

	assert(0 == ret); // success
	llBlkSeq = llFileBlkSeq + 1;
	if (0 == llFileBlkSeq || iFileBlockSize != iBlockSize)
	{
		// shift
		++iFileNo;
		iOffset = 0;
	}

	logerr("INFO: iFileBlockSize %d llFileBlkSeq %lu iOffset %u", 
			iFileBlockSize, llFileBlkSeq, iOffset);
	return 0;	
}

void SafeSleepMS(int64_t llSleepTime) // ms
{
	uint64_t llBeginTime = dbcomm::GetTickMS();
	uint64_t llNow = llBeginTime;
	while (static_cast<int64_t>(llNow - llBeginTime) < llSleepTime)
	{
		assert(0 < llBeginTime + llSleepTime - llNow);
		usleep((llBeginTime + llSleepTime - llNow) * 1000);
		llNow = dbcomm::GetTickMS();
	}
	logdebug("llNow %lu llBeginTime %lu llSleepTime %lu", 
			llNow, llBeginTime, llSleepTime);
}

void SafeSleep(uint32_t iSleepSecond)
{
	uint32_t iBeginTime = dbcomm::GetTickSecond();
	uint32_t iNow = iBeginTime;
	while (iNow - iBeginTime < iSleepSecond)
	{
		assert(0 < iBeginTime + iSleepSecond - iNow);
		sleep(iBeginTime + iSleepSecond - iNow);
		iNow = dbcomm::GetTickSecond();
	}
	logdebug("sleep iBeginTime %u iNow %u iSleepSecond %d", 
			iBeginTime, iNow, iSleepSecond);
}

void UnlinkAndSleep(const std::string& sKvDBFile, int iSleepSecond)
{
	int ret = unlink(sKvDBFile.c_str());
	logerr("unlink %s iSleepSecond %d ret %d %s", 
			sKvDBFile.c_str(), iSleepSecond, ret, strerror(errno));
	SafeSleep(iSleepSecond);
}

int UnlinkInUrgentMode(const std::string& sKvPath, const int iFileNo)
{
	std::string sDataFileName;
	int ret = dbcomm::DumpToDBDataFileName(iFileNo, sDataFileName);
	if (0 != ret) {
		logerr("DumpToDBDataFileName iFileNo %d ret %d", iFileNo, ret);
		return -1;
	}

	sDataFileName = dbcomm::ConcatePath(sKvPath, sDataFileName);
	ret = unlink(sDataFileName.c_str());
	logerr("URGENT %s unlink ret %d", sDataFileName.c_str(), ret);
	return ret;
}


int MoveFileIntoRecycleDir(
		const std::string& sKvPath, 
		const std::string& sKvRecyclePath, const int iFileNo)
{
    std::string sDataFileName;
	int ret = dbcomm::DumpToDBDataFileName(iFileNo, sDataFileName);
	if (0 != ret)
	{
		logerr("DumpToDBDataFileName iFileNo %d ret %d", iFileNo, ret);
		return -1;
	}

	const std::string 
        sKvPathFile = dbcomm::ConcatePath(sKvPath, sDataFileName);
	if (0 != access(sKvPath.c_str(), F_OK))
	{
		logerr("sKvPathFile %s don't exist", sKvPathFile.c_str());
		return 0; // mv nothing: may already been moved;
	}

	{
		int iFileSize = dbcomm::GetFileSize(sKvPathFile.c_str());
		if (0 <= iFileSize && iFileSize <= getpagesize())
		{
			// empty file: simple one
			UnlinkAndSleep(sKvPathFile, 0);
			logerr("unlink small file %s ret %d %s", 
					sKvPathFile.c_str(), ret, strerror(errno));
			return 0;
		}
	}

    std::string sRecycleFileName = dbcomm::ConcatePath(sKvRecyclePath, sDataFileName);
	ret = dbcomm::IsLevelDBLogFormat(sKvPathFile.c_str());
	if (0 >= ret)
	{
		// ERROR or old bitcask format
		logerr("IsLevelDBLogFormat %s ret %d", sKvPathFile.c_str(), ret);

		UnlinkAndSleep(sKvPathFile, 0);
//		UnlinkAndSleep(sKvPathFile, DEFAULT_UNLINK_SLEEP_TIME);
		return 0;
	}

	assert(1 == ret);
	// deal with recycle only if file is already leveldb log format;
	// =>
	for (int iRetryCnt = 0; iRetryCnt < 3; ++iRetryCnt)
	{
		ret = dbcomm::DeprecateAllDataBlock(sKvPathFile.c_str());
		logerr("DeprecateAllDataBlock %s ret %d", sKvPathFile.c_str(), ret);
		if (0 == ret)
		{
			ret = rename(sKvPathFile.c_str(), sRecycleFileName.c_str());
			if (0 != ret)
			{
				logerr("rename %s %s ret %d strerror %s", 
						sKvPathFile.c_str(), sRecycleFileName.c_str(), 
						ret, strerror(errno));
				return -2;
			}

			logerr("MERGE MOVE %s %s", 
					sKvPathFile.c_str(), sRecycleFileName.c_str());
			return 0;
		}

		sleep(1); // retry after sleep 1s
	}

	// consider following code as very rare case
	// can't deprecate data block event after 3times retry
	// : remove this file
	UnlinkAndSleep(sKvPathFile, DEFAULT_UNLINK_SLEEP_TIME);
	return 0;
}


int InitLogBufferWriter(
		const int iBlockSize, 
		const int iMaxMergeBufferSize, 
		const uint64_t llBlkSeq, 
		const uint32_t iFileNo, 
		clsLevelDBLogBufferIterWriter& oWriter)
{
	// open clsLevelDBLogBufferIterWriter at sFileName with llNewBlkSeq
	int ret = oWriter.Init(
			iBlockSize, iMaxMergeBufferSize, 
			0, llBlkSeq, 0, iFileNo, 0);
	if (0 != ret)
	{
		logerr(
				"clsLevelDBLogBufferIterWriter::Init %d %ld ret %d", 
				iFileNo, llBlkSeq, ret);
		return -3;
	}

	return 0;
}

int UpdateShmHash(
		memkv::clsNewMemKv& oMemKv, 
		dbcomm::HashBaseLock* poHashBaseLock, 
		const memkv::HitRecordVec& vecHitRecord)
{
	HitRecordVec::const_iterator it;
	for (it = vecHitRecord.begin(); it != vecHitRecord.end(); ++it)
	{
		int ret = 0;
		const HitRecord_t *pstHitRecord = &(it->stHitRecord);
		int iOldFileNo = it->iOldFileNo;
		//uint32_t iOldOffset = it->iOldOffset;
		int iNewFileNo = pstHitRecord->iFileNo;
		uint32_t iNewOffset = pstHitRecord->iOffset;
		const char *sKey = pstHitRecord->sKey;
		uint8_t cKeyLen = pstHitRecord->cKeyLen;

		uint64_t llLogID = 0;
		assert(sizeof(llLogID) == static_cast<size_t>(cKeyLen));
		memcpy(&llLogID, sKey, cKeyLen);

        uint32_t iHash = cutils::dict_int_hash_func(llLogID);
        dbcomm::HashLock hashlock(poHashBaseLock, iHash);
		hashlock.WriteLock();

		NewBasic_t tBasicInfo = {0};
		ret = oMemKv.Get(llLogID, tBasicInfo);
		if (0 != ret)
		{
			logerr("clsNewMemKv::Get key %lu ret %d", llLogID, ret);
			return -1;
		}

		if (static_cast<uint32_t>(iOldFileNo) != tBasicInfo.iFileNo)
		//		|| iOldOffset != tBasicInfo.iOffset)
		{
			continue;
		}

		tBasicInfo.iFileNo = iNewFileNo;
		tBasicInfo.iOffset = iNewOffset;
		ret = oMemKv.Update(llLogID, tBasicInfo);
		if (0 != ret)
		{
			logerr("MergeThread %lu clsNewMemKv::Update key %lu ret %d", 
					pthread_self(), llLogID, ret);
			return -1;
		}
	}

	return 0;
}

class MergeIterWriter
{
public:
	MergeIterWriter(
			const std::string& sKvPath, 
			const bool bNeedCompresse, 
			const bool bNeedSpeedCtrl, 
			const int iMinFlushSize, 
			const int iMaxWriteSpeed, 
			clsLevelDBLogBufferIterWriter& oWriter)
		: m_sKvPath(sKvPath)
		, m_bNeedCompresse(bNeedCompresse)
		, m_bNeedSpeedCtrl(bNeedSpeedCtrl)
		, m_iMinFlushSize(iMinFlushSize)
		, m_llMaxWriteSpeed(iMaxWriteSpeed)
		, m_llPrevTime(0)
		, m_llPrevWriteSize(0)
		, m_oWriter(oWriter)
	{
		assert(false == m_sKvPath.empty());
		assert(0 <= m_llMaxWriteSpeed);
	}

	int WriteWithCtrl(const Record_t& stRecord, int& iFileNo, uint32_t& iOffset)
	{
		int ret = Write(stRecord, iFileNo, iOffset);
		if (false == m_bNeedSpeedCtrl || 0 != ret)
		{
			return ret;
		}

		assert(0 == ret && true == m_bNeedSpeedCtrl); // success write
		// 1MB
		if (m_oWriter.GetBufferUsedSize() >= m_iMinFlushSize)
		{
			ret = m_oWriter.Flush();
			if (0 != ret)
			{
				return ret;
			}
		}

		// only when flush out ?
		return ApplySimpleSpeedControlNotFlush();
		// return ApplySimpleSpeedControl();
	}

	int Write(const Record_t& stRecord, int& iFileNo, uint32_t& iOffset)
	{
		// add speed ctrl ?
		if (m_bNeedCompresse && 
                0 == (dbcomm::RECORD_COMPRESSE & stRecord.cFlag))
		{
			return WriteWithCompresse(stRecord, iFileNo, iOffset);
		}

		return WriteWithoutCompresse(stRecord, iFileNo, iOffset);
	}

	int Write(const char* pValue, int iValLen, int& iFileNo, uint32_t& iOffset)
	{
		uint32_t iAbsFileNo = 0;
		int ret = m_oWriter.Write(pValue, iValLen, iAbsFileNo, iOffset);
		if (0 == ret)
		{
			assert(0 < static_cast<int>(iAbsFileNo));
			iFileNo = -(static_cast<int>(iAbsFileNo));
		}
		return ret;
	}

private:
	int WriteWithoutCompresse(
			const Record_t& stRecord, int& iFileNo, uint32_t& iOffset)
	{
		// try to compress before write back ?
        int iRecordSize = dbimpl::CalculateRecordSize(stRecord); 
        assert(0 < iRecordSize);
    
        std::string sRecordBuffer(iRecordSize, '\0');
        dbimpl::ToBufferImpl(
                stRecord, iRecordSize, &sRecordBuffer[0], iRecordSize);

		int ret = Write(
                sRecordBuffer.data(), sRecordBuffer.size(), iFileNo, iOffset);
		return ret;
	}

	int WriteWithCompresse(
			const Record_t& stRecord, int& iFileNo, uint32_t& iOffset)
	{
		assert(0 == (dbcomm::RECORD_COMPRESSE & stRecord.cFlag));
        return WriteWithoutCompresse(stRecord, iFileNo, iOffset);
//		Record_t tCompresseRecord = {0};
//		Compresse::CompresseRecordNoFree(
//				const_cast<Record_t*>(&stRecord), &tCompresseRecord);
//		int ret = 0;
//		if (stRecord.iValLen <= tCompresseRecord.iValLen)
//		{
//			ret = WriteWithoutCompresse(stRecord, iFileNo, iOffset);
//		}
//		else
//		{
//			// write back the compress one
//			ret = WriteWithoutCompresse(tCompresseRecord, iFileNo, iOffset);
//		}
//
//		Compresse::FreeRecord(&tCompresseRecord);
//		return ret;
	}

	int ApplySimpleSpeedControlNotFlush()
	{
		int64_t llCurrentSpeed = m_oWriter.GetAccWriteSize() - m_llPrevWriteSize;
		llCurrentSpeed = 0 > llCurrentSpeed ? 0 : llCurrentSpeed;
		if (llCurrentSpeed <= m_llMaxWriteSpeed)
		{
			return 0;
		}

		// else
		uint64_t llNow = dbcomm::GetTickMS();
		if (llNow >= m_llPrevTime + 1000) // 1s
		{
			m_llPrevTime = llNow;
			m_llPrevWriteSize = m_oWriter.GetAccWriteSize();
			return 0;
		}

		// usleep ctrl write speed
		int64_t llSleepTime = 1000 - (llNow - m_llPrevTime);
		llSleepTime = 0 > llSleepTime ? 0 : llSleepTime;
		assert(llSleepTime < 1000);
		if (0 < llSleepTime)
		{
			usleep(llSleepTime * 1000); // ms
		}

		m_llPrevTime = llNow;
		m_llPrevWriteSize = m_oWriter.GetAccWriteSize();
		return 0;
	}

	int ApplySimpleSpeedControl()
	{
		uint64_t llNow = dbcomm::GetTickMS();
		if (llNow >= m_llPrevTime + 1000) // 1s
		{
			m_llPrevTime = llNow;
			m_llPrevWriteSize = m_oWriter.GetAccWriteSize();
			return 0;
		}

		// iNow == m_iPrevSecond;
		int64_t llCurrentSpeed = m_oWriter.GetAccWriteSize() - m_llPrevWriteSize;
		llCurrentSpeed = 0 > llCurrentSpeed ? 0 : llCurrentSpeed;
		if (llCurrentSpeed <= m_llMaxWriteSpeed)
		{
			return 0; 
		}

//		// flush before usleep
//		int ret = m_oWriter.Flush();
//		if (0 != ret)
//		{
//			return ret;
//		}
//
//		assert(0 == ret);
//		llNow = dbcomm::GetTickMS(); // reflash
		int64_t llSleepTime = 1000 - (llNow - m_llPrevTime);
		llSleepTime = 0 > llSleepTime ? 0 : llSleepTime;
		assert(llSleepTime < 1000);
		if (0 < llSleepTime)
		{
			usleep(llSleepTime * 1000); // ms
		}

		m_llPrevTime = llNow;
		m_llPrevWriteSize = m_oWriter.GetAccWriteSize();
		return 0;
	}

private:
	const std::string& m_sKvPath;
	const bool m_bNeedCompresse;
	const bool m_bNeedSpeedCtrl;
	const uint32_t m_iMinFlushSize;
	const int64_t m_llMaxWriteSpeed;
	uint64_t m_llPrevTime;
	uint64_t m_llPrevWriteSize;

	clsLevelDBLogBufferIterWriter& m_oWriter;
};

void PushHitRecord(
		const std::string& sRawRecord, 
		int iOldFileNo, uint32_t iOldOffset, 
		int iNewFileNo, uint32_t iNewOffset, 
		HitRecordVec& vecHitRecord)
{
	HR_t stHR= {0};    
	using namespace dbimpl;
	assert(false == sRawRecord.empty());
	assert(true == BitCaskRecord::IsAValidRecord(
				sRawRecord.data(), sRawRecord.data() + sRawRecord.size()));
	{
		BitCaskRecord::pointer 
			tPtr = BitCaskRecord::MakeBitCaskRecordPtr(sRawRecord.data());
		HitRecord_t& stHRec = stHR.stHitRecord;
		// build a HitRecord_t
		stHRec.cFlag = tPtr.head->cFlag;
		stHRec.cKeyLen = tPtr.head->cKeyLen;
		stHRec.iValLen = tPtr.head->GetValueLen();
		memcpy(stHRec.sKey, tPtr.head->sKey, tPtr.head->cKeyLen);
		stHRec.iVerA = tPtr.tail->GetVerA();
		stHRec.iVerB = tPtr.tail->GetVerB();
		stHRec.iFileNo = iNewFileNo;
		stHRec.iOffset = iNewOffset;
		// end of build a HitRecord_t
	}

	stHR.iOldFileNo = iOldFileNo;
	stHR.iOldOffset = iOldOffset;
	vecHitRecord.push_back(stHR);
}

void PushHitRecord(
		const Record_t& stRecord, 
		int iOldFileNo, uint32_t iOldOffset, 
		int iNewFileNo, uint32_t iNewOffset, 
		HitRecordVec& vecHitRecord)
{
	HR_t stHR= {0};    
	{
		HitRecord_t& stHRec = stHR.stHitRecord;
		// build a HitRecord_t
		stHRec.cFlag = stRecord.cFlag;
		stHRec.cKeyLen = stRecord.cKeyLen;
		stHRec.iValLen = stRecord.iValLen;
		memcpy(stHRec.sKey, stRecord.sKey, stRecord.cKeyLen);
		stHRec.iVerA = stRecord.iVerA;
		stHRec.iVerB = stRecord.iVerB;
		stHRec.iFileNo = iNewFileNo;
		stHRec.iOffset = iNewOffset;
		// end of build a HitRecord_t
	}

	stHR.iOldFileNo = iOldFileNo;
	stHR.iOldOffset = iOldOffset;
	vecHitRecord.push_back(stHR);
}

// mem visitor
struct RecordInfo_t
{
	Record_t rec;
	int fileno;
	uint32_t offset;
};

class clsPMergeMemVisitor : public clsNewMemVisitor
{
private:
	std::vector<RecordInfo_t> m_vecRecords;
	std::unordered_set<int> m_setFileNo;

	uint64_t m_llRecordCnt;
	uint64_t m_llRecordSize;
	uint64_t m_llTotalRecordCnt;
	uint64_t m_llTotalRecordSize;

public:

	clsPMergeMemVisitor(
			memkv::clsNewMemKv& oMemKv, 
            const std::vector<int>& vecFileNo)
		: clsNewMemVisitor(oMemKv)
		, m_setFileNo(vecFileNo.begin(), vecFileNo.end())
		, m_llRecordCnt(0)
		, m_llRecordSize(0)
		, m_llTotalRecordCnt(0)
		, m_llTotalRecordSize(0)
	{
		assert(m_vecRecords.empty());
		assert(m_setFileNo.size() == vecFileNo.size());
	}

	~clsPMergeMemVisitor()
	{
		Clear();
	}

	virtual int OnHead(const HeadWrapper& oHead) override
	{
		NewBasic_t tBasicInfo = {0};
		oHead.GetBasicInfo(tBasicInfo);
		
		if (m_setFileNo.end() == m_setFileNo.find(*oHead.pFileNo))
		{
			return 0;
		}

		assert(m_setFileNo.end() != m_setFileNo.find(*oHead.pFileNo));

		RecordInfo_t a = {0};

		a.fileno = *oHead.pFileNo;
		//a.offset = *oHead.pOffset;
		a.offset = 0;

		Record_t& stRecord = a.rec;
		stRecord.cFlag = tBasicInfo.cFlag;
		assert(false == dbcomm::TestFlag(stRecord.cFlag, dbcomm::RECORD_COMPRESSE));
		if (HasCompresseFlag(oHead))
		{
			stRecord.cFlag = dbcomm::AddFlag(
                    stRecord.cFlag, dbcomm::RECORD_COMPRESSE);
		}

		memcpy(stRecord.sKey, oHead.pLogID, sizeof(*oHead.pLogID));
		stRecord.cKeyLen = sizeof(*oHead.pLogID);
		assert(static_cast<size_t>(stRecord.cKeyLen) == sizeof(uint64_t));
		stRecord.iVerA = tBasicInfo.iVersion;
		stRecord.iVerB = tBasicInfo.iVersion;

		stRecord.iValLen = *oHead.pDataLen;
		assert(NULL == stRecord.pVal);
		assert(0 <= stRecord.iValLen);
		if (0 < stRecord.iValLen)
		{
			stRecord.pVal = reinterpret_cast<char*>(malloc(stRecord.iValLen));	
			memcpy(stRecord.pVal, oHead.pData, stRecord.iValLen);
		}
		
		m_vecRecords.push_back(a);

		++m_llRecordCnt;
		++m_llTotalRecordCnt;
		m_llRecordSize += stRecord.iValLen;
		m_llTotalRecordCnt += stRecord.iValLen;
		return 0;
	}

	inline void Clear()
	{
		if (m_vecRecords.empty())
		{
			return ;
		}

		m_llRecordCnt = 0;
		m_llRecordSize = 0;

		for (size_t idx = 0; idx < m_vecRecords.size(); ++idx)
		{
			Record_t& stRecord = m_vecRecords[idx].rec;
			if (0 < stRecord.iValLen)
			{
				free(stRecord.pVal);
				stRecord.pVal = NULL;
			}

			assert(NULL == stRecord.pVal);
		}

		m_vecRecords.clear();
	}

	inline std::vector<RecordInfo_t>& GetRecords()
	{
		return m_vecRecords;
	}

	inline uint64_t GetRecordCnt() const
	{
		return m_llRecordCnt;
	}

	inline uint64_t GetRecordSize() const
	{
		return m_llRecordSize;
	}

	inline uint64_t GetTotalRecordCnt() const {
		return m_llTotalRecordCnt;
	}

	inline uint64_t GetTotalRecordSize() const {
		return m_llTotalRecordSize;
	}
};



} // namespace


namespace memkv {


PMergetor::PMergetor(
		const char* sPath, 
		const char* sKvRecyclePath, 
		clsNewMemKv& oMemKv, 
		HashBaseLock* poHashBaseLock)
	: m_sKvPath(sPath)
	, m_sKvRecyclePath(sKvRecyclePath)
	, m_oMemKv(oMemKv)
	, m_poHashBaseLock(poHashBaseLock)
	, m_poWriter(NULL)
	, m_iStop(0)
{
	assert(NULL != sPath);
	assert(NULL != sKvRecyclePath);
	assert(NULL != poHashBaseLock);

	assert(0 == dbcomm::CheckAndFixDirPath(m_sKvRecyclePath));
}

PMergetor::~PMergetor()
{
	if (NULL != m_poWriter)
	{
		delete m_poWriter;
		m_poWriter = NULL;
	}
}


int PMergetor::Init()
{
	assert(NULL == m_poWriter);

    const int iBlockSize = 4 * 1024;
    const int iMaxMergeBufferSize = 21 * 1024 * 1024;
	
	// bIsMergeWrite == true
	m_poWriter = new dbcomm::clsLevelDBLogBufferIterWriter(
			m_sKvPath.c_str(), m_sKvRecyclePath.c_str(), true);
	logerr("clsLevelDBLogBufferIterWriter %s %s", 
			m_sKvPath.c_str(), m_sKvRecyclePath.c_str());
	assert(NULL != m_poWriter);

	// update merge meta info
	uint32_t iFileNo = 0;
	uint32_t iOffset = 0;
	uint64_t llBlkSeq = 0;
	int ret = ReadMergeMetaInfo(
            m_sKvPath, iBlockSize, iFileNo, iOffset, llBlkSeq);
	if (0 != ret)
	{
		logerr("ReadMergeMetaInfo %s ret %d", m_sKvPath.c_str(), ret);
		return -2;
	}

	assert(0 < iFileNo);
	assert(0 < llBlkSeq);
	if (iOffset > static_cast<uint32_t>(iBlockSize))
	{
		// a merge file have valid write
		++iFileNo; // shift to next 
		iOffset = 0;
		++llBlkSeq;
	}

	assert(0 < llBlkSeq);

	logerr("INFO %s iFileNo %d llBlkSeq %lu", m_sKvPath.c_str(), iFileNo, llBlkSeq);
	// try open LogBufferWrite with given metainfo & recyle list
	ret = InitLogBufferWriter(
			iBlockSize, iMaxMergeBufferSize, llBlkSeq, iFileNo, *m_poWriter);
	if (0 != ret)
	{
		logerr("InitLogBufferWriter %s ret %d", m_sKvPath.c_str(), ret);
		return -3;
	}

	logerr("INFO: path %s iFileNo %u iOffset %u llBlkSeq %lu", 
			m_sKvPath.c_str(), 
			m_poWriter->GetFileNo(), m_poWriter->GetCurrentOffset(), 
			m_poWriter->GetCurrentBlkSeq());
	return 0;
}

void PMergetor::GatherFilesToMerge(
		std::vector<int>& vecWriteFile, std::vector<int>& vecMergeFile)
{
	vecWriteFile.clear();
	vecMergeFile.clear();

	// TODO
	dbcomm::GatherWriteFilesFromDataPath(m_sKvPath.c_str(), vecWriteFile);
    std::sort(vecWriteFile.begin(), vecWriteFile.end());
	int iMinWriteFileNo = vecWriteFile.empty() ? 0 : vecWriteFile.front();
	int iMaxWriteFileNo = vecWriteFile.empty() ? 0 : vecWriteFile.back();

	vecWriteFile.clear();
	for(int i = iMinWriteFileNo; i < iMaxWriteFileNo; i++)
	{
		vecWriteFile.push_back(i);
	}

	dbcomm::GatherMergeFilesFromDataPath(m_sKvPath.c_str(), vecMergeFile);
    std::sort(vecMergeFile.begin(), 
            vecMergeFile.end(), std::greater<int>());
}


int PMergetor::DumpPartialFile(const HitRecordVec& vecHitRecord)
{
	int ret = m_poWriter->Flush();
	if (0 != ret)
	{
		logerr("m_poWriter::Flush ret %d", ret);
		return -3;
	}

	ret = UpdateShmHash(
			m_oMemKv, m_poHashBaseLock, vecHitRecord);
	if (0 > ret)
	{
		logerr("UpdateShmHash ret %d", ret);
		return -4;
	}

	return 0;
}

template <typename MemKvVisitorType>
int PMergetor::MergeSomeFile_MemInner(
		const std::vector<int>& vecFile, 
		const int iUrgentDiskRatio, 
		MemKvVisitorType& visitor)
{
	if (vecFile.empty())
	{
		return 0;
	}

	const bool bNeedCompresse = false;
	const bool bEnableMergeWriteSpeedCtrl = true;
    const int iMinMergeFlushSize = 4 * 1024 * 1024;
    const int iMaxMergeWriteSpeed = 64 * 1024 * 1024;
    

	MergeIterWriter oIterWriter(
			m_sKvPath, bNeedCompresse, 
			bEnableMergeWriteSpeedCtrl, 
            iMinMergeFlushSize, iMaxMergeWriteSpeed, 
			*m_poWriter);

	HitRecordVec vecHitRecord;
	int64_t llTotalCnt = 0;
	int ret = 0;
	logerr("begint to merge %s iFileNo %u", 
			m_sKvPath.c_str(), m_poWriter->GetFileNo());
	for (uint64_t idx = 0; idx < m_oMemKv.GetIdxHeadSize(); ++idx)
	{
		if (m_iStop)
		{
			return -1;
		}

		// 1. get iFileNo's records
		visitor.Clear();
		int ret = -1;
		{
            dbcomm::HashLock hasklock(m_poHashBaseLock, idx);
			hasklock.ReadLock();

			ret = m_oMemKv.Visit(&visitor, idx);
		}

		if (0 != ret)
		{
			logerr("clsMemKv::Visit idx %lu ret %d", idx, ret);
			return -1;
		}

		if (visitor.GetRecords().empty())
		{
			continue;
		}

		// 2. dump records
		for (size_t i = 0; i < visitor.GetRecords().size(); ++i)
		{
			++llTotalCnt;
			RecordInfo_t& rec = visitor.GetRecords()[i];

			int iNewFileNo = 0;
			uint32_t iNewOffset = 0;
//			ret = oIterWriter.Write(rec.rec, iNewFileNo, iNewOffset);
			ret = oIterWriter.WriteWithCtrl(
                    rec.rec, iNewFileNo, iNewOffset);
			if (0 != ret)
			{
				logerr("MergeIterWriter::Write ret %d", ret);
				return -2;
			}

			PushHitRecord(
					rec.rec, rec.fileno, 
                    rec.offset, iNewFileNo, iNewOffset, 
					vecHitRecord);
		}

        const size_t iMaxMergeHitRecordCnt = 10 * 1000;
        if (iMaxMergeHitRecordCnt <= vecHitRecord.size()) {
			logerr("%s iFileNo %u vecHitRecord.size %lu", 
					m_sKvPath.c_str(), 
                    m_poWriter->GetFileNo(), vecHitRecord.size());
			ret = DumpPartialFile(vecHitRecord);
			if (0 != ret)
			{
				return ret;
			}
			vecHitRecord.clear(); 
            // clear the flush & update vecHitRecord
		}
	}

	ret = DumpPartialFile(vecHitRecord);
	if (0 != ret)
	{
		logerr("DumpPartialFile ret %d", ret);
		return -5;
	}

	vecHitRecord.clear();
	return 0;
}

int PMergetor::MergeSomeFile_Mem(
		const std::vector<int>& vecFile, 
		const int iUrgentDiskRatio)
{
	time_t start = time(NULL);

	clsPMergeMemVisitor visitor(m_oMemKv, vecFile);
	int ret = MergeSomeFile_MemInner(vecFile, iUrgentDiskRatio, visitor);
	time_t end = time(NULL);

    std::stringstream ss;
	for (size_t i = 0; i < vecFile.size(); ++i)
	{
		ss << " " << vecFile[i];
	}

    std::string sMsg = ss.str();
	logerr("MergeSomeFile_Mem ret %d use %ld record_cnt %lu "
			"record_size %lu %s", ret, end - start, 
			visitor.GetTotalRecordCnt(), 
			visitor.GetTotalRecordSize(), 
			sMsg.c_str());
	logerr("%s ret %d use %ld record_cnt %lu "
			"record_size %lu %s", m_sKvPath.c_str(), ret, end - start, 
			visitor.GetTotalRecordCnt(), 
			visitor.GetTotalRecordSize(), 
			sMsg.c_str());
	return ret;
}


int PMergetor::MergeAllFiles(
		const std::vector<int>& vecFile, 
		int iUrgentDiskRatio, 
		size_t iMaxSplit)
{
	assert(0 < iMaxSplit);
	int ret = 0;
	// merge by mem
	logerr("%s vecFile.size %lu", m_sKvPath.c_str(), vecFile.size());
	Print(vecFile, "MergeAllFiles vecFile");

    std::vector< std::vector<int> > vecSplit;
	size_t iSizePerSplit = std::max<size_t>(
            5, vecFile.size() / iMaxSplit);
	SplitVector(vecFile, iSizePerSplit, vecSplit);
	printf ( "vecFile.size %zu iMaxSplit %zu "
            "iSizePerSplit %zu vecSplit.size %zu\n", 
			vecFile.size(), iMaxSplit, iSizePerSplit, vecSplit.size() );

	for (size_t i = 0; i < vecSplit.size(); ++i)
	{
		time_t iBeginTime = time(nullptr);
		Print(vecSplit[i], "vecSplit[i] ");
		ret = MergeSomeFile_Mem(vecSplit[i], iUrgentDiskRatio);
		if (0 != ret)
		{
			logerr("MergeSomeFile_Mem ret %d", ret);
			return -3;
		}

		const bool bUrgent = CheckMerge::CheckDiskRatio(
				m_sKvPath.c_str(), iUrgentDiskRatio);
		for (size_t idx = 0; idx < vecSplit[i].size(); ++idx)
		{
			int iFileNo = vecSplit[i][idx];
			if (bUrgent) {
				ret = UnlinkInUrgentMode(m_sKvPath, iFileNo);
			}
			else {
				ret = MoveFileIntoRecycleDir(m_sKvPath, m_sKvRecyclePath, iFileNo);
			}

			if (0 != ret)
			{
				logerr("MoveFileIntoRecycleDir iUrgentDiskRatio %d iFileNo %d ret %d", 
						iUrgentDiskRatio, iFileNo, ret);
				return -4;
			}
		}

		time_t iEndTime = time(nullptr);
		logerr("%s complete merge vecSplit[%lu].size %lu costtime %u", 
				m_sKvPath.c_str(), i, vecSplit[i].size(), static_cast<uint32_t>(iEndTime - iBeginTime));
		// a success merge
	}

	return 0;
}


// 0: no merge
// 1: merge immediate
// 2: merge by time
int PMergetor::CalculateMergeMode(
		size_t iWriteFileCount, size_t iMergeFileCount)
{
	if (!CheckMerge::CheckFile(m_sKvPath.c_str()))
	{
		logerr("DEBUG: CheckFile %s false", m_sKvPath.c_str());
		return MERGE_MODE_NIL;
	}

    const int iMaxDiskRatio = 60;
	if (CheckMerge::CheckDiskRatio(m_sKvPath.c_str(), iMaxDiskRatio))
	{
		logerr("DEBUG: CheckDiskRatio true");
		// disk ratio >= 80 => urgent mode for merge
        const int iUrgentDiskRatio = 75;
		// iUrgentDiskRatio = max(80, iUrgentDiskRatio);
		bool bUrgentMode = 
            CheckMerge::CheckDiskRatio(m_sKvPath.c_str(), iUrgentDiskRatio);

		int iMergeFileRatio = bUrgentMode ? iUrgentDiskRatio : iMaxDiskRatio;
		logerr("bUrgentMode %d iMaxDiskRatio %d iUrgentDiskRatio %d iMergeFileRatio %d [%zu %zu %zu]", 
				static_cast<int>(bUrgentMode), iMaxDiskRatio, iUrgentDiskRatio, iMergeFileRatio, 
				iMergeFileCount, iMergeFileCount * iMergeFileRatio, iWriteFileCount);
		if (iMergeFileCount * iMergeFileRatio <= iWriteFileCount * 100)
		{
			return bUrgentMode ? MERGE_MODE_URGENT : MERGE_MODE_IMMEDIATE;
		}

//		if (true == bUrgentMode)
//		{
//			logerr("URGENT MODE iWriteFileCount %zu GetUrgentMergeWFileCount %d", 
//					iWriteFileCount, m_oConfig.GetUrgentMergeWFileCount());
//			if (static_cast<int>(iWriteFileCount) >= 
//					m_oConfig.GetUrgentMergeWFileCount())
//			{
//				return MERGE_MODE_IMMEDIATE;
//			}
//		}
//
//		if (CheckMerge::CheckUsageAgainstRecycle(
//					m_sKvPath, m_sKvRecyclePath, 
//					m_oConfig.GetMergeAgainstRecycleRatio()))
//		{
//			logerr("DEBUG: CheckUsageAgainstRecycle true");
//			return MERGE_MODE_IMMEDIATE;
//		}
	}

    static int arrMergeTime[3] = {1, 10, 18};
	if (CheckMerge::CheckTime(3, arrMergeTime)) {
		return MERGE_MODE_NORMAL;
	}

	return MERGE_MODE_NIL;
}

//bool PMergetor::IsNeedGenMergeInfo()
//{
//    static int arrMergeTime = {1, 8, 15};
//	int *pMergeTime = arrMergeTime;
//	assert(pMergeTime != nullptr);
//	int iGenMergeInfoTime = *pMergeTime; // MergeTime0
//	if (iGenMergeInfoTime < 0 || iGenMergeInfoTime > 23) {
//		return false;
//	}
//
//	int iSleepTime = CalcSleepTime(iGenMergeInfoTime);
//	if (0 == iSleepTime)
//	{
//		if (false == m_bGenMergeInfoMark)
//		{
//			m_bGenMergeInfoMark = true;
//			return true;
//		}
//
//		return false;
//	}
//
//	m_bGenMergeInfoMark = false;
//	return false;
//}

#define IOPRIO_CLASS_IDLE 3
#define IOPRIO_WHO_PROCESS 1
#define IOPRIO_CLASS_SHIFT  (13)
#define IOPRIO_PRIO_VALUE(class, data)  (((class) << IOPRIO_CLASS_SHIFT) | data)

int PMergetor::Merge()
{
	assert(false == m_sKvPath.empty());
	assert(false == m_sKvRecyclePath.empty());
	BindToLastCpu();

    const size_t iMaxSplitSize = 5;
    int iUrgentDiskRatio = 75;
	// size_t iMaxSplitSize = m_oConfig.GetMaxMergeSplitSize();
	assert(0 < iMaxSplitSize);
	// int iUrgentDiskRatio = m_oConfig.GetUrgentDiskRatio();
	
    int ret = 0;
	// add for backup mergeinfo
	// end of backup mergeinfo
	bool bAlreadyMerge = false;
	while( !m_iStop ) 
	{
		const char *test_merge = "/home/qspace/data/kvsvr/test_merge";
		bool bTestMerge = !access( test_merge ,F_OK ) ;
		if( bTestMerge )
		{
			unlink( test_merge );
		}

        std::vector<int> vecWriteFile;
        std::vector<int> vecMergeFile;
		GatherFilesToMerge(vecWriteFile, vecMergeFile);
		logerr("%s vecWriteFile.size %lu vecMergeFile.size %lu", 
				m_sKvPath.c_str(), vecWriteFile.size(), vecMergeFile.size());
		int iMergeMode = CalculateMergeMode(
				vecWriteFile.size(), vecMergeFile.size());
		if (MERGE_MODE_NIL == iMergeMode)
		{
			iMergeMode = bTestMerge ? MERGE_MODE_IMMEDIATE : MERGE_MODE_NIL;
		}

		//if( !IsNeedToMerge() && !bTestMerge ) 
		if (MERGE_MODE_NIL == iMergeMode)
		{
			bAlreadyMerge = false;
			sleep(10);
			continue;
		}

		sleep(10);

		assert(MERGE_MODE_NIL != iMergeMode);
		if (MERGE_MODE_NORMAL == iMergeMode)
		{
			if (bAlreadyMerge)
			{
				logerr("iMergeMode %d bAlreadyMerge %d", 
						iMergeMode, static_cast<int>(bAlreadyMerge));
				// DO NOTHING FOR NOW
//				continue;
			}
			bAlreadyMerge = true;
		}

		{
			//make merge name
			struct tm now;
			time_t tx = time(NULL);
			localtime_r(&tx, &now);
			snprintf( m_szMergeName, sizeof(m_szMergeName), 
					"plog_%02d%02d%02d_%02d%02d%02d",
					now.tm_year + 1900, now.tm_mon + 1, now.tm_mday,
					now.tm_hour, now.tm_min, now.tm_sec );
		}

		if (size_t(3) >= vecWriteFile.size() && MERGE_MODE_NORMAL == iMergeMode)
		{
			logerr("INFO: drop merge %s"
					" vecWriteFile.size %zu vecMergeFile.size %zu", 
					m_sKvPath.c_str(), vecWriteFile.size(), vecMergeFile.size());
			sleep(10);
			continue;
		}

		// all the old merge file will be scan into vecMergeFile
		// : to avoid the conflict: merge fileno and write to the same fileno
		//   => simpley iter into next merge fileno;
		ret = m_poWriter->IterIntoNextFile();
		if (0 != ret)
		{
			logerr("clsLevelDBLogBufferIterWriter::IterIntoNextFile %s ret %d", 
					m_sKvPath.c_str(), ret);
			return -10;
		}

		std::sort(vecWriteFile.begin(), vecWriteFile.end());
		if (false == vecWriteFile.empty())
		{
			assert(vecWriteFile.front() <= vecWriteFile.back());
		}

		ret = MergeAllFiles(
                vecWriteFile, iUrgentDiskRatio, iMaxSplitSize);
		if (ret < 0) 
		{
			logerr("MergeWriteFile ret %d", ret);
			return -1;
		}

		ret = MergeAllFiles(
                vecMergeFile, iUrgentDiskRatio, iMaxSplitSize);
		if (ret < 0) 
		{
			logerr("MergeMergeFile ret %d", ret);
			return -2;
		}
	}
	return 0;
}


} // namespace memkv



