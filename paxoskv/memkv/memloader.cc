
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "memloader.h"
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <list>
#include <vector>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include "core/err_code.h"
#include "core/paxos.pb.h"
#include "core/plog_helper.h"
#include "cutils/cqueue.h"
#include "cutils/hash_utils.h"
#include "cutils/hassert.h"
#include "dbcomm/bitcask_log_impl.h"
#include "dbcomm/db_comm.h"
#include "dbcomm/db_compresse.h"
#include "dbcomm/leveldb_log.h"
#include "dbcomm/leveldb_log_impl.h"
#include "dbcomm/mmap_file.h"
#include "comm/kvsvrcomm.h"
#include "mem_assert.h"
#include "memcomm.h"
#include "memkv.h"

namespace {

#define MAX_VALUE_LEN 20 * 1024 * 1024

int UpdateBasicInfo(const paxos::PaxosLog& oPLog,
                    memkv::NewBasic_t& tBasicInfo) {
    memset(&tBasicInfo, 0, sizeof(tBasicInfo));
    // assert(tBasicInfo, 0);
    tBasicInfo.llMaxIndex = paxos::get_max_index(oPLog);
    tBasicInfo.llChosenIndex = paxos::get_chosen_index(oPLog);
    tBasicInfo.llReqID = paxos::get_chosen_reqid(oPLog);

    // left out: iFileNo, iOffset;
    auto chosen_ins = paxos::get_chosen_ins(oPLog);
    if (nullptr != chosen_ins) {
        assert(chosen_ins->has_accepted_value());
        paxos::DBData data;
        if (false ==
            data.ParseFromString(chosen_ins->accepted_value().data())) {
            return BROKEN_DBDATA;
        }

        tBasicInfo.iVersion = data.version();
        tBasicInfo.cFlag = data.flag();
    }

    assert(0 == tBasicInfo.cState);
    if (paxos::has_pending_ins(oPLog)) {
        tBasicInfo.cState = PENDING;
        auto max_ins = paxos::get_max_ins(oPLog);
        assert(nullptr != max_ins);
        assert(false == max_ins->chosen());
        if (max_ins->has_accepted_value()) {
            paxos::DBData data;
            if (false ==
                data.ParseFromString(max_ins->accepted_value().data())) {
                return BROKEN_DBDATA;
            }

            assert(data.version() >= tBasicInfo.iVersion);
        }
    }

    return 0;
}

int DoReloadProcessOnRecord(memkv::clsNewMemKv& oMemKv, const int iFileNo,
                            const uint32_t iOffset,
                            const std::string& sRawRecord,
                            std::vector<uint64_t>& vecChosenMsgLogID) {
    assert(false == sRawRecord.empty());
    assert(dbimpl::BitCaskRecord::IsAValidRecord(
        sRawRecord.data(), sRawRecord.data() + sRawRecord.size()));

    dbimpl::BitCaskRecord::pointer tPtr =
        dbimpl::BitCaskRecord::MakeBitCaskRecordPtr(sRawRecord.data());
    if (sizeof(uint64_t) != tPtr.head->cKeyLen) {
        logerr("LOADINFO: SKIP INVALID KEY cKeyLen %u %d %u",
               tPtr.head->cKeyLen, iFileNo, iOffset);
        return -1;
    }

    assert(sizeof(uint64_t) == tPtr.head->cKeyLen);
    assert(tPtr.head->GetValueLen() <= MAX_VALUE_LEN);  // TODO

    uint64_t llLogID = 0;
    memcpy(&llLogID, tPtr.head->sKey, sizeof(llLogID));
    std::string sUnCompresseBuffer;
    int ret =
        dbcomm::Compresse::MayUnCompresse(tPtr.body, tPtr.head->GetValueLen(),
                                          tPtr.head->cFlag, sUnCompresseBuffer);
    if (0 > ret) {
        logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u", llLogID, iFileNo,
               iOffset);
        return ret;
    }

    assert(0 == ret || 1 == ret);
    paxos::PaxosLog oPLog;
    if (0 == ret) {
        assert(sUnCompresseBuffer.empty());
        if (false ==
            oPLog.ParseFromArray(tPtr.body, tPtr.head->GetValueLen())) {
            logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u PaxosLog", llLogID,
                   iFileNo, iOffset);
            return BROKEN_PAXOS_LOG;
        }
    } else {
        assert(1 == ret);
        assert(false == sUnCompresseBuffer.empty());
        if (false == oPLog.ParseFromString(sUnCompresseBuffer)) {
            logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u PaxosLog", llLogID,
                   iFileNo, iOffset);
            return BROKEN_PAXOS_LOG;
        }
    }

    // check paxos::PaxosLog
    memkv::NewBasic_t tBasicInfo = {0};
    ret = UpdateBasicInfo(oPLog, tBasicInfo);
    if (0 != ret) {
        logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u UpdateBasicInfo ret %d",
               llLogID, iFileNo, iOffset, ret);
        return ret;
    }

    logerr("DEBUGINFO: key %lu iFileNo %u iOffset %u index %lu %lu", llLogID,
           iFileNo, iOffset, tBasicInfo.llMaxIndex, tBasicInfo.llChosenIndex);
    assert(0 == ret);
    tBasicInfo.iFileNo = iFileNo;  // int => uint32_t
    tBasicInfo.iOffset = iOffset;
    // may need check tBasicInfo.cFlag with tPtr.head->cFlag ?
    assert(tBasicInfo.iVersion == tPtr.tail->GetVerA());
    assert(tBasicInfo.iVersion == tPtr.tail->GetVerB());
    memkv::AssertCheck(tBasicInfo, oPLog);
    ret = oMemKv.Set(llLogID, tBasicInfo, oPLog);
    assert(0 == ret);

    logdebug("DEBUG: [%d %u] key %lu [%lu %c %u %lu]", iFileNo, iOffset,
             llLogID, tBasicInfo.llMaxIndex,
             tBasicInfo.cState == PENDING ? 'P' : ' ', tBasicInfo.iVersion,
             tBasicInfo.llReqID);

    if (false == paxos::has_pending_ins(oPLog)) {
        // chosen only
        vecChosenMsgLogID.push_back(llLogID);
    }

    return 0;
}

int DoReloadRecord(
    const int iFileNo,
    const std::vector<std::pair<uint32_t, std::string>>& vecRawRecord,
    memkv::clsNewMemKv& oMemKv, std::vector<uint64_t>& vecChosenMsgLogID) {
    if (vecRawRecord.empty()) {
        return 0;
    }

    int ret = 0;
    uint32_t iBrokenRecordCnt = 0;
    for (size_t idx = 0; idx < vecRawRecord.size(); ++idx) {
        uint32_t iRecordOffset = vecRawRecord[idx].first;
        const std::string& sRawRecord = vecRawRecord[idx].second;

        assert(false == sRawRecord.empty());
        assert(dbimpl::BitCaskRecord::IsAValidRecord(
            sRawRecord.data(), sRawRecord.data() + sRawRecord.size()));

        ret = DoReloadProcessOnRecord(oMemKv, iFileNo, iRecordOffset,
                                      sRawRecord, vecChosenMsgLogID);
        if (0 != ret) {
            ++iBrokenRecordCnt;
            logerr("LOADINFO: SKIP %d %u DoReloadProcessOnRecord ret %d",
                   iFileNo, iRecordOffset, ret);
            continue;
        }

        assert(0 == ret);
    }

    logerr("LOADINFO: STAT %d %zu %u", iFileNo, vecRawRecord.size(),
           iBrokenRecordCnt);
    printf("LOADINFO: STAT %d %zu %u\n", iFileNo, vecRawRecord.size(),
           iBrokenRecordCnt);
    assert(0 == iBrokenRecordCnt);
    return 0;
}

int ReadOneBitCaskFileImpl(
    const std::string& sFileName,
    std::vector<std::pair<uint32_t, std::string>>& vecRawRecord) {
    // TODO
    dbcomm::clsReadOnlyMMapFile oMMFile;
    int ret = oMMFile.OpenFile(sFileName);
    if (0 != ret) {
        logerr("clsReadOnlyMMapFile::OpenFile %s ret %d", sFileName.c_str(),
               ret);
        return ret;
    }

    assert(0 == ret);
    assert(oMMFile.End() >= oMMFile.Begin());

    const char* pIter = oMMFile.Begin();
    uint32_t iBrokenRecordCnt = 0;
    while (true) {
        if (pIter == oMMFile.End()) {
            break;
        }

        assert(pIter < oMMFile.End());
        const auto tPtr = dbimpl::BitCaskRecord::MakeBitCaskRecordPtr(pIter);
        if (false == dbimpl::BitCaskRecord::IsAValidRecord(
                         tPtr.ptr, tPtr.ptr + tPtr.tail->GetRecordLen())) {
            ++iBrokenRecordCnt;
            logerr("LOADINFO: SKIP BROKEN RECORD %s iRecordOffset %ld",
                   sFileName.c_str(), pIter - oMMFile.Begin());
            ++pIter;
            continue;
        }

        assert(0 < tPtr.tail->GetRecordLen());
        vecRawRecord.emplace_back(
            pIter - oMMFile.Begin(),
            std::string(tPtr.ptr, tPtr.tail->GetRecordLen()));

        pIter += tPtr.tail->GetRecordLen();
    }

    logerr("LOADINFO: %s vecRawRecord.size %zu iBrokenRecordCnt %u",
           sFileName.c_str(), vecRawRecord.size(), iBrokenRecordCnt);
    printf("LOADINFO: %s vecRawRecord.size %zu iBrokenRecordCnt %u\n",
           sFileName.c_str(), vecRawRecord.size(), iBrokenRecordCnt);
    assert(0 == iBrokenRecordCnt);
    return 0;
}

int ReadOneFileImpl(
    const std::string& sFileName,
    std::vector<std::pair<uint32_t, std::string>>& vecRawRecord) {
    assert(false == sFileName.empty());
    {
        int ret = dbcomm::IsLevelDBLogFormat(sFileName.c_str());
        if (1 != ret) {
            assert(0 == ret);
            struct stat stStat = {0};
            assert(0 == stat(sFileName.c_str(), &stStat));
            if (0 == stStat.st_size) {
                assert(0 == stStat.st_size);
                logerr("LOADINFO: ignore empty file %s", sFileName.c_str());
                printf("ignore empty file %s %d\n", sFileName.c_str(),
                       static_cast<int>(stStat.st_size));
                return 0;
            }

            assert(0 < stStat.st_size);
            return ReadOneBitCaskFileImpl(sFileName, vecRawRecord);
        }

        hassert(1 == ret, "%s %d", sFileName.c_str(), ret);
    }

    dbcomm::clsReadOnlyMMapFile oMMFile;
    int ret = oMMFile.OpenFile(sFileName);
    if (0 != ret) {
        logerr("clsReadOnlyMMapFile::OpenFile %s ret %d", sFileName.c_str(),
               ret);
        return ret;
    }

    assert(0 == ret);
    assert(oMMFile.End() >= oMMFile.Begin());

    dbcomm::clsLevelDBLogAttachReader oReader;
    ret = oReader.Attach(oMMFile.Begin(), oMMFile.End(), 0);
    if (0 != ret) {
        logerr("clsLevelDBLogAttachReader::Attach %s ret %d", sFileName.c_str(),
               ret);
        return ret;
    }

    assert(0 == ret);
    printf("begin read %s\n", sFileName.c_str());
    assert(vecRawRecord.empty());

    uint32_t iBrokenRecordCnt = 0;
    while (true) {
        std::string sRawRecord;
        uint32_t iRecordOffset = 0;
        // ret = oReader.ReadSkipError(sRawRecord, iRecordOffset);
        ret = oReader.Read(sRawRecord, iRecordOffset);
        if (0 != ret) {
            if (1 == ret) {
                break;
            }

            assert(0 > ret);
            logerr("LOADINFO: ReadSkipError %s ret %d", sFileName.c_str(), ret);
            if (DB_READ_ERROR_UNEXPECTED_EOF == ret) {
                // spec case
                std::string sNewRawRecord;
                uint32_t iNewRecordOffset = 0;
                int iNewRet =
                    oReader.ReadSkipError(sNewRawRecord, iNewRecordOffset);
                logerr(
                    "IMPORTANT!!!: iRecordOffset %u ReadSkipError "
                    "iNewRecordOffset %u iNewRet %d",
                    iRecordOffset, iNewRecordOffset, iNewRet);
                if (1 == iNewRet) {
                    break;  // EOF
                }
            }

            return ret;
        }

        assert(0 == ret);
        assert(0 < iRecordOffset);
        assert(false == sRawRecord.empty());
        if (false ==
            dbimpl::BitCaskRecord::IsAValidRecord(
                sRawRecord.data(), sRawRecord.data() + sRawRecord.size())) {
            ++iBrokenRecordCnt;
            logerr("LOADINFO: SKIP BROKEN RECORD %s iRecordOffset %u",
                   sFileName.c_str(), iRecordOffset);
            continue;
        }

        assert(false == sRawRecord.empty());
        vecRawRecord.emplace_back(iRecordOffset, std::move(sRawRecord));
    }

    logerr("LOADINFO: %s vecRawRecord.size %zu iBrokenRecordCnt %u",
           sFileName.c_str(), vecRawRecord.size(), iBrokenRecordCnt);
    printf("LOADINFO: %s vecRawRecord.size %zu iBrokenRecordCnt %u\n",
           sFileName.c_str(), vecRawRecord.size(), iBrokenRecordCnt);
    assert(0 == iBrokenRecordCnt);
    return 0;
}

void PushIntoCQueue(std::vector<cutils::clsSimpleCQueue<
                        std::tuple<int, uint32_t, std::string>>>& vecCQueue,
                    std::tuple<int, uint32_t, std::string>& item,
                    const size_t iQueueIdx) {
    assert(iQueueIdx < vecCQueue.size());
    while (true) {
        assert(false == std::get<2>(item).empty());
        int ret = vecCQueue[iQueueIdx].Enqueue(item);
        if (0 == ret) {
            assert(std::get<2>(item).empty());
            break;
        }

        usleep(100);
    }
}

int ReadOneFile(const uint32_t iMemIdxSize, const std::string& sDataPath,
                const int iFileNo,
                std::vector<cutils::clsSimpleCQueue<
                    std::tuple<int, uint32_t, std::string>>>& vecCQueue,
                uint64_t& llReadRecordCnt) {
    assert(false == sDataPath.empty());
    assert(0 != iFileNo);
    assert(false == vecCQueue.empty());

    std::string sFileName;
    assert(0 == dbcomm::DumpToDBFileName(std::abs(iFileNo),
                                         iFileNo > 0 ? 'w' : 'm', sFileName));
    sFileName = dbcomm::ConcatePath(sDataPath, sFileName);

    assert(false == sFileName.empty());

    std::vector<std::pair<uint32_t, std::string>> vecRawRecord;
    int ret = ReadOneFileImpl(sFileName, vecRawRecord);
    if (0 != ret) {
        logerr("ReadOneFileImpl %s ret %d", sFileName.c_str(), ret);
        return ret;
    }

    llReadRecordCnt = vecRawRecord.size();
    for (auto iter = vecRawRecord.rbegin(); iter != vecRawRecord.rend();
         ++iter) {
        std::string& sRawRecord = iter->second;
        assert(false == sRawRecord.empty());
        uint32_t iRecordOffset = iter->first;

        assert(dbimpl::BitCaskRecord::IsAValidRecord(
            sRawRecord.data(), sRawRecord.data() + sRawRecord.size()));
        dbimpl::BitCaskRecord::pointer tPtr =
            dbimpl::BitCaskRecord::MakeBitCaskRecordPtr(sRawRecord.data());

        assert(sizeof(uint64_t) == tPtr.head->cKeyLen);
        uint32_t iQueueIdx = 0;
        {
            uint64_t llLogID = 0;
            memcpy(&llLogID, tPtr.head->sKey, tPtr.head->cKeyLen);
            iQueueIdx = cutils::dict_int_hash_func(llLogID);
            iQueueIdx = iQueueIdx % iMemIdxSize % vecCQueue.size();
        }

        assert(static_cast<size_t>(iQueueIdx) < vecCQueue.size());
        std::tuple<int, uint32_t, std::string> item =
            std::make_tuple(iFileNo, iRecordOffset, std::move(sRawRecord));
        assert(false == std::get<2>(item).empty());
        PushIntoCQueue(vecCQueue, item, iQueueIdx);
        assert(std::get<2>(item).empty());
    }

    return 0;
}

int ReadAllWriteFiles(const uint32_t iMemIdxSize, const std::string& sDataPath,
                      std::vector<cutils::clsSimpleCQueue<
                          std::tuple<int, uint32_t, std::string>>>& vecCQueue,
                      uint64_t& llWriteFilesRecordCnt) {
    llWriteFilesRecordCnt = 0;
    std::vector<int> vecWriteFile;
    dbcomm::GatherWriteFilesFromDataPath(sDataPath, vecWriteFile);

    printf("%s vecWriteFile.size %zu\n", sDataPath.c_str(),
           vecWriteFile.size());
    if (vecWriteFile.empty()) {
        return 0;
    }

    for (size_t idx = 0; idx < vecWriteFile.size(); ++idx) {
        assert(0 < vecWriteFile[idx]);
    }

    std::sort(vecWriteFile.begin(), vecWriteFile.end(), std::greater<int>());
    assert(vecWriteFile.front() >= vecWriteFile.back());

    for (size_t idx = 0; idx < vecWriteFile.size(); ++idx) {
        assert(0 < vecWriteFile[idx]);
        uint64_t llReadRecordCnt = 0;
        int ret = ReadOneFile(iMemIdxSize, sDataPath, vecWriteFile[idx],
                              vecCQueue, llReadRecordCnt);
        assert(0 == ret);
        llWriteFilesRecordCnt += llReadRecordCnt;
    }

    return 0;
}

int ReadAllMergeFiles(const uint32_t iMemIdxSize, const std::string& sDataPath,
                      std::vector<cutils::clsSimpleCQueue<
                          std::tuple<int, uint32_t, std::string>>>& vecCQueue,
                      uint64_t& llMergeFilesRecordCnt) {
    llMergeFilesRecordCnt = 0;
    std::vector<int> vecMergeFile;
    dbcomm::GatherMergeFilesFromDataPath(sDataPath, vecMergeFile);

    printf("%s vecMergeFile.size %zu\n", sDataPath.c_str(),
           vecMergeFile.size());
    if (vecMergeFile.empty()) {
        return 0;
    }

    for (size_t idx = 0; idx < vecMergeFile.size(); ++idx) {
        assert(0 > vecMergeFile[idx]);
    }

    std::sort(vecMergeFile.begin(), vecMergeFile.end());
    assert(vecMergeFile.front() <= vecMergeFile.back());
    for (size_t idx = 0; idx < vecMergeFile.size(); ++idx) {
        assert(0 > vecMergeFile[idx]);
        uint64_t llReadRecordCnt = 0;
        int ret = ReadOneFile(iMemIdxSize, sDataPath, vecMergeFile[idx],
                              vecCQueue, llReadRecordCnt);
        assert(0 == ret);
        llMergeFilesRecordCnt += llReadRecordCnt;
    }

    return 0;
}

uint64_t ReadAllDiskFileWorker(
    const uint32_t iMemIdxSize, const std::string& sDataPath,
    std::vector<
        cutils::clsSimpleCQueue<std::tuple<int, uint32_t, std::string>>>&
        vecCQueue) {
    BindWorkerCpu();
    std::vector<int> vecWriteFile;
    dbcomm::GatherWriteFilesFromDataPath(sDataPath, vecWriteFile);

    std::vector<int> vecMergeFile;
    dbcomm::GatherMergeFilesFromDataPath(sDataPath, vecMergeFile);

    int ret = 0;
    uint64_t llWriteFilesRecordCnt = 0;
    ret = ReadAllWriteFiles(iMemIdxSize, sDataPath, vecCQueue,
                            llWriteFilesRecordCnt);
    assert(0 == ret);

    uint64_t llMergeFilesRecordCnt = 0;
    ret = ReadAllMergeFiles(iMemIdxSize, sDataPath, vecCQueue,
                            llMergeFilesRecordCnt);
    assert(0 == ret);

    return llWriteFilesRecordCnt + llMergeFilesRecordCnt;
}

uint64_t LoadToMemKvWorker(
    memkv::clsNewMemKv& oMemKv,
    cutils::clsSimpleCQueue<std::tuple<int, uint32_t, std::string>>& queRecords,
    bool& bHaveReadAll) {
    uint32_t iLoop = 0;
    uint64_t llBrokenRecordCnt = 0;
    uint64_t llSetKeyCnt = 0;
    uint64_t llTotalKeyCnt = 0;

    while (true) {
        ++iLoop;
        if (0 == iLoop % 1000000) {
            // Print queRecords status ?
        }

        std::tuple<int, uint32_t, std::string> item;
        int ret = queRecords.Dequeue(item);
        assert(0 == ret || 1 == ret);
        if (1 == ret) {
            // queue empty
            if (false == bHaveReadAll) {
                usleep(100);
                continue;
            }

            assert(bHaveReadAll);
            bool bRealyEmpty = true;
            // just try to reduce the prob. (max retry: 10ms)
            // => not safe !!!
            for (int i = 0; i < 100; ++i) {
                ret = queRecords.Dequeue(item);
                if (0 == ret) {
                    bRealyEmpty = false;
                    break;
                }
                assert(1 == ret);  // empty
                usleep(100);
            }

            if (bRealyEmpty) {
                break;
            }
            assert(0 == ret);
        }

        ++llTotalKeyCnt;
        int iFileNo = std::get<0>(item);
        uint32_t iOffset = std::get<1>(item);
        std::string sRawRecord = std::move(std::get<2>(item));
        assert(false == sRawRecord.empty());

        assert(dbimpl::BitCaskRecord::IsAValidRecord(
            sRawRecord.data(), sRawRecord.data() + sRawRecord.size()));
        auto tPtr =
            dbimpl::BitCaskRecord::MakeBitCaskRecordPtr(sRawRecord.data());
        if (sizeof(uint64_t) != tPtr.head->cKeyLen) {
            ++llBrokenRecordCnt;
            logerr("LOADINFO: SKIP INVALID KEY %d %d %u", 
                    tPtr.head->cKeyLen, iFileNo, iOffset);
            continue;
        }

        assert(sizeof(uint64_t) == tPtr.head->cKeyLen);
        assert(tPtr.head->GetValueLen() <= MAX_VALUE_LEN);

        uint64_t llLogID = 0;
        memcpy(&llLogID, tPtr.head->sKey, sizeof(llLogID));
        // check llLogID is in memkv ?
        ret = oMemKv.Has(llLogID);
        assert(0 <= ret);
        if (1 == ret) {
            continue;
            // ignore in this case: memkv alreay have most recent
            // llLogID data;
        }

        assert(0 == ret);
        // else => don't exist yet
        std::string sUnCompresseBuffer;
        ret = dbcomm::Compresse::MayUnCompresse(
            tPtr.body, tPtr.head->GetValueLen(), tPtr.head->cFlag,
            sUnCompresseBuffer);
        if (0 > ret) {
            ++llBrokenRecordCnt;
            logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u", llLogID, iFileNo,
                   iOffset);
            continue;
        }

        assert(0 == ret || 1 == ret);
        paxos::PaxosLog oPLog;
        if (0 == ret) {
            assert(sUnCompresseBuffer.empty());
            if (false ==
                oPLog.ParseFromArray(tPtr.body, tPtr.head->GetValueLen())) {
                ++llBrokenRecordCnt;
                logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u PaxosLog",
                       llLogID, iFileNo, iOffset);
                continue;
            }
        } else {
            assert(1 == ret);
            assert(false == sUnCompresseBuffer.empty());
            if (false == oPLog.ParseFromString(sUnCompresseBuffer)) {
                ++llBrokenRecordCnt;
                logerr("LOADINFO: SKIP BROKEN RECORD %lu %d %u PaxosLog",
                       llLogID, iFileNo, iOffset);
                continue;
            }
        }

        assert(0 != iFileNo);

        // check paxos::PaxosLog;
        memkv::NewBasic_t tBasicInfo = {0};
        ret = UpdateBasicInfo(oPLog, tBasicInfo);
        if (0 != ret) {
            ++llBrokenRecordCnt;
            logerr(
                "LOADINFO: SKIP BROKEN RECORD %lu %d %u UpdateBasicInfo ret %d",
                llLogID, iFileNo, iOffset, ret);
            continue;
        }

        assert(0 == ret);
        tBasicInfo.iFileNo = iFileNo;
        tBasicInfo.iOffset = iOffset;
        assert(tBasicInfo.iVersion == tPtr.tail->GetVerA());
        assert(tBasicInfo.iVersion == tPtr.tail->GetVerB());
        memkv::AssertCheck(tBasicInfo, oPLog);
        ret = oMemKv.Set(llLogID, tBasicInfo, oPLog);
        assert(0 == ret);
        ++llSetKeyCnt;
    }

    assert(queRecords.IsEmpty());
    logerr("LOADINFO: llTotalKeyCnt %lu llBrokenRecordCnt %lu llSetKeyCnt %lu",
           llTotalKeyCnt, llBrokenRecordCnt, llSetKeyCnt);
    assert(0 == llBrokenRecordCnt);
    return llTotalKeyCnt;
}

}  // namespace

namespace memkv {

// 1. load write file
//    : from new to old; push record to worker in the opposite of [0,
//    max_filesize] order;
// 2. load merge file
//    : from new to old; push record in opposite order;
int LoadAllDiskFile(const std::string& sDataPath, memkv::clsNewMemKv& oMemKv,
                    const size_t iWorkerCnt) {
    struct timeval tStartTime = {0};
    gettimeofday(&tStartTime, NULL);

    const size_t MAX_QUEUE_LEN = 100000;
    assert(0 < iWorkerCnt);
    std::vector<cutils::clsSimpleCQueue<std::tuple<int, uint32_t, std::string>>>
        vecCQueue;
    vecCQueue.reserve(iWorkerCnt);
    for (size_t idx = 0; idx < iWorkerCnt; ++idx) {
        vecCQueue.emplace_back(MAX_QUEUE_LEN);
    }
    assert(vecCQueue.size() == iWorkerCnt);

    bool bHaveReadAll = false;
    std::future<uint64_t> futReadWorker = std::async(
        std::launch::async, ReadAllDiskFileWorker, oMemKv.GetIdxHeadSize(),
        std::cref(sDataPath), std::ref(vecCQueue));
    assert(futReadWorker.valid());

    std::vector<std::future<uint64_t>> vecWorker;
    for (size_t idx = 0; idx < iWorkerCnt; ++idx) {
        vecWorker.emplace_back(
            std::async(std::launch::async, LoadToMemKvWorker, std::ref(oMemKv),
                       std::ref(vecCQueue[idx]), std::ref(bHaveReadAll)));
        assert(vecWorker.back().valid());
    }

    uint64_t llTotalReadKeyCnt = futReadWorker.get();
    bHaveReadAll = true;  // signal
    uint64_t llTotalWorkerGetKeyCnt = 0;
    for (size_t idx = 0; idx < vecWorker.size(); ++idx) {
        llTotalWorkerGetKeyCnt += vecWorker[idx].get();
    }

    struct timeval tEndTime = {0};
    gettimeofday(&tEndTime, NULL);

    logerr(
        "LOADINFO: Path %s iWorkerCnt %zu llTotalReadKeyCnt %lu"
        " llTotalWorkerGetKeyCnt %lu cost time %d(s)",
        sDataPath.c_str(), iWorkerCnt, llTotalReadKeyCnt,
        llTotalWorkerGetKeyCnt,
        static_cast<int>(tEndTime.tv_sec - tStartTime.tv_sec));
    assert(llTotalReadKeyCnt == llTotalWorkerGetKeyCnt);
    return 0;
}

// the most recent write file
// => read from old to new; start offset to max_filesize per file;
int LoadMostRecentWriteFile(const std::string& sDataPath,
                            memkv::clsNewMemKv& oMemKv,
                            const size_t iMaxReloadRecordCnt,
                            std::set<uint64_t>& setChosenMsgLogID) {
    assert(0 < iMaxReloadRecordCnt);

    std::vector<int> vecWriteFile;
    dbcomm::GatherWriteFilesFromDataPath(sDataPath, vecWriteFile);

    printf("path %s writefile.size %zu\n", sDataPath.c_str(),
           vecWriteFile.size());
    logerr("LOADINFO: path %s vecWriteFile.size %zu", sDataPath.c_str(),
           vecWriteFile.size());
    if (vecWriteFile.empty()) {
        return 0;  // do nothing;
    }

    // check
    for (size_t idx = 0; idx < vecWriteFile.size(); ++idx) {
        assert(0 < vecWriteFile[idx]);  // write fileno > 0
    }

    std::sort(vecWriteFile.begin(), vecWriteFile.end());
    assert(vecWriteFile.front() <= vecWriteFile.back());
    assert(false == vecWriteFile.empty());

    std::list<std::pair<int, std::vector<std::pair<uint32_t, std::string>>>>
        lstRawRecord;

    int ret = 0;
    size_t iRawRecordCnt = 0;
    for (auto iter = vecWriteFile.rbegin(); iter != vecWriteFile.rend();
         ++iter) {
        assert(iRawRecordCnt < iMaxReloadRecordCnt);
        const int iFileNo = *iter;
        assert(0 != iFileNo);

        std::string sFileName;
        assert(0 == dbcomm::DumpToDBFileName(
                        std::abs(iFileNo), iFileNo > 0 ? 'w' : 'm', sFileName));
        sFileName = dbcomm::ConcatePath(sDataPath, sFileName);
        assert(false == sFileName.empty());

        std::vector<std::pair<uint32_t, std::string>> vecRawRecord;
        ret = ReadOneFileImpl(sFileName, vecRawRecord);
        assert(0 == ret);

        printf(
            "LOADINFO: %s iFileNo %d iRawRecordCnt %zu\n"
            " iMaxReloadRecordCnt %zu",
            sDataPath.c_str(), iFileNo, iRawRecordCnt, iMaxReloadRecordCnt);
        logerr(
            "LOADINFO: %s iFileNo %d iRawRecordCnt %zu"
            " iMaxReloadRecordCnt %zu",
            sDataPath.c_str(), iFileNo, iRawRecordCnt, iMaxReloadRecordCnt);
        if (vecRawRecord.empty()) {
            continue;
        }

        if (vecRawRecord.size() + iRawRecordCnt <= iMaxReloadRecordCnt) {
            iRawRecordCnt += vecRawRecord.size();
            assert(iRawRecordCnt <= iMaxReloadRecordCnt);
            lstRawRecord.emplace_back(iFileNo, std::move(vecRawRecord));

            continue;
        }

        assert(vecRawRecord.size() + iRawRecordCnt > iMaxReloadRecordCnt);
        std::vector<std::pair<uint32_t, std::string>> vecNewRawRecord;
        vecNewRawRecord.reserve(iMaxReloadRecordCnt - iRawRecordCnt);
        size_t ibegin =
            vecRawRecord.size() - (iMaxReloadRecordCnt - iRawRecordCnt);
        assert(0 < ibegin);
        assert(ibegin < vecRawRecord.size());
        for (; ibegin < vecRawRecord.size(); ++ibegin) {
            vecNewRawRecord.emplace_back(std::move(vecRawRecord[ibegin]));
        }

        assert(vecNewRawRecord.size() + iRawRecordCnt == iMaxReloadRecordCnt);
        iRawRecordCnt += vecNewRawRecord.size();
        assert(iRawRecordCnt == iMaxReloadRecordCnt);
        lstRawRecord.emplace_back(iFileNo, std::move(vecNewRawRecord));
        break;
    }

    assert(0 <= ret);
    assert(iRawRecordCnt <= iMaxReloadRecordCnt);
    std::vector<uint64_t> vecChosenMsgLogID;
    for (auto iter = lstRawRecord.crbegin(); iter != lstRawRecord.crend();
         ++iter) {
        assert(0 != iter->first);
        assert(false == iter->second.empty());
        ret = DoReloadRecord(iter->first, iter->second, oMemKv,
                             vecChosenMsgLogID);
        assert(0 == ret);
    }

    setChosenMsgLogID =
        std::set<uint64_t>(vecChosenMsgLogID.begin(), vecChosenMsgLogID.end());
    return 0;
}

}  // namespace memkv
