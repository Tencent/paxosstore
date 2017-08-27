
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <cstring>
#include <cassert>
#include "cutils/log_utils.h"
#include "cutils/mem_utils.h"
#include "core/err_code.h"
#include "core/paxos.pb.h"
#include "memkv/memkv.h"
#include "memkv/pmergetor.h"
#include "memkv/memcomm.h"
#include "memkv/memloader.h"
#include "dbcomm/newstorage.h"
#include "dbcomm/bitcask_log_impl.h"
#include "hard_memkv.h"
#include "db_option.h"


namespace {

void StartPLogMerge(
        memkv::PMergetor& mergetor, bool& stop)
{
    while (false == stop) {
        auto ret = mergetor.Merge();
        sleep(10);
    }
}

uint64_t to_logid(const std::string& key)
{
    assert(sizeof(uint64_t) == key.size());
    uint64_t logid = 0;
    memcpy(&logid, key.data(), key.size());
    return logid;
}

void to_plogheader(
        const memkv::NewBasic_t& info, 
        paxos::PaxosLogHeader& header)
{
    header.set_max_index(info.llMaxIndex);
    header.set_chosen_index(info.llChosenIndex);
    header.set_reqid(info.llReqID);
    header.set_version(info.iVersion);
}

void to_info(
        const paxos::PaxosLogHeader& header, 
        memkv::NewBasic_t& info)
{
    info.llMaxIndex = header.max_index();
    info.llChosenIndex = header.chosen_index();
    info.llReqID = header.reqid();
    info.iVersion = header.version();
    info.cState = (
            header.max_index() != header.chosen_index()) ? PENDING : 0;
    // cFlag == 0;
    // info.cState = paxos::has_pending_ins(plog) ? PENDING : 0;
}


} // namespace


namespace paxoskv {

clsHardMemKv::clsHardMemKv() = default;

clsHardMemKv::~clsHardMemKv()
{
	mergetor_->Stop();
}


int clsHardMemKv::Init(const Option& option)
{
    int ret = 0;
    assert(nullptr == memkv_);
    memkv_ = cutils::make_unique<memkv::clsNewMemKv>();
    assert(nullptr != memkv_);

    ret = memkv_->Init(
            option.mem_idx_headsize, 
            option.db_plog_path.c_str(), 
            option.mem_append_blknum,
			option.idx_shm_key,
			option.data_block_shm_key);
    if (0 != ret) {
        logerr("memkv::Init ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    ret = memkv_->StartBuildIdx();
    if (0 != ret) {
        logerr("memkv::StartBuildIdx ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    const auto& plog_path = option.db_plog_path;
    std::set<uint64_t> chosen_keys;
    ret = memkv_->ReloadDiskFile(
            false, 
            [=](memkv::clsNewMemKv& memkv) {
                return memkv::LoadAllDiskFile(
                        plog_path, memkv, size_t{8});
            }, 
            [&](memkv::clsNewMemKv& memkv) {
                return memkv::LoadMostRecentWriteFile(
                        plog_path, memkv, size_t{500}, chosen_keys);
            });
    if (0 != ret) {
        logerr("memkv::ReloadDiskFile ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    ret = memkv_->StartMemMergeThread();
    if (0 != ret) {
        logerr("memkv::StartMemMergeThread ret %d", ret);
        return ret;
    }

    assert(0 == ret);

    assert(nullptr == storage_);
    storage_ = cutils::make_unique<dbcomm::NewStorage>();
    assert(nullptr != storage_);

    const std::string recycle_path = option.db_plog_path + "/recycle";
    ret = storage_->Init(
            option.db_plog_path.c_str(), 
            recycle_path.c_str(), 
            option.ldb_blksize, 
            option.ldb_dio_bufsize, 
            option.ldb_adjust_strategy, 
            option.ldb_set_waittime);
    if (0 != ret) {
        logerr("storage::Init ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    assert(nullptr == mergetor_);
    mergetor_ = cutils::make_unique<
        memkv::PMergetor>(
                option.db_plog_path.c_str(), 
                recycle_path.c_str(), 
                *memkv_, storage_->GetHaseBaseLock());
    assert(nullptr != mergetor_);
    ret = mergetor_->Init();
    if (0 != ret) {
        logerr("PMergetor::Init ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    workers_.emplace_back(
            cutils::make_unique<cutils::AsyncWorker>(
                StartPLogMerge, std::ref(*mergetor_)));
    assert(nullptr != workers_.back());
    return 0;
}

int clsHardMemKv::Read(
        const std::string& key, 
        paxos::PaxosLogHeader& header, 
        paxos::PaxosLog& plog)
{
    if (key.size() != sizeof(uint64_t)) {
        return -1;
    }

    uint64_t logid = to_logid(key);
    memkv::NewBasic_t info = {0};
    auto ret = memkv_->Get(logid, info, plog);
    if (0 != ret) {
        logerr("memkv::Get key %lu ret %d", 
                logid, ret);
        return ret;
    }

    assert(0 == ret);
    to_plogheader(info, header);
    return 0;
}

int clsHardMemKv::ReadHeader(
        const std::string& key, 
        paxos::PaxosLogHeader& header)
{
    if (key.size() != sizeof(uint64_t)) {
        return -1;
    }

    uint64_t logid = to_logid(key);

    memkv::NewBasic_t info = {0};
    auto ret = memkv_->Get(logid, info);
    if (0 != ret) {
        logerr("memkv::Get key %lu ret %d", logid, ret);
        return ret;
    }

    to_plogheader(info, header);
    return 0;
}

int clsHardMemKv::Write(
        const std::string& key, 
        const paxos::PaxosLogHeader& header, 
        const paxos::PaxosLog& plog)
{
    if (key.size() != sizeof(uint64_t)) {
        return -1;
    }

    uint64_t logid = to_logid(key);
    // 0. build basic info
    memkv::NewBasic_t info = {0};
    to_info(header, info);

    // TODO: 
    // optimize step:
    //   0. raw_value = to_string(plog);
    //   1. write storage;
    //   2. write memkv;
    // 1. write storage
    int ret = 0;
    {
        std::string raw_value;
        if (false == plog.SerializeToString(&raw_value)) {
            return SERIALIZE_DBDATA_ERR;
        }

        assert(false == raw_value.empty());
        dbimpl::Record_t record = {0};
        assert(0 == info.cFlag);
        record.cFlag = info.cFlag;
        memcpy(record.sKey, &logid, sizeof(logid));
        record.cKeyLen = sizeof(logid);
        record.iVerA = info.iVersion;
        record.iVerB = info.iVersion;
        record.iValLen = raw_value.size();
        record.pVal = &raw_value[0];
        
        uint32_t fileno = 0;
        uint32_t offset = 0;
        ret = storage_->Add(
                &record, &fileno, &offset);
        if (0 != ret) {
            logerr("storage::Add key %lu ret %d", logid, ret);
            return ret;
        }

        assert(0 == ret);
        info.iFileNo = fileno;
        info.iOffset = offset;
        assert(0 < info.iFileNo); // *.w file
    }
    //
    // 2. write memkv
    ret = memkv_->Set(logid, info, plog);
    if (0 != ret) {
        logerr("memkv::Set key %lu ret %d", logid, ret);
        return ret;
    }

    assert(0 == ret);
    return 0;
}


void update_option(clsHardMemKv& hmemkv, Option& option)
{
    option.pfn_read = 
        [&](const std::string& key, 
            paxos::PaxosLogHeader& header, 
            paxos::PaxosLog& plog) -> int {
            return hmemkv.Read(key, header, plog);
        };

    option.pfn_read_header = 
        [&](const std::string& key, 
            paxos::PaxosLogHeader& header) -> int {
            return hmemkv.ReadHeader(key, header);
        };

    option.pfn_write = 
        [&](const std::string& key, 
            const paxos::PaxosLogHeader& header, 
            const paxos::PaxosLog& plog) -> int {
            return hmemkv.Write(key, header, plog);
        };
}




} // namespace paxoskv



