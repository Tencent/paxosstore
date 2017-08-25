
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <string>
#include <memory>
#include <functional>
#include <stdint.h>
#include "cutils/id_utils.h"
#include "db_option.h"

namespace paxos {

class Message;
class PInsAliveState;
class PaxosLogHeader;
class PaxosLog;
class PLogWrapper;
class SmartPaxosMsgSender;

} // namespace paxos;

namespace dbcomm {

class HashBaseLock;

} // namespace dbcomm

namespace cutils {

class IDGenerator;

} // namespace cutils


namespace paxoskv {

class PInsAliveStateTimeoutCache;
class clsGetLocalOutHelper;

struct Option;


class DBImpl {

public:
    DBImpl(uint16_t member_id, Option& option);

    ~DBImpl();

    int Init(
            paxos::SmartPaxosMsgSender* msg_sender);

    int Get(
            const std::string& key, 
            uint64_t other_max_index, 
            uint8_t peer_status, 
            std::string& value, 
            uint32_t& version);

    int Set(
            const std::string& key, 
            const std::string& value, 
            const uint32_t prev_version, 
            uint64_t& forward_reqid, 
            uint32_t& new_version);

    int PostPaxosMsg(const paxos::Message& msg);

    int BatchTriggerCatchUpOn(
            const std::vector<std::string>& keys, 
            const std::vector<uint8_t>& peers);

    int TryRedo(
            const std::string& key, 
            uint64_t max_index, 
            bool is_read_redo);


    void MayTriggerACatchUp(
            int ret_code, 
            const std::string& key, 
            uint64_t max_index);

    void TriggerACatchUpOn(const std::string& key);

    int StartGetLocalOutWorker();
    int StartTimeoutWorker();

private:
    int CheckReqID(
            const std::string& key, 
            uint64_t set_on_index, 
            uint64_t reqid);

    int SetNoLock(
            const std::string& key, 
            uint64_t reqid, 
            const std::string& value, 
            const uint32_t* prev_version, 
            uint32_t& new_version, 
            std::shared_ptr<paxos::PInsAliveState>& alive_state);


    int DoWriteNoLock(
            const paxos::PLogWrapper& wrapper, 
            const paxos::PaxosLogHeader& header, 
            const paxos::PaxosLog& plog);

    int SetNoLockNoWrite(
            const std::string& key, 
            uint64_t reqid, 
            const std::string& value, 
            const uint32_t* prev_version, 
            uint32_t& new_version, 
            paxos::PaxosLogHeader& header, 
            paxos::PaxosLog& plog, 
            std::unique_ptr<paxos::PLogWrapper>& wrapper, 
            std::shared_ptr<paxos::PInsAliveState>& alive_state, 
            std::unique_ptr<paxos::Message>& rsp_msg);

    int PostPaxosMsgNoLock(
            const paxos::Message& req_msg, 
            std::unique_ptr<paxos::Message>& rsp_msg);


private:
    const uint16_t member_id_ = 0; 
    Option option_;

    paxos::SmartPaxosMsgSender* msg_sender_ = nullptr;
    std::unique_ptr<clsGetLocalOutHelper> localout_helper_;

    std::unique_ptr<cutils::IDGenerator> idgen_;

    std::unique_ptr<dbcomm::HashBaseLock> hashbase_lock_;

    std::unique_ptr<PInsAliveStateTimeoutCache> alive_state_cache_;
}; // class DBImpl


} // namespace paxoskv


