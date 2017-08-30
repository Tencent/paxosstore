
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <string>
#include <tuple>
#include <vector>
#include <map>
#include <memory>

namespace paxos {
    class Entry;
    class Message;
    class PaxosInstance;
    class PaxosLog;

    class PInsAliveState;
    class PInsWrapper;
    class PLogWrapper;
}

namespace test {

extern const uint8_t selfid;
extern const uint64_t test_index;
extern const uint64_t test_reqid;
extern const uint64_t test_logid;
extern const std::string test_value;

void set_key(paxos::Message& msg, uint64_t logid);


void set_accepted_value(const paxos::Entry& entry, paxos::Message& msg);

void set_test_accepted_value(paxos::Message& msg);

void set_test_accepted_value(paxos::PaxosInstance& pins_impl);

void set_diff_test_accepted_value(paxos::Message& msg);

void check_test_value(const paxos::Entry& value);

void check_entry_equal(const paxos::Entry& a, const paxos::Entry& b);

std::unique_ptr<paxos::PInsAliveState> EmptyPInsState(uint64_t index);

paxos::PaxosInstance EmptyPIns(uint64_t index);

std::tuple<std::unique_ptr<paxos::PInsAliveState>, paxos::PaxosInstance> WaitPropRsp();

std::tuple<std::unique_ptr<paxos::PInsAliveState>, paxos::PaxosInstance> WaitAccptRsp(uint64_t index);

std::tuple<std::unique_ptr<paxos::PInsAliveState>, paxos::PaxosInstance> WaitFastAccptRsp();

paxos::PaxosLog PLogOnlyChosen(uint64_t chosen_index);

paxos::PaxosLog PLogWithPending(uint64_t pending_index);

std::vector<paxos::PaxosLog> VecPLogOnlyChosen(uint64_t chosen_index);

std::map<uint8_t, paxos::PLogWrapper> MapPLogWrapper(std::vector<paxos::PaxosLog>& vec_plog);


} // namespace test
