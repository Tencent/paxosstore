
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cassert>
#include "test_helper.h"
#include "pins_wrapper.h"
#include "plog_wrapper.h"
#include "paxos.pb.h"
#include "mem_utils.h"

using namespace paxos;

namespace test {

const uint8_t selfid = 1;
const uint64_t test_index = 1;
const uint64_t test_reqid = 1;
const uint64_t test_logid = 123;
const std::string test_value = "test@test.com";


void set_accepted_value(const Entry& entry, Message& msg)
{
    auto new_entry = msg.mutable_accepted_value();
    assert(nullptr != new_entry);
    *new_entry = entry;
}

void set_test_accepted_value(Message& msg)
{
    auto entry = msg.mutable_accepted_value();
    assert(nullptr != entry);
    entry->set_reqid(test_reqid);
    entry->set_data(test_value);
}

void set_test_accepted_value(PaxosInstance& pins_impl)
{
    auto entry = pins_impl.mutable_accepted_value();
    assert(nullptr != entry);
    entry->set_reqid(test_reqid);
    entry->set_data(test_value);
}

void set_diff_test_accepted_value(Message& msg)
{
    auto entry = msg.mutable_accepted_value();
    assert(nullptr != entry);
    entry->set_reqid(test_reqid + test_reqid);
    entry->set_data(test_value + test_value);
}


void check_test_value(const Entry& value)
{
    assert(value.reqid() == test_reqid);
    assert(value.data() == test_value);
}

void check_entry_equal(const Entry& a, const Entry& b)
{
    assert(a.reqid() == b.reqid());
    assert(a.data() == b.data());
}


std::unique_ptr<PInsAliveState> EmptyPInsState(uint64_t index)
{
    return cutils::make_unique<PInsAliveState>(
			test_logid, index, cutils::prop_num_compose(selfid, 0));
}

PaxosInstance EmptyPIns(uint64_t index)
{
    PaxosInstance pins_impl;
    pins_impl.set_index(index);
    pins_impl.set_proposed_num(0);
    assert(false == pins_impl.has_promised_num());
    assert(false == pins_impl.has_accepted_num());
    assert(false == pins_impl.has_accepted_value());
    return pins_impl;
}

std::tuple<std::unique_ptr<PInsAliveState>, PaxosInstance> 
WaitPropRsp()
{
    auto pins_state = EmptyPInsState(test_index);
	assert(nullptr != pins_state);
    auto pins_impl = EmptyPIns(test_index);
    
    Message begin_prop_msg;
	begin_prop_msg.set_logid(pins_state->GetLogID());
	begin_prop_msg.set_index(pins_state->GetIndex());
    begin_prop_msg.set_type(MessageType::BEGIN_PROP);
    set_test_accepted_value(begin_prop_msg);

    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;
    std::tie(write, rsp_msg_type) = 
        pins_state->Step(begin_prop_msg, pins_impl);
    return std::make_tuple(std::move(pins_state), pins_impl);
}

std::tuple<std::unique_ptr<PInsAliveState>, PaxosInstance>
WaitAccptRsp(uint64_t index)
{
    auto pins_state = EmptyPInsState(index);
	assert(nullptr != pins_state);
    auto pins_impl = EmptyPIns(index);

    Message begin_prop_msg;
	begin_prop_msg.set_logid(pins_state->GetLogID());
	begin_prop_msg.set_index(pins_state->GetIndex());
    begin_prop_msg.set_type(MessageType::BEGIN_PROP);
    set_test_accepted_value(begin_prop_msg);

    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;
    std::tie(write, rsp_msg_type) = 
        pins_state->Step(begin_prop_msg, pins_impl);
    assert(true == write);
    assert(MessageType::PROP == rsp_msg_type);
    
    Message prop_rsp_msg;
    prop_rsp_msg.set_type(MessageType::PROP_RSP);
	prop_rsp_msg.set_logid(pins_state->GetLogID());
	prop_rsp_msg.set_index(pins_state->GetIndex());
    prop_rsp_msg.set_from(2);
    prop_rsp_msg.set_proposed_num(pins_impl.proposed_num());
    prop_rsp_msg.set_promised_num(pins_impl.proposed_num());
    std::tie(write, rsp_msg_type) = 
        pins_state->Step(prop_rsp_msg, pins_impl);
    assert(true == write);
    assert(MessageType::ACCPT == rsp_msg_type);
    assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
    return std::make_tuple(std::move(pins_state), pins_impl);
}

std::tuple<std::unique_ptr<PInsAliveState>, PaxosInstance>
WaitFastAccptRsp()
{
    auto pins_state = EmptyPInsState(test_index);
	assert(nullptr != pins_state);
    auto pins_impl = EmptyPIns(test_index);

    Message bfast_prop_msg;
	bfast_prop_msg.set_logid(pins_state->GetLogID());
	bfast_prop_msg.set_index(pins_state->GetIndex());
    bfast_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
    set_test_accepted_value(bfast_prop_msg);

    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;
    std::tie(write, rsp_msg_type) = 
        pins_state->Step(bfast_prop_msg, pins_impl);
    assert(true == write);
    assert(MessageType::FAST_ACCPT == rsp_msg_type);
    assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
    return std::make_tuple(std::move(pins_state), pins_impl);
}

PaxosLog PLogOnlyChosen(uint64_t chosen_index)
{
    PaxosLog plog_impl;
    auto chosen_ins = plog_impl.mutable_chosen_ins();
    assert(nullptr != chosen_ins);
    chosen_ins->set_index(chosen_index);
    uint64_t proposed_num = cutils::prop_num_compose(selfid, 0);
    chosen_ins->set_proposed_num(proposed_num);
    chosen_ins->set_promised_num(proposed_num);
    chosen_ins->set_accepted_num(proposed_num);
    set_test_accepted_value(*chosen_ins);
    return plog_impl;
}

PaxosLog PLogWithPending(uint64_t pending_index)
{
    auto plog_impl = PLogOnlyChosen(1);
    auto pending_ins = plog_impl.mutable_pending_ins();
    assert(nullptr != pending_ins);
    assert(pending_index > plog_impl.chosen_ins().index());
    pending_ins->set_index(pending_index);
    return plog_impl;
}

std::vector<PaxosLog> VecPLogOnlyChosen(uint64_t chosen_index)
{
    std::vector<PaxosLog> vec_plog_impl;
    vec_plog_impl.resize(3);
    for (size_t i = 0; i < 3; ++i) {
        vec_plog_impl[i] = PLogOnlyChosen(chosen_index);
    }

    return vec_plog_impl;
}

std::map<uint8_t, PLogWrapper> MapPLogWrapper(std::vector<PaxosLog>& vec_plog)
{
    std::map<uint8_t, PLogWrapper> map_plog_wrapper;
    for (size_t i = 0; i < 3; ++i) {
        map_plog_wrapper.emplace(
                std::make_pair<
                    const uint8_t, PLogWrapper>(
                        i+1, PLogWrapper{
                            static_cast<uint8_t>(i+1), 
                            test_logid, nullptr, vec_plog[i]}));
    }

    return map_plog_wrapper;
}

} // namespace test
