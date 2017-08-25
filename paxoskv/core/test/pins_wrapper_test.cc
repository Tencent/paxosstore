
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <vector>
#include "gtest/gtest.h"
#include "pins_wrapper.h"
#include "id_utils.h"
#include "test_helper.h"


using namespace paxos;
using namespace test;

namespace {

Message SimpleReqMsg()
{
    Message msg;
    msg.set_index(test_index);
    msg.set_logid(test_logid);
    msg.set_from(2);
    msg.set_to(selfid);
    msg.set_proposed_num(
            cutils::prop_num_compose(2, 10));
    set_test_accepted_value(msg);
    return msg;
}

} // namespace


TEST(PInsAliveStateTest, SimpleConstruct)
{
    PInsAliveState pins_state(
			test_logid, test_index, 
			cutils::prop_num_compose(selfid, 1));
    assert(false == pins_state.IsChosen());
}


TEST(PInsAliveStateTest, BeginProp)
{
    Message begin_prop_msg;
    begin_prop_msg.set_type(MessageType::BEGIN_PROP);
    set_test_accepted_value(begin_prop_msg);

    bool write = false;
    MessageType rsp_msg_type = MessageType::NOOP;
    // case 1
    {
        auto pins_state = EmptyPInsState(1);
        auto pins_impl = EmptyPIns(1);

		assert(false == pins_impl.is_fast());
		begin_prop_msg.set_logid(pins_state->GetLogID());
		begin_prop_msg.set_index(pins_state->GetIndex());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(begin_prop_msg, pins_impl);
        assert(true == write);
        assert(MessageType::PROP == rsp_msg_type);

        assert(0 < pins_impl.proposed_num());
        assert(pins_impl.has_promised_num());
        assert(pins_impl.proposed_num() == pins_impl.promised_num());
        assert(false == pins_impl.has_accepted_num());
        assert(false == pins_impl.has_accepted_value());

        assert(false == pins_state->IsChosen());
		assert(pins_impl.is_fast());
        auto proposing_value = pins_state->TestProposingValue();
        assert(nullptr != proposing_value);
        assert(proposing_value->reqid() == 
                begin_prop_msg.accepted_value().reqid());
        assert(proposing_value->data() == 
                begin_prop_msg.accepted_value().data());
        assert(pins_state->TestProposedNum() == pins_impl.proposed_num());
        auto& rsp_votes = pins_state->TestRspVotes();
        assert(true == rsp_votes.empty());
    }

    // case 2
    {
        auto pins_state = EmptyPInsState(1);
        auto pins_impl = EmptyPIns(1);

        pins_impl.set_promised_num(cutils::prop_num_compose(2, 0));
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(begin_prop_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);

        assert(0 == pins_impl.proposed_num());
        assert(cutils::prop_num_compose(2, 0) == 
                pins_impl.promised_num());
        assert(false == pins_impl.has_accepted_num());
        assert(false == pins_impl.has_accepted_value());

        assert(false == pins_state->IsChosen());
		assert(false == pins_impl.is_fast());
        assert(nullptr == pins_state->TestProposingValue());
    }
}

TEST(PInsAliveStateTest, BeginFastProp)
{
    Message bfast_prop_msg;
    bfast_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
    set_test_accepted_value(bfast_prop_msg);

    bool write = false;
    MessageType rsp_msg_type = MessageType::NOOP;
    // case 1
    {
        auto pins_state = EmptyPInsState(1);
        auto pins_impl = EmptyPIns(1);

		bfast_prop_msg.set_logid(pins_state->GetLogID());
		bfast_prop_msg.set_index(pins_state->GetIndex());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(bfast_prop_msg, pins_impl);
        assert(true == write);
        assert(MessageType::FAST_ACCPT == rsp_msg_type);

        assert(0 < pins_impl.proposed_num());
        assert(pins_impl.has_promised_num());
        assert(pins_impl.proposed_num() == pins_impl.promised_num());
        assert(pins_impl.has_accepted_num());
        assert(pins_impl.proposed_num() == pins_impl.accepted_num());
        assert(pins_impl.has_accepted_value());
        check_entry_equal(
                pins_impl.accepted_value(), 
                bfast_prop_msg.accepted_value());

        assert(false == pins_state->IsChosen());
		assert(pins_impl.is_fast());
        auto proposing_value = pins_state->TestProposingValue();
        assert(nullptr != proposing_value);
        check_entry_equal(*proposing_value, 
                bfast_prop_msg.accepted_value());
        assert(pins_state->TestProposedNum() == pins_impl.proposed_num());
        auto& rsp_votes = pins_state->TestRspVotes();
        assert(true == rsp_votes.empty());
    }

    // case 2
    {
        auto pins_state = EmptyPInsState(1);
        auto pins_impl = EmptyPIns(1);

        pins_impl.set_promised_num(cutils::prop_num_compose(2, 0));
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(bfast_prop_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);

        assert(0 == pins_impl.proposed_num());
        assert(cutils::prop_num_compose(2, 0) == 
                pins_impl.promised_num());
        assert(false == pins_impl.has_accepted_num());
        assert(false == pins_impl.has_accepted_value());

        assert(false == pins_state->IsChosen());
		assert(false == pins_impl.is_fast());
        assert(nullptr == pins_state->TestProposingValue());
    }
}

TEST(PInsAliveStateTest, TryProp)
{
    Message try_prop_msg;
    try_prop_msg.set_type(MessageType::TRY_PROP);
    set_test_accepted_value(try_prop_msg);
    try_prop_msg.mutable_accepted_value()->set_reqid(0); // indicate try prop

    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;

    // case 1
    {
        auto pins_state = EmptyPInsState(test_index);
        auto pins_impl = EmptyPIns(test_index);

		try_prop_msg.set_logid(pins_state->GetLogID());
		try_prop_msg.set_index(pins_state->GetIndex());

        assert(PropState::NIL == pins_state->TestPropState());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(try_prop_msg, pins_impl);
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::PROP == rsp_msg_type);
		assert(false == pins_impl.is_fast());

        assert(pins_impl.has_promised_num());
        assert(pins_impl.proposed_num() == pins_impl.promised_num());
        assert(nullptr != pins_state->TestProposingValue());

        uint64_t proposed_num = pins_impl.proposed_num();
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
		prop_rsp_msg.set_logid(pins_state->GetLogID());
		prop_rsp_msg.set_index(pins_state->GetIndex());
        prop_rsp_msg.set_from(2);
        prop_rsp_msg.set_proposed_num(proposed_num);
        prop_rsp_msg.set_promised_num(proposed_num);
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::ACCPT == rsp_msg_type);

        assert(pins_impl.has_accepted_num());
        assert(pins_impl.accepted_num() == proposed_num);
        assert(pins_impl.has_accepted_value());
        assert(0 == pins_impl.accepted_value().reqid());
        assert(test_value == pins_impl.accepted_value().data());
    }

    // case 2 TryProp in WAIT_PREPARE state
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitPropRsp();

        // reject
        uint64_t proposed_num = pins_impl.proposed_num();
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
		prop_rsp_msg.set_logid(pins_state->GetLogID());
		prop_rsp_msg.set_index(pins_state->GetIndex());
        prop_rsp_msg.set_from(2);
        prop_rsp_msg.set_proposed_num(proposed_num);
        prop_rsp_msg.set_promised_num(
                cutils::PropNumGen(2, 0).Next(proposed_num));
        assert(prop_rsp_msg.promised_num() > prop_rsp_msg.proposed_num());

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(try_prop_msg, pins_impl);
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::PROP == rsp_msg_type);

        assert(proposed_num < pins_impl.proposed_num());
        assert(pins_state->TestRspVotes().empty());
        assert(nullptr != pins_state->TestProposingValue());
    }

    // case 3 TryProp in WAIT_ACCPT state (normal accpt)
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

        // reject
        uint64_t proposed_num = pins_impl.proposed_num();
        Message accpt_rsp_msg;
        accpt_rsp_msg.set_type(MessageType::ACCPT_RSP);
		accpt_rsp_msg.set_logid(pins_state->GetLogID());
		accpt_rsp_msg.set_index(pins_state->GetIndex());
        accpt_rsp_msg.set_from(2);
        accpt_rsp_msg.set_proposed_num(proposed_num);
        accpt_rsp_msg.set_accepted_num(
                cutils::PropNumGen(2, 0).Next(proposed_num));
        assert(accpt_rsp_msg.accepted_num() > accpt_rsp_msg.proposed_num());

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(accpt_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(try_prop_msg, pins_impl);
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::PROP == rsp_msg_type);

        assert(proposed_num < pins_impl.proposed_num());
        assert(accpt_rsp_msg.accepted_num() < pins_impl.proposed_num());
        assert(pins_state->TestRspVotes().empty());
        assert(nullptr != pins_state->TestProposingValue());
    }

    // case 4 TryPropo in WAIT_ACCPT state (fast accpt)
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitFastAccptRsp();

        // reject
        uint64_t proposed_num = pins_impl.proposed_num();
        Message faccpt_rsp_msg;
        faccpt_rsp_msg.set_type(MessageType::FAST_ACCPT_RSP);
		faccpt_rsp_msg.set_logid(pins_state->GetLogID());
		faccpt_rsp_msg.set_index(pins_state->GetIndex());
        faccpt_rsp_msg.set_from(2);
        faccpt_rsp_msg.set_proposed_num(proposed_num);
        faccpt_rsp_msg.set_accepted_num(
                cutils::PropNumGen(2, 0).Next(proposed_num));
        assert(faccpt_rsp_msg.accepted_num() > proposed_num);

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(faccpt_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(try_prop_msg, pins_impl);
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::PROP == rsp_msg_type);
        assert(proposed_num < pins_impl.proposed_num());
    }
}

TEST(PInsAliveStateTest, FastAccptRsp)
{
    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;

    // case 1
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitFastAccptRsp();

        uint64_t proposed_num = pins_impl.proposed_num();
        Message faccpt_rsp_msg;
        faccpt_rsp_msg.set_type(MessageType::FAST_ACCPT_RSP);
		faccpt_rsp_msg.set_logid(pins_state->GetLogID());
		faccpt_rsp_msg.set_index(pins_state->GetIndex());
        faccpt_rsp_msg.set_from(2);
        faccpt_rsp_msg.set_proposed_num(proposed_num);
        faccpt_rsp_msg.set_accepted_num(proposed_num);
        // accepted

        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(faccpt_rsp_msg, pins_impl);
        assert(PropState::CHOSEN == pins_state->TestPropState());
        assert(false == write);
        assert(MessageType::CHOSEN == rsp_msg_type);
        assert(pins_state->IsChosen());
        assert(nullptr == pins_state->TestProposingValue());
        assert(pins_state->TestRspVotes().empty());
        assert(test_reqid == pins_impl.accepted_value().reqid());
        assert(test_value == pins_impl.accepted_value().data());
		assert(pins_impl.is_fast());
    }

    // case 2 ignore
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitFastAccptRsp();

        Message faccpt_rsp_msg;
        faccpt_rsp_msg.set_type(MessageType::FAST_ACCPT_RSP);
		faccpt_rsp_msg.set_logid(pins_state->GetLogID());
		faccpt_rsp_msg.set_index(pins_state->GetIndex());
        faccpt_rsp_msg.set_from(2);
        faccpt_rsp_msg.set_proposed_num(
                cutils::prop_num_compose(2, 0));

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(faccpt_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(pins_state->TestRspVotes().empty());
		assert(pins_impl.is_fast());
    }

    // case 3 reject
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitFastAccptRsp();

		assert(pins_impl.is_fast());
        uint64_t proposed_num = pins_impl.proposed_num();
        Message faccpt_rsp_msg;
        faccpt_rsp_msg.set_type(MessageType::FAST_ACCPT_RSP);
		faccpt_rsp_msg.set_logid(pins_state->GetLogID());
		faccpt_rsp_msg.set_index(pins_state->GetIndex());
        faccpt_rsp_msg.set_from(2);
        faccpt_rsp_msg.set_proposed_num(proposed_num);
        faccpt_rsp_msg.set_accepted_num(
                cutils::PropNumGen(2, 0).Next(proposed_num));
        assert(faccpt_rsp_msg.accepted_num() > proposed_num);

        // reject
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(faccpt_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());
		assert(pins_impl.is_fast());

        auto pins_state_bak = pins_state->TestClone();
        auto pins_impl_bak = pins_impl;
        auto faccpt_rsp_msg_bak = faccpt_rsp_msg;
        {
            // reject
            faccpt_rsp_msg.set_from(3);
            assert(faccpt_rsp_msg.accepted_num() > faccpt_rsp_msg.proposed_num());

            assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
            std::tie(write, rsp_msg_type) = 
                pins_state->Step(faccpt_rsp_msg, pins_impl);
            assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
            assert(true == write);
            assert(MessageType::PROP == rsp_msg_type);
            assert(pins_state->TestRspVotes().empty());
			assert(false == pins_impl.is_fast());
        }

        pins_state = std::move(pins_state_bak);
        pins_impl = pins_impl_bak;
        faccpt_rsp_msg = faccpt_rsp_msg_bak;
		assert(pins_impl.is_fast());

        // redundance reject msg
        assert(faccpt_rsp_msg.accepted_num() > faccpt_rsp_msg.proposed_num());

        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(faccpt_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(size_t{1} == pins_state->TestRspVotes().size());
		assert(pins_impl.is_fast());

        {
            // accpt
            faccpt_rsp_msg.set_from(3);
            faccpt_rsp_msg.set_accepted_num(faccpt_rsp_msg.proposed_num());

            assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
            std::tie(write, rsp_msg_type) = 
                pins_state->Step(faccpt_rsp_msg, pins_impl);
            assert(PropState::CHOSEN == pins_state->TestPropState());
            assert(false == write);
            assert(MessageType::CHOSEN == rsp_msg_type);
            assert(pins_state->IsChosen());
            assert(nullptr == pins_state->TestProposingValue());
			assert(pins_impl.is_fast());
        }
    }

    // case 4 reject with accepted_num == 0 <=> promoised_num > proposed_num
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

        uint64_t proposed_num = pins_impl.proposed_num();
        Message faccpt_rsp_msg;
        faccpt_rsp_msg.set_type(MessageType::FAST_ACCPT_RSP);
		faccpt_rsp_msg.set_logid(pins_state->GetLogID());
		faccpt_rsp_msg.set_index(pins_state->GetIndex());
        faccpt_rsp_msg.set_from(2);
        faccpt_rsp_msg.set_proposed_num(proposed_num);
        // 0 accepted_num
        faccpt_rsp_msg.set_accepted_num(0);
        faccpt_rsp_msg.set_promised_num(
                cutils::PropNumGen(3, 0).Next(proposed_num));

        // reject
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(faccpt_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());
        assert(faccpt_rsp_msg.promised_num() == pins_state->TestMaxHintNum());
    }
}


TEST(PInsAliveStateTest, AccptRsp)
{
    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;

    // case 1
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

        uint64_t proposed_num = pins_impl.proposed_num();
        Message accpt_rsp_msg;
        accpt_rsp_msg.set_type(MessageType::ACCPT_RSP);
		accpt_rsp_msg.set_logid(pins_state->GetLogID());
		accpt_rsp_msg.set_index(pins_state->GetIndex());
        accpt_rsp_msg.set_from(2);
        accpt_rsp_msg.set_proposed_num(proposed_num);
        accpt_rsp_msg.set_accepted_num(proposed_num);
        // accepted

        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(accpt_rsp_msg, pins_impl);
        assert(PropState::CHOSEN == pins_state->TestPropState());
        assert(false == write);
        assert(MessageType::CHOSEN == rsp_msg_type);
        assert(pins_state->IsChosen());
        assert(nullptr == pins_state->TestProposingValue());
        assert(pins_state->TestRspVotes().empty());
        assert(test_reqid == pins_impl.accepted_value().reqid());
        assert(test_value == pins_impl.accepted_value().data());
		assert(pins_impl.is_fast());

        // pins_state mark as chosen, nomore msg!!
    }

    // case 2
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

        Message accpt_rsp_msg;
        accpt_rsp_msg.set_type(MessageType::ACCPT_RSP);
		accpt_rsp_msg.set_logid(pins_state->GetLogID());
		accpt_rsp_msg.set_index(pins_state->GetIndex());
        accpt_rsp_msg.set_from(2);
        accpt_rsp_msg.set_proposed_num(
                cutils::prop_num_compose(2, 0));
        accpt_rsp_msg.set_accepted_num(accpt_rsp_msg.proposed_num());
        assert(accpt_rsp_msg.proposed_num() != pins_impl.proposed_num());

        // ingore
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(accpt_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(false == pins_state->IsChosen());
        assert(pins_state->TestRspVotes().empty());
		assert(pins_impl.is_fast());
    }

    // case 3 reject
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

		assert(pins_impl.is_fast());
        uint64_t proposed_num = pins_impl.proposed_num();
        Message accpt_rsp_msg;
        accpt_rsp_msg.set_type(MessageType::ACCPT_RSP);
		accpt_rsp_msg.set_logid(pins_state->GetLogID());
		accpt_rsp_msg.set_index(pins_state->GetIndex());
        accpt_rsp_msg.set_from(2);
        accpt_rsp_msg.set_proposed_num(proposed_num);
        accpt_rsp_msg.set_accepted_num(
                cutils::PropNumGen(2, 0).Next(proposed_num));
        assert(accpt_rsp_msg.accepted_num() > proposed_num);

        // reject
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(accpt_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());
		assert(pins_impl.is_fast());

        auto pins_state_bak = pins_state->TestClone();
        auto pins_impl_bak = pins_impl;
        auto accpt_rsp_msg_bak = accpt_rsp_msg;
        {
            // reject 
            accpt_rsp_msg.set_from(3);
            assert(accpt_rsp_msg.accepted_num() > 
                    accpt_rsp_msg.proposed_num());
            assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
            std::tie(write, rsp_msg_type) = 
                pins_state->Step(accpt_rsp_msg, pins_impl);
            assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
            assert(true == write);
            assert(MessageType::PROP == rsp_msg_type);
            assert(pins_state->TestRspVotes().empty());
			assert(false == pins_impl.is_fast());
        }

        pins_state = std::move(pins_state_bak);
        pins_impl = pins_impl_bak;
        accpt_rsp_msg = accpt_rsp_msg_bak;
		assert(pins_impl.is_fast());
        {
            // accpt
            accpt_rsp_msg.set_from(3);
            accpt_rsp_msg.set_accepted_num(accpt_rsp_msg.proposed_num());
            assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
            std::tie(write, rsp_msg_type) = 
                pins_state->Step(accpt_rsp_msg, pins_impl);
            assert(PropState::CHOSEN == pins_state->TestPropState());
            assert(false == write);
            assert(MessageType::CHOSEN == rsp_msg_type);
            assert(pins_state->IsChosen());
            assert(nullptr == pins_state->TestProposingValue());
            assert(pins_state->TestRspVotes().empty());
            assert(test_reqid == pins_impl.accepted_value().reqid());
            assert(test_value == pins_impl.accepted_value().data());
			assert(pins_impl.is_fast());
        }
    }

    // case 4 reject with accepted_num == 0 <=> promised_num
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

        uint64_t proposed_num = pins_impl.proposed_num();
        Message accpt_rsp_msg;
        accpt_rsp_msg.set_type(MessageType::ACCPT_RSP);
		accpt_rsp_msg.set_logid(pins_state->GetLogID());
		accpt_rsp_msg.set_index(pins_state->GetIndex());
        accpt_rsp_msg.set_from(2);
        accpt_rsp_msg.set_proposed_num(proposed_num);
        // 0 = > indicate a reject
        accpt_rsp_msg.set_accepted_num(0);
        // => reject by promised_num
        accpt_rsp_msg.set_promised_num(
                cutils::PropNumGen(3, 0).Next(proposed_num));
        assert(accpt_rsp_msg.promised_num() > accpt_rsp_msg.proposed_num());

        // reject
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(accpt_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(size_t{1} == pins_state->TestRspVotes().size());
        assert(accpt_rsp_msg.promised_num() == pins_state->TestMaxHintNum());
    }
}


TEST(PInsAliveStateTest, PropRsp)
{
    bool write = false;
    auto rsp_msg_type = MessageType::NOOP;
    // case 1
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitPropRsp();

        uint64_t proposed_num = pins_impl.proposed_num();
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
		prop_rsp_msg.set_logid(pins_state->GetLogID());
		prop_rsp_msg.set_index(pins_state->GetIndex());
        prop_rsp_msg.set_from(2);
        prop_rsp_msg.set_proposed_num(proposed_num);
        // promised
        prop_rsp_msg.set_promised_num(proposed_num);
        assert(false == prop_rsp_msg.has_accepted_num());

        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(false == pins_impl.has_accepted_num());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::ACCPT == rsp_msg_type);
        assert(pins_impl.has_accepted_num());
        assert(pins_impl.has_accepted_value());
        check_entry_equal(*(pins_state->TestProposingValue()), 
                pins_impl.accepted_value());

        // reject will be ignore
        prop_rsp_msg.set_from(3);
        prop_rsp_msg.set_promised_num(
                cutils::prop_num_compose(2, 1));
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(pins_state->TestRspVotes().empty());

		assert(pins_impl.is_fast());
        //assert(pins_state->GetStrictPropFlag());
    }

    // case 2 ignore
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitPropRsp();

        uint64_t proposed_num = cutils::prop_num_compose(1, 10);
        assert(proposed_num != pins_impl.proposed_num());
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
		prop_rsp_msg.set_logid(pins_state->GetLogID());
		prop_rsp_msg.set_index(pins_state->GetIndex());
        prop_rsp_msg.set_from(2);
        prop_rsp_msg.set_proposed_num(proposed_num);
        prop_rsp_msg.set_promised_num(proposed_num);

        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(false == pins_impl.has_accepted_num());
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(true == pins_state->TestRspVotes().empty());
		assert(pins_impl.is_fast());
        // assert(pins_state->GetStrictPropFlag());
    }

    // case 3 reject
    {
        // auto pins_state = EmptyPInsState();
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitPropRsp();
		assert(pins_impl.is_fast());

        uint64_t proposed_num = pins_impl.proposed_num();
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
		prop_rsp_msg.set_logid(pins_state->GetLogID());
		prop_rsp_msg.set_index(pins_state->GetIndex());
        prop_rsp_msg.set_from(2);
        prop_rsp_msg.set_proposed_num(proposed_num);
        prop_rsp_msg.set_promised_num(
                cutils::prop_num_compose(2, 10));
        assert(false == prop_rsp_msg.has_accepted_num());
        assert(prop_rsp_msg.promised_num() > proposed_num);

        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(false == write);
        assert(MessageType::NOOP == rsp_msg_type);
        assert(false == pins_impl.has_accepted_num());
        assert(size_t{1} == pins_state->TestRspVotes().size());
		assert(pins_impl.is_fast());

        // case 3.1
        auto pins_state_bak = pins_state->TestClone();
        auto pins_impl_bak = pins_impl;
        auto prop_rsp_msg_bak = prop_rsp_msg;
        {
            prop_rsp_msg.set_from(3);
            assert(prop_rsp_msg.promised_num() > 
                    pins_impl.proposed_num());
            // reject
            assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
            std::tie(write, rsp_msg_type) =
                pins_state->Step(prop_rsp_msg, pins_impl);
            assert(true == write);
            assert(MessageType::PROP == rsp_msg_type);
            assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
            assert(true == pins_state->TestRspVotes().empty());
			assert(false == pins_impl.is_fast());
            // assert(pins_state->GetStrictPropFlag());;
        }

        // case 3.2
        pins_state = std::move(pins_state_bak);
        pins_impl = pins_impl_bak;
        prop_rsp_msg = prop_rsp_msg_bak;
		assert(pins_impl.is_fast());
        {
            prop_rsp_msg.set_from(3);
            prop_rsp_msg.set_promised_num(prop_rsp_msg.proposed_num());
            // promised
            assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
            std::tie(write, rsp_msg_type) = 
                pins_state->Step(prop_rsp_msg, pins_impl);
            assert(true == write);
            assert(MessageType::ACCPT == rsp_msg_type);
            assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
            assert(true == pins_state->TestRspVotes().empty());

            assert(pins_impl.has_accepted_num());
            assert(pins_impl.has_accepted_value());
			assert(pins_impl.is_fast());
            //assert(pins_state->GetStrictPropFlag());
        }
    }

    // case 4 promised with accepted value
    {
		std::unique_ptr<paxos::PInsAliveState> pins_state;
        PaxosInstance pins_impl;
        std::tie(pins_state, pins_impl) = WaitPropRsp();

        // assert(pins_state->GetStrictPropFlag());
		assert(pins_impl.is_fast());
        uint64_t proposed_num = pins_impl.proposed_num();
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
		prop_rsp_msg.set_logid(pins_state->GetLogID());
		prop_rsp_msg.set_index(pins_state->GetIndex());
        prop_rsp_msg.set_from(2);
        prop_rsp_msg.set_proposed_num(proposed_num);
        prop_rsp_msg.set_promised_num(proposed_num);
        prop_rsp_msg.set_accepted_num(
                cutils::prop_num_compose(2, 0));
        assert(prop_rsp_msg.accepted_num() < proposed_num);
        set_diff_test_accepted_value(prop_rsp_msg);

        assert(PropState::WAIT_PREPARE == pins_state->TestPropState());
        assert(false == pins_impl.has_accepted_num());
        std::tie(write, rsp_msg_type) = 
            pins_state->Step(prop_rsp_msg, pins_impl);
        assert(PropState::WAIT_ACCEPT == pins_state->TestPropState());
        assert(true == write);
        assert(MessageType::ACCPT == rsp_msg_type);
        assert(pins_impl.has_accepted_num());
        assert(proposed_num == pins_impl.accepted_num());
        assert(pins_impl.has_accepted_value());
        check_entry_equal(pins_impl.accepted_value(), 
                prop_rsp_msg.accepted_value());
        assert(nullptr != pins_state->TestProposingValue());
        assert(test_reqid != pins_state->TestProposingValue()->reqid());
        assert(prop_rsp_msg.accepted_num() == 
                pins_state->TestMaxAcceptedHintNum());
		assert(pins_impl.is_fast());
        // assert(false == pins_state->GetStrictPropFlag());
    }
}

namespace {

PaxosInstance FullPIns()
{
    PaxosInstance pins_impl;
    pins_impl.set_index(test_index);
    auto proposed_num = cutils::prop_num_compose(1, 0);
    pins_impl.set_proposed_num(proposed_num);
    pins_impl.set_promised_num(proposed_num);
    pins_impl.set_accepted_num(proposed_num);
    {
        auto entry = pins_impl.mutable_accepted_value();
        assert(nullptr != entry);
        entry->set_reqid(test_reqid);
        entry->set_data(test_value);
    }
    return pins_impl;
}

} // namespace

TEST(PInsWrapperTest, SimpleConstruct)
{
    // not chosen
    {
        PaxosInstance pins_impl;
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);
        assert(false == pins_wrapper.IsChosen());
    }

    // chosen
    {
        PaxosInstance pins_impl;
        PInsWrapper pins_wrapper(true, nullptr, pins_impl);
        assert(true == pins_wrapper.IsChosen());
    }
}

TEST(PInsWrapperTest, ChosenStepMsg)
{
    PaxosInstance pins_impl = FullPIns();
    PInsWrapper pins_wrapper(true, nullptr, pins_impl);
    assert(true == pins_wrapper.IsChosen());

    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;
    // case 1
    {
        Message req_msg;
        req_msg.set_type(MessageType::CHOSEN);
        req_msg.set_index(pins_impl.index());

        std::tie(write, rsp_msg) = pins_wrapper.Step(req_msg);
        assert(false == write);
        assert(nullptr == rsp_msg);
    }

    // case 2
    {
        std::vector<MessageType> vec_msg_type = {
            MessageType::PROP_RSP, 
            MessageType::ACCPT_RSP, 
            MessageType::FAST_ACCPT_RSP, 
            MessageType::PROP, 
            MessageType::ACCPT, 
            MessageType::FAST_ACCPT, 
        };
        assert(false == vec_msg_type.empty());
        for (auto msg_type : vec_msg_type) {

            Message req_msg;
            req_msg.set_type(msg_type);
            req_msg.set_index(pins_impl.index());
            req_msg.set_logid(test_logid);
            req_msg.set_from(2);
            req_msg.set_to(selfid);

            std::tie(write, rsp_msg) = pins_wrapper.Step(req_msg);
            assert(false == write);
            assert(nullptr != rsp_msg);

            assert(MessageType::CHOSEN == rsp_msg->type());
            assert(rsp_msg->index() == pins_impl.index());
            assert(rsp_msg->logid() == test_logid);
            assert(rsp_msg->to() == 2);
            assert(rsp_msg->from() == selfid);
            assert(rsp_msg->has_promised_num());
            assert(rsp_msg->has_accepted_num());
            assert(rsp_msg->has_accepted_value());
            check_entry_equal(
                    rsp_msg->accepted_value(), pins_impl.accepted_value());
        }
    }
}

TEST(PInsWrapperTest, NotChosenStepChosenMsg)
{
    PaxosInstance pins_impl = FullPIns();

    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    Message chosen_msg;
    chosen_msg.set_type(MessageType::CHOSEN);
    chosen_msg.set_index(pins_impl.index());
    chosen_msg.set_proposed_num(pins_impl.accepted_num());
    set_accepted_value(pins_impl.accepted_value(), chosen_msg);

    Message nmchosen_msg;
    nmchosen_msg.set_type(MessageType::CHOSEN);
    nmchosen_msg.set_index(pins_impl.index());
    nmchosen_msg.set_proposed_num(
            cutils::prop_num_compose(2, 0));
    set_diff_test_accepted_value(nmchosen_msg);


    // case 1 nullptr == pins_state
    {
        // case 1.1 accepted_num match
        {
            PInsWrapper pins_wrapper(false, nullptr, pins_impl);
            assert(false == pins_wrapper.IsChosen());

            std::tie(write, rsp_msg) = pins_wrapper.Step(chosen_msg);
            assert(false == write);
            assert(nullptr == rsp_msg);
            assert(pins_wrapper.IsChosen());
        }

        // case 1.2 accepted_num don't match
        {
            pins_impl = FullPIns();
            PInsWrapper pins_wrapper(false, nullptr, pins_impl);
            assert(false == pins_wrapper.IsChosen());

            auto old_proposed_num = pins_impl.proposed_num();
            std::tie(write, rsp_msg) = pins_wrapper.Step(nmchosen_msg);
            assert(true == write);
            assert(nullptr == rsp_msg);
            assert(pins_wrapper.IsChosen());
            check_entry_equal(
                    nmchosen_msg.accepted_value(), pins_impl.accepted_value());
            assert(nmchosen_msg.accepted_num() <= pins_impl.proposed_num());
            assert(old_proposed_num < pins_impl.proposed_num());
            assert(pins_impl.proposed_num() == pins_impl.promised_num());
            assert(pins_impl.proposed_num() == pins_impl.accepted_num());
        }
    }

    // case 2 nullptr != pins_state 
    {
        
        // case 2.1
        {
            pins_impl = FullPIns(); // reset

            auto pins_state = EmptyPInsState(pins_impl.index());
            PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);
            assert(false == pins_wrapper.IsChosen());
            assert(false == pins_state->IsChosen());

            auto old_proposed_num = pins_impl.proposed_num();
            std::tie(write, rsp_msg) = pins_wrapper.Step(chosen_msg);
            assert(false == write);
            assert(nullptr == rsp_msg);
            assert(pins_wrapper.IsChosen());
            assert(pins_state->IsChosen());
            assert(old_proposed_num == pins_impl.proposed_num());
        }

        // case 2.2
        {
            pins_impl = FullPIns();
            auto pins_state = EmptyPInsState(pins_impl.index());
            PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);

            auto old_proposed_num = pins_impl.proposed_num();
            std::tie(write, rsp_msg) = pins_wrapper.Step(nmchosen_msg);
            assert(true == write);
            assert(nullptr == rsp_msg);
            assert(pins_wrapper.IsChosen());
            check_entry_equal(
                    nmchosen_msg.accepted_value(), pins_impl.accepted_value());
            assert(nmchosen_msg.accepted_num() <= pins_impl.proposed_num());
            assert(old_proposed_num < pins_impl.proposed_num());
            assert(pins_impl.proposed_num() == pins_impl.promised_num());
            assert(pins_impl.proposed_num() == pins_impl.accepted_num());
        }
    }
}

TEST(PInsWrapperTest, NotChosenStepPropMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    Message prop_msg;
    prop_msg.set_type(MessageType::PROP);
    prop_msg.set_index(test_index);
    prop_msg.set_proposed_num(
            cutils::prop_num_compose(2, 10));
    prop_msg.set_logid(test_logid);
    prop_msg.set_from(2);
    prop_msg.set_to(selfid);

    // case 1 promised => empty stat
    {
        auto pins_impl = EmptyPIns(test_index);
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);
        
        assert(false == pins_impl.has_promised_num());
        std::tie(write, rsp_msg) = pins_wrapper.Step(prop_msg);
        assert(true == write);
        assert(nullptr != rsp_msg);
        assert(pins_impl.has_promised_num());
        assert(pins_impl.promised_num() == prop_msg.proposed_num());

        assert(MessageType::PROP_RSP == rsp_msg->type());
        assert(prop_msg.proposed_num() == rsp_msg->proposed_num());
        // indicate => promised
        assert(prop_msg.proposed_num() == rsp_msg->promised_num());
        assert(prop_msg.index() == rsp_msg->index());
        assert(prop_msg.logid() == rsp_msg->logid());
        assert(prop_msg.to() == rsp_msg->from());
        assert(prop_msg.from() == rsp_msg->to());
    }

    // case 2 promised => not empty stat
    {
        auto pins_impl = EmptyPIns(test_index);
        pins_impl.set_promised_num(
                cutils::prop_num_compose(3, 1));
        assert(pins_impl.promised_num() < prop_msg.proposed_num());
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        uint64_t prev_promised_num = pins_impl.promised_num();
        std::tie(write, rsp_msg) = pins_wrapper.Step(prop_msg);
        assert(true == write);
        assert(nullptr != rsp_msg);
        assert(prev_promised_num < pins_impl.promised_num());
        assert(pins_impl.promised_num() == prop_msg.proposed_num());

        assert(MessageType::PROP_RSP == rsp_msg->type());
        assert(prop_msg.proposed_num() == rsp_msg->proposed_num());
        // indicate => promised
        assert(prop_msg.proposed_num() == rsp_msg->promised_num());
    }

    // case 3 reject
    {
        auto pins_impl = EmptyPIns(test_index);
        pins_impl.set_promised_num(
                cutils::PropNumGen(3, 0).Next(prop_msg.proposed_num()));
        assert(pins_impl.promised_num() > prop_msg.proposed_num());
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        uint64_t prev_promised_num = pins_impl.promised_num();
        std::tie(write, rsp_msg) = pins_wrapper.Step(prop_msg);
        assert(false == write);
        assert(nullptr != rsp_msg);
        assert(prev_promised_num == pins_impl.promised_num());

        assert(MessageType::PROP_RSP == rsp_msg->type());
        assert(prop_msg.proposed_num() == rsp_msg->proposed_num());
        assert(prev_promised_num == rsp_msg->promised_num());
    }
}


TEST(PInsWrapperTest, NotChosenStepAccptMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    Message accpt_msg = SimpleReqMsg();
    accpt_msg.set_type(MessageType::ACCPT);

    // case 1 accepted => empty stat
    {
        auto pins_impl = EmptyPIns(test_index);
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(accpt_msg);
        assert(true == write);
        assert(nullptr != rsp_msg);
        assert(pins_impl.has_promised_num());
        assert(pins_impl.promised_num() == accpt_msg.proposed_num());
        assert(pins_impl.has_accepted_num());
        assert(pins_impl.accepted_num() == accpt_msg.proposed_num());
        assert(pins_impl.has_accepted_value());
        check_entry_equal(
                pins_impl.accepted_value(), accpt_msg.accepted_value());

        assert(MessageType::ACCPT_RSP == rsp_msg->type());
        assert(accpt_msg.proposed_num() == rsp_msg->proposed_num());
        assert(accpt_msg.proposed_num() == rsp_msg->accepted_num());
        assert(false == rsp_msg->has_promised_num());
        assert(accpt_msg.index() == rsp_msg->index());
        assert(accpt_msg.logid() == rsp_msg->logid());
        assert(accpt_msg.to() == rsp_msg->from());
        assert(accpt_msg.from() == rsp_msg->to());
    }

    // case 2: accepted => not empty stat
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = cutils::prop_num_compose(3, 1);
        assert(old_proposed_num < accpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        pins_impl.set_accepted_num(old_proposed_num);
        {
            auto entry = pins_impl.mutable_accepted_value();
            assert(nullptr != entry);
            entry->set_reqid(test_reqid + test_reqid);
            entry->set_data(test_value + test_value);
        }
        assert(pins_impl.accepted_value().reqid() != 
                accpt_msg.accepted_value().reqid());
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(accpt_msg);
        assert(true == write);
        assert(nullptr != rsp_msg);
        assert(old_proposed_num < pins_impl.promised_num());
        assert(accpt_msg.proposed_num() == pins_impl.promised_num());
        assert(accpt_msg.proposed_num() == pins_impl.accepted_num()); 
        check_entry_equal(accpt_msg.accepted_value(), pins_impl.accepted_value());
    }

    // case 3. reject
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = 
            cutils::PropNumGen(3, 0).Next(accpt_msg.proposed_num());
        assert(old_proposed_num > accpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        assert(false == pins_impl.has_accepted_num());
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(accpt_msg);
        assert(false == write);
        assert(nullptr != rsp_msg);
        assert(MessageType::ACCPT_RSP == rsp_msg->type());
        assert(old_proposed_num == pins_impl.promised_num());
        assert(false == pins_impl.has_accepted_num());
        assert(false == pins_impl.has_accepted_value());

        // not equal accepted_num => reject
        assert(rsp_msg->proposed_num() != rsp_msg->accepted_num());
        assert(0 == rsp_msg->accepted_num());
        assert(rsp_msg->has_promised_num());
    }

    // case 4. reject
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = 
            cutils::PropNumGen(3, 0).Next(accpt_msg.proposed_num());
        assert(old_proposed_num > accpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        pins_impl.set_accepted_num(old_proposed_num);
        {
            auto entry = pins_impl.mutable_accepted_value();
            assert(nullptr != entry);
            entry->set_reqid(test_reqid + test_reqid);
            entry->set_data(test_value + test_value);
        }
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(accpt_msg);
        assert(false == write);
        assert(nullptr != rsp_msg);
        assert(MessageType::ACCPT_RSP == rsp_msg->type());
        assert(old_proposed_num == pins_impl.accepted_num());

        assert(rsp_msg->proposed_num() < rsp_msg->accepted_num());
        assert(false == rsp_msg->has_promised_num());
    }
}


TEST(PInsWrapperTest, NotChosenStepFastAccptMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    auto faccpt_msg = SimpleReqMsg();
    faccpt_msg.set_type(MessageType::FAST_ACCPT);

    // case 1: fast accpt => empty stat
    {
        auto pins_impl = EmptyPIns(test_index);
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_msg);
        assert(true == write);
        assert(nullptr != rsp_msg);
        assert(pins_impl.has_promised_num());
        assert(pins_impl.promised_num() == faccpt_msg.proposed_num());
        assert(pins_impl.has_accepted_num());
        assert(pins_impl.accepted_num() == faccpt_msg.proposed_num());
        assert(pins_impl.has_accepted_value());
        check_entry_equal(
                pins_impl.accepted_value(), faccpt_msg.accepted_value());

        assert(MessageType::FAST_ACCPT_RSP == rsp_msg->type());
        assert(faccpt_msg.proposed_num() == rsp_msg->proposed_num());
        assert(faccpt_msg.proposed_num() == rsp_msg->accepted_num());
        assert(false == rsp_msg->has_promised_num());
    }

    // case 2: fast accpt => not empty stat : only promised
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = cutils::prop_num_compose(3, 0);
        assert(old_proposed_num < faccpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_msg);
        assert(true == write);
        assert(nullptr != rsp_msg);
        assert(old_proposed_num < pins_impl.promised_num());
        assert(pins_impl.has_accepted_num());
        assert(old_proposed_num < pins_impl.accepted_num());
    }

    // case 3: fast accpt => reject because local accepted before
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = cutils::prop_num_compose(3, 0);
        assert(old_proposed_num < faccpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        pins_impl.set_accepted_num(old_proposed_num);
        {
            auto entry = pins_impl.mutable_accepted_value();
            assert(nullptr != entry);
            entry->set_reqid(test_reqid + test_reqid);
            entry->set_data(test_value + test_value);
        }
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_msg);
        assert(false == write);
        assert(nullptr != rsp_msg);
        assert(MessageType::FAST_ACCPT_RSP == rsp_msg->type());
        assert(pins_impl.accepted_num() == rsp_msg->accepted_num());
        assert(false == rsp_msg->has_promised_num());
        assert(faccpt_msg.proposed_num() != rsp_msg->accepted_num());
    }

    // case 4: reject => not accepted, but promised high proposed_num
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = 
            cutils::PropNumGen(3, 0).Next(faccpt_msg.proposed_num());
        assert(old_proposed_num > faccpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        assert(false == pins_impl.has_accepted_num());
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_msg);
        assert(false == write);
        assert(nullptr != rsp_msg);
        assert(MessageType::FAST_ACCPT_RSP == rsp_msg->type());

        assert(0 == rsp_msg->accepted_num());
        assert(rsp_msg->has_promised_num());
        assert(old_proposed_num == rsp_msg->promised_num());
    }

    // case 5: reject => accepted large propsed num
    {
        auto pins_impl = EmptyPIns(test_index);
        uint64_t old_proposed_num = 
            cutils::PropNumGen(3, 0).Next(faccpt_msg.proposed_num());
        assert(old_proposed_num > faccpt_msg.proposed_num());
        pins_impl.set_promised_num(old_proposed_num);
        pins_impl.set_accepted_num(old_proposed_num);
        {
            auto entry = pins_impl.mutable_accepted_value();
            assert(nullptr != entry);
            entry->set_reqid(test_reqid + test_reqid);
            entry->set_data(test_value + test_value);
        }
        PInsWrapper pins_wrapper(false, nullptr, pins_impl);

        std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_msg);
        assert(false == write);
        assert(nullptr != rsp_msg);
        assert(MessageType::FAST_ACCPT_RSP == rsp_msg->type());

        assert(false == rsp_msg->has_promised_num());
        assert(old_proposed_num == rsp_msg->accepted_num());
    }
}

TEST(PInsWrapperTest, NotChosenStepBeginPropMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    Message begin_prop_msg;
    begin_prop_msg.set_type(MessageType::BEGIN_PROP);
    begin_prop_msg.set_logid(test_logid);
    begin_prop_msg.set_index(test_index);
    begin_prop_msg.set_to(selfid);
    set_test_accepted_value(begin_prop_msg);

    auto pins_impl = EmptyPIns(test_index);
    auto pins_state = EmptyPInsState(pins_impl.index());
    PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);

    std::tie(write, rsp_msg) = pins_wrapper.Step(begin_prop_msg);
    assert(true == write);
    assert(nullptr != rsp_msg);
    assert(0 < pins_impl.proposed_num());
    assert(pins_impl.has_promised_num());
    assert(pins_impl.promised_num() == pins_impl.proposed_num());

    assert(MessageType::PROP == rsp_msg->type());
    assert(test_logid == rsp_msg->logid());
    assert(test_index == rsp_msg->index());
    assert(0 == rsp_msg->to()); // broad-cast
    assert(rsp_msg->proposed_num() == pins_impl.proposed_num());
}

TEST(PInsWrapperTest, NotChosenStepBeginFastPropMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    Message fbegin_prop_msg;
    fbegin_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
    fbegin_prop_msg.set_logid(test_logid);
    fbegin_prop_msg.set_index(test_index);
    fbegin_prop_msg.set_to(selfid);
    set_test_accepted_value(fbegin_prop_msg);

    auto pins_impl = EmptyPIns(test_index);
    auto pins_state = EmptyPInsState(pins_impl.index());
    PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);

    std::tie(write, rsp_msg) = pins_wrapper.Step(fbegin_prop_msg);
    assert(true == write);
    assert(nullptr != rsp_msg);
    assert(0 < pins_impl.proposed_num());
    assert(pins_impl.has_promised_num());
    assert(pins_impl.has_accepted_num());
    check_entry_equal(
            fbegin_prop_msg.accepted_value(), pins_impl.accepted_value());

    assert(MessageType::FAST_ACCPT == rsp_msg->type());
    assert(test_logid == rsp_msg->logid());
    assert(test_index == rsp_msg->index());
    assert(0 == rsp_msg->to());
    assert(rsp_msg->proposed_num() == pins_impl.proposed_num());
    assert(rsp_msg->has_accepted_value());
    check_entry_equal(
            fbegin_prop_msg.accepted_value(), rsp_msg->accepted_value());
}


TEST(PInsWrapperTest, NotChosenStepTryPropMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

    Message try_prop_msg;
    try_prop_msg.set_type(MessageType::TRY_PROP);
    try_prop_msg.set_logid(test_logid);
    try_prop_msg.set_index(test_index);
    try_prop_msg.set_to(selfid);
    set_test_accepted_value(try_prop_msg);
    try_prop_msg.mutable_accepted_value()->set_reqid(0);
    assert(0 == try_prop_msg.accepted_value().reqid());

    auto pins_impl = EmptyPIns(test_index);
    auto pins_state = EmptyPInsState(pins_impl.index());
    PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);

    std::tie(write, rsp_msg) = pins_wrapper.Step(try_prop_msg);
    assert(true == write);
    assert(nullptr != rsp_msg);
    assert(0 < pins_impl.proposed_num());
    assert(pins_impl.has_promised_num());
    assert(false == pins_impl.has_accepted_num());

    assert(MessageType::PROP == rsp_msg->type());
    assert(test_logid == rsp_msg->logid());
    assert(test_index == rsp_msg->index());
    assert(0 == rsp_msg->to());
    assert(rsp_msg->proposed_num() == pins_impl.proposed_num());
}

TEST(PInsWrapperTest, NotChosenStepPropRspMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

	std::unique_ptr<paxos::PInsAliveState> pins_state;
    PaxosInstance pins_impl;
    std::tie(pins_state, pins_impl) = WaitPropRsp();

    PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);
    Message prop_rsp_msg;
    prop_rsp_msg.set_type(MessageType::PROP_RSP);
    prop_rsp_msg.set_logid(test_logid);
    prop_rsp_msg.set_index(test_index);
    prop_rsp_msg.set_from(2);
    prop_rsp_msg.set_to(selfid);
    prop_rsp_msg.set_proposed_num(pins_impl.proposed_num());

    // case 1
    prop_rsp_msg.set_promised_num(
            cutils::PropNumGen(3, 0).Next(pins_impl.proposed_num()));
    assert(prop_rsp_msg.promised_num() > pins_impl.proposed_num());

    std::tie(write, rsp_msg) = pins_wrapper.Step(prop_rsp_msg);
    assert(false == write);
    assert(nullptr == rsp_msg);

    // case 2
    prop_rsp_msg.set_from(3);
    prop_rsp_msg.set_promised_num(prop_rsp_msg.proposed_num());
    
    std::tie(write, rsp_msg) = pins_wrapper.Step(prop_rsp_msg);
    assert(true == write);
    assert(nullptr != rsp_msg);
    assert(MessageType::ACCPT == rsp_msg->type());
    assert(test_logid == rsp_msg->logid());
    assert(test_index == rsp_msg->index());
    assert(selfid == rsp_msg->from());
    assert(0 == rsp_msg->to());
    assert(pins_impl.proposed_num() == rsp_msg->proposed_num());
    assert(pins_impl.has_accepted_value());
}

TEST(PInsWrapperTest, NotChosenStepAccptRspMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

	std::unique_ptr<paxos::PInsAliveState> pins_state;
    PaxosInstance pins_impl;
    std::tie(pins_state, pins_impl) = WaitAccptRsp(1);

    PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);
    Message accpt_rsp_msg;
    accpt_rsp_msg.set_type(MessageType::ACCPT_RSP);
    accpt_rsp_msg.set_logid(test_logid);
    accpt_rsp_msg.set_index(test_index);
    accpt_rsp_msg.set_from(2);
    accpt_rsp_msg.set_to(selfid);
    accpt_rsp_msg.set_proposed_num(pins_impl.proposed_num());

    // case 1
    accpt_rsp_msg.set_accepted_num(0);
    accpt_rsp_msg.set_promised_num(
            cutils::PropNumGen(3, 0).Next(pins_impl.proposed_num()));
    assert(accpt_rsp_msg.promised_num() > pins_impl.proposed_num());

    std::tie(write, rsp_msg) = pins_wrapper.Step(accpt_rsp_msg);
    assert(false == write);
    assert(nullptr == rsp_msg);

    // case 2
    accpt_rsp_msg.set_from(3);
    accpt_rsp_msg.clear_promised_num();
    accpt_rsp_msg.set_accepted_num(accpt_rsp_msg.proposed_num());
    assert(accpt_rsp_msg.proposed_num() == pins_impl.proposed_num());
    std::tie(write, rsp_msg) = pins_wrapper.Step(accpt_rsp_msg);
    assert(false == write); // mark chosen, but not need write disk
    assert(nullptr != rsp_msg);
    assert(MessageType::CHOSEN == rsp_msg->type());
    assert(pins_wrapper.IsChosen());
    assert(pins_state->IsChosen());
    assert(test_logid == rsp_msg->logid());
    assert(test_index == rsp_msg->index());
    assert(selfid == rsp_msg->from());
    assert(0 == rsp_msg->to());
    assert(rsp_msg->has_accepted_value());
}

TEST(PInsWrapperTest, NotChosenStepFastAccptRspMsg)
{
    bool write = false;
    std::unique_ptr<Message> rsp_msg = nullptr;

	std::unique_ptr<paxos::PInsAliveState> pins_state;
    PaxosInstance pins_impl;
    std::tie(pins_state, pins_impl) = WaitFastAccptRsp();

    PInsWrapper pins_wrapper(false, pins_state.get(), pins_impl);
    Message faccpt_rsp_msg;
    faccpt_rsp_msg.set_type(MessageType::FAST_ACCPT_RSP);
    faccpt_rsp_msg.set_logid(test_logid);
    faccpt_rsp_msg.set_index(test_index);
    faccpt_rsp_msg.set_from(2);
    faccpt_rsp_msg.set_to(selfid);
    faccpt_rsp_msg.set_proposed_num(pins_impl.proposed_num());

    // case 1
    faccpt_rsp_msg.set_accepted_num(0);
    faccpt_rsp_msg.set_promised_num(
            cutils::PropNumGen(3, 0).Next(pins_impl.proposed_num()));
    assert(faccpt_rsp_msg.promised_num() > pins_impl.proposed_num());

    std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_rsp_msg);
    assert(false == write);
    assert(nullptr == rsp_msg);

    // case 2
    faccpt_rsp_msg.set_from(3);
    faccpt_rsp_msg.clear_promised_num();
    faccpt_rsp_msg.set_accepted_num(faccpt_rsp_msg.proposed_num());
    assert(faccpt_rsp_msg.proposed_num() == pins_impl.proposed_num());

    std::tie(write, rsp_msg) = pins_wrapper.Step(faccpt_rsp_msg);
    assert(false == write);
    assert(nullptr != rsp_msg);
    assert(MessageType::CHOSEN == rsp_msg->type());
    assert(pins_wrapper.IsChosen());
    assert(pins_state->IsChosen());
    assert(test_logid == rsp_msg->logid());
    assert(test_index == rsp_msg->index());
    assert(selfid == rsp_msg->from());
    assert(0 == rsp_msg->to());
    assert(rsp_msg->has_accepted_value());
}

