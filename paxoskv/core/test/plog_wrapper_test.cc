
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <string>
#include "gtest/gtest.h"
#include "core/plog_wrapper.h"
#include "core/paxos.pb.h"
#include "core/pins_wrapper.h"
#include "core/plog_helper.h"
#include "test_helper.h"


using namespace paxos;
using namespace test;
using paxos::get_pending_ins;
using paxos::get_chosen_ins;
using paxos::get_chosen_index;

namespace {

bool is_promised(Message& msg)
{
    assert(MessageType::PROP_RSP == msg.type());
    assert(msg.has_promised_num());
    return msg.proposed_num() == msg.promised_num();
}

bool is_accepted(Message& msg)
{
    assert(MessageType::ACCPT_RSP == msg.type());
    assert(msg.has_accepted_num());
    return msg.proposed_num() == msg.accepted_num();
}

bool is_fast_accepted(Message& msg)
{
    assert(MessageType::FAST_ACCPT_RSP == msg.type());
    assert(msg.has_accepted_num());
    return msg.proposed_num() == msg.accepted_num();
}



} // namespace


TEST(PLogWrapperTest, SimpleConstruct)
{
    // case 1
    {
        PaxosLog plog_impl;
        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};

        assert(false == plog_wrapper.NeedDiskWrite());
    }

    // case 2
    {
        PaxosLog plog_impl = PLogWithPending(10);
        assert(paxos::has_pending_ins(plog_impl));

        auto chosen_ins = paxos::get_chosen_ins(plog_impl);
        assert(nullptr != chosen_ins);
        auto pending_ins = get_pending_ins(plog_impl);
        assert(nullptr != pending_ins);

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};

        assert(false == plog_wrapper.NeedDiskWrite());
    }
}

TEST(PLogWrapperTest, TestGetInstance)
{
    auto err = 0;
    std::unique_ptr<PInsWrapper> pins_wrapper = nullptr;

    // case 1
    {
        auto plog_impl = PLogOnlyChosen(1);
        assert(false == paxos::has_pending_ins(plog_impl));
        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};
        
        auto commited_index = get_chosen_index(plog_impl);
        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(pins_wrapper->IsChosen());
        assert(paxos::get_chosen_ins(plog_impl) == 
                pins_wrapper->TestGetPaxosInstance());

        assert(false == paxos::has_pending_ins(plog_impl));
        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index+1);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(get_chosen_index(plog_impl) == commited_index);
        assert(false == pins_wrapper->IsChosen());
        assert(paxos::has_pending_ins(plog_impl));
        assert(paxos::get_pending_ins(plog_impl) == 
                pins_wrapper->TestGetPaxosInstance());
        assert(paxos::get_pending_ins(plog_impl)->index() == commited_index+1);

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index+10);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(get_chosen_index(plog_impl) == commited_index);
        assert(false == pins_wrapper->IsChosen());
        assert(paxos::has_pending_ins(plog_impl));
        assert(get_pending_ins(plog_impl) == 
                pins_wrapper->TestGetPaxosInstance());
        assert(get_pending_ins(plog_impl)->index() == commited_index+10);

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index+1);
        assert(-1 == err);
        assert(nullptr == pins_wrapper);
        assert(get_chosen_index(plog_impl) == commited_index);
        assert(paxos::has_pending_ins(plog_impl));
        assert(get_pending_ins(plog_impl)->index() == commited_index+10);
    }

    // case 2
    {
        auto plog_impl = PLogWithPending(2);
        assert(1 == paxos::get_chosen_ins(plog_impl)->index());
        assert(paxos::has_pending_ins(plog_impl));
        assert(2 == get_pending_ins(plog_impl)->index());

        uint64_t promised_num = cutils::prop_num_compose(3, 10);
        {
            get_pending_ins(plog_impl)->set_promised_num(promised_num); 
        }

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(1);
        assert(0 == err);
        assert(nullptr != pins_wrapper);

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(2);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(pins_wrapper->TestGetPaxosInstance() == 
                get_pending_ins(plog_impl));
    }
}


TEST(PLogWrapperTest, TestGet)
{
    auto err = 0;
    std::string value;

    // case 1
    {
        auto plog_impl = PLogOnlyChosen(2);
        assert(false == paxos::has_pending_ins(plog_impl));
        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};

        std::tie(err, value) = paxos::get_value(plog_impl, 2);
        assert(0 == err);
        assert(value == paxos::get_chosen_ins(plog_impl)->accepted_value().data());

        std::tie(err, value) = paxos::get_value(plog_impl, 1);
        assert(0 > err);

        std::tie(err, value) = paxos::get_value(plog_impl, 3);
        assert(0 > err);
    }

    // case 2
    {
        auto plog_impl = PLogWithPending(3);
        assert(paxos::has_pending_ins(plog_impl));

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};

        for (uint64_t index = 1; index < 5; ++index) {
            std::tie(err, value) = paxos::get_value(plog_impl, index);
            if (2 == index) {
                assert(0 == err);
            }
            else {
                assert(0 > err);
            }
        }
    }
}

TEST(PLogWrapperTest, TestSimpleStep)
{
    auto err = 0;
    std::unique_ptr<Message> rsp_msg = nullptr;

    auto plog_impl = PLogWithPending(10);
    assert(paxos::has_pending_ins(plog_impl));

    PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};
    assert(false == plog_wrapper.NeedDiskWrite());
    // case 1: error case
    {
        Message msg;
        msg.set_to(selfid+1);
        std::tie(err, rsp_msg) = plog_wrapper.Step(msg);
        assert(-1 == err);

        msg.set_to(selfid);
        msg.set_index(0);
        std::tie(err, rsp_msg) = plog_wrapper.Step(msg);
        assert(-1 == err);

        msg.set_index(2);
        std::tie(err, rsp_msg) = plog_wrapper.Step(msg);
        assert(-1 == err);

        msg.set_key(paxos::to_paxos_key(test_logid));
        assert(msg.index() != paxos::get_chosen_ins(plog_impl)->index());
        assert(msg.index() != get_pending_ins(plog_impl)->index());
        std::tie(err, rsp_msg) = plog_wrapper.Step(msg);
        assert(0 == err);
        assert(nullptr == rsp_msg);
//        assert(MessageType::NOOP == rsp_msg->type());
//        assert(plog_impl.pending_ins().index() == rsp_msg->index());
    }
    
    // case 2: Message::NOOP
    {
        Message noop;
        noop.set_to(selfid);
        // for chosen ins
        noop.set_index(paxos::get_chosen_ins(plog_impl)->index());
        noop.set_key(paxos::to_paxos_key(test_logid));
        noop.set_type(MessageType::NOOP);
        std::tie(err, rsp_msg) = plog_wrapper.Step(noop);
        assert(0 == err);
        assert(nullptr == rsp_msg);
        assert(false == plog_wrapper.NeedDiskWrite());

        noop.set_index(get_pending_ins(plog_impl)->index());
        std::tie(err, rsp_msg) = plog_wrapper.Step(noop);
        assert(0 == err);
        assert(nullptr == rsp_msg);
        assert(false == plog_wrapper.NeedDiskWrite());
    }
    
    // case 3: anymsg: step chosen ins
    {
        Message anymsg;
        anymsg.set_index(paxos::get_chosen_ins(plog_impl)->index());
        anymsg.set_key(paxos::to_paxos_key(test_logid));
        anymsg.set_to(selfid);
        anymsg.set_from(2);
        
        anymsg.set_type(MessageType::CHOSEN);
        anymsg.set_proposed_num(paxos::get_chosen_ins(plog_impl)->proposed_num());
        std::tie(err, rsp_msg) = plog_wrapper.Step(anymsg);
        assert(0 == err);
        assert(nullptr == rsp_msg);
        assert(false == plog_wrapper.NeedDiskWrite());

        std::vector<MessageType> vec_msg_type = {
            ///MessageType::PROP_RSP, 
            ///MessageType::ACCPT_RSP, 
            ///MessageType::FAST_ACCPT_RSP, 
            MessageType::GET_CHOSEN, 
            MessageType::PROP, 
            MessageType::ACCPT, 
            MessageType::FAST_ACCPT, 
        };

        anymsg.set_proposed_num(
                cutils::PropNumGen(2, 0).Next(paxos::get_chosen_ins(plog_impl)->proposed_num()));
        for (auto msg_type : vec_msg_type) {
            anymsg.set_type(msg_type);
            std::tie(err, rsp_msg) = plog_wrapper.Step(anymsg);
            assert(0 == err);
            assert(nullptr!= rsp_msg);  
            assert(false == plog_wrapper.NeedDiskWrite());

            assert(MessageType::CHOSEN == rsp_msg->type());
            assert(anymsg.index() == rsp_msg->index());
            assert(anymsg.key() == rsp_msg->key());
            assert(selfid == rsp_msg->from());
            assert(anymsg.from() == rsp_msg->to());
            assert(rsp_msg->has_accepted_num());
            assert(paxos::get_chosen_ins(plog_impl)->accepted_num() == 
                    rsp_msg->accepted_num());
            assert(rsp_msg->has_accepted_value());
            check_entry_equal(rsp_msg->accepted_value(), 
                    paxos::get_chosen_ins(plog_impl)->accepted_value());
        }
    }
}

TEST(PLogWrapperTest, TestAcceptorStep)
{
    auto err = 0;
    std::unique_ptr<Message> rsp_msg = nullptr;

    // case 1: chosen msg
    {
        auto plog_impl = PLogWithPending(2);
        assert(paxos::has_pending_ins(plog_impl));
        assert(2 == get_pending_ins(plog_impl)->index());

        PLogWrapper plog_wrapper(selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl);
        assert(false == plog_wrapper.NeedDiskWrite());
        assert(false == plog_wrapper.NeedDiskWrite());
        
        Message chosen_msg;
        chosen_msg.set_type(MessageType::CHOSEN);
        chosen_msg.set_to(selfid);
        chosen_msg.set_key(paxos::to_paxos_key(test_logid));
        chosen_msg.set_from(2);
        chosen_msg.set_proposed_num(
                cutils::prop_num_compose(2, 0));
        set_test_accepted_value(chosen_msg);
        {
            auto pending_ins = get_pending_ins(plog_impl);
            assert(nullptr != pending_ins);
            assert(false == pending_ins->has_accepted_num());
            assert(false == pending_ins->has_accepted_value());
            chosen_msg.set_index(pending_ins->index());

            std::tie(err, rsp_msg) = plog_wrapper.Step(chosen_msg);
            assert(0 == err);
            assert(nullptr == rsp_msg);
        }
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(false == paxos::has_pending_ins(plog_impl));
        assert(chosen_msg.index() == paxos::get_chosen_ins(plog_impl)->index());
        assert(chosen_msg.index() == get_chosen_index(plog_impl));
        check_entry_equal(
                chosen_msg.accepted_value(), 
                paxos::get_chosen_ins(plog_impl)->accepted_value());
    }

    // case 2: prop msg
    {
        auto plog_impl = PLogOnlyChosen(1);
        assert(false == paxos::has_pending_ins(plog_impl));

        PLogWrapper plog_wrapper(selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl);
        assert(false == plog_wrapper.NeedDiskWrite());

        Message prop_msg;
        prop_msg.set_type(MessageType::PROP);
        prop_msg.set_index(3);
        prop_msg.set_key(paxos::to_paxos_key(test_logid));
        prop_msg.set_to(selfid);
        prop_msg.set_from(2);
        prop_msg.set_proposed_num(
                cutils::prop_num_compose(prop_msg.from(), 0));

        std::tie(err, rsp_msg) = plog_wrapper.Step(prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(paxos::has_pending_ins(plog_impl));
        assert(MessageType::PROP_RSP == rsp_msg->type());
        assert(prop_msg.proposed_num() == rsp_msg->proposed_num());
        assert(is_promised(*rsp_msg));
    }

    // case 3: accpt msg;
    {
        auto plog_impl = PLogWithPending(2);
        assert(paxos::has_pending_ins(plog_impl));

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};
        assert(false == plog_wrapper.NeedDiskWrite());

        Message accpt_msg;
        accpt_msg.set_type(MessageType::ACCPT);
        accpt_msg.set_index(2);
        accpt_msg.set_key(paxos::to_paxos_key(test_logid));
        accpt_msg.set_to(selfid);
        accpt_msg.set_from(2);
        accpt_msg.set_proposed_num(
                cutils::PropNumGen(2, 0).Next(get_pending_ins(plog_impl)->proposed_num()));
        set_test_accepted_value(accpt_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(accpt_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
        auto& pending_ins = *get_pending_ins(plog_impl);
        assert(pending_ins.has_promised_num());
        assert(accpt_msg.proposed_num() == pending_ins.promised_num());
        assert(pending_ins.has_accepted_num());
        assert(accpt_msg.proposed_num() == pending_ins.accepted_num());
        assert(pending_ins.has_accepted_value());
        check_entry_equal(
                pending_ins.accepted_value(), accpt_msg.accepted_value());
    }

    // case 4: fast accpt msg
    {
        auto plog_impl = PLogWithPending(2);
        assert(paxos::has_pending_ins(plog_impl));

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};
        assert(false == plog_wrapper.NeedDiskWrite());

        Message faccpt_msg;
        faccpt_msg.set_type(MessageType::FAST_ACCPT);
        faccpt_msg.set_index(2);
        faccpt_msg.set_key(paxos::to_paxos_key(test_logid));
        faccpt_msg.set_to(selfid);
        faccpt_msg.set_from(2);
        faccpt_msg.set_proposed_num(
                cutils::PropNumGen(2, 20).Next(get_pending_ins(plog_impl)->proposed_num()));
        set_test_accepted_value(faccpt_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(faccpt_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());

        auto& pending_ins = *get_pending_ins(plog_impl);
        assert(pending_ins.has_promised_num());
        assert(faccpt_msg.proposed_num() == pending_ins.promised_num());
        assert(pending_ins.has_accepted_num());
        assert(faccpt_msg.proposed_num() == pending_ins.accepted_num());
        assert(pending_ins.has_accepted_value());
        check_entry_equal(
                pending_ins.accepted_value(), faccpt_msg.accepted_value());
    }
}

TEST(PLogWrapperTest, TestProposerStep)
{
    auto err = 0;
    std::unique_ptr<Message> rsp_msg = nullptr;

    // case 1: nullptr pins_state
    {
        auto plog_impl = PLogWithPending(2);
        assert(paxos::has_pending_ins(plog_impl));

        PLogWrapper plog_wrapper(selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl);

        auto& pending_ins = *get_pending_ins(plog_impl);
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
        prop_rsp_msg.set_to(selfid);
        prop_rsp_msg.set_key(paxos::to_paxos_key(test_logid));
        prop_rsp_msg.set_index(pending_ins.index());
        std::tie(err, rsp_msg) = plog_wrapper.Step(prop_rsp_msg);
        assert(0 == err);
        assert(nullptr == rsp_msg);
    }

    // case 2: begin prop msg
    {
        auto pins_state = EmptyPInsState(2);
        auto plog_impl = PLogWithPending(2);

        PLogWrapper plog_wrapper(selfid, selfid, paxos::to_paxos_key(test_logid), pins_state.get(), plog_impl);

        auto& pending_ins = *get_pending_ins(plog_impl);
        Message begin_prop_msg;
        begin_prop_msg.set_type(MessageType::BEGIN_PROP);
        begin_prop_msg.set_index(pending_ins.index());
        begin_prop_msg.set_key(paxos::to_paxos_key(test_logid));
        begin_prop_msg.set_to(selfid);
        set_test_accepted_value(begin_prop_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(begin_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(MessageType::PROP == rsp_msg->type());
        assert(0 < rsp_msg->proposed_num());
        assert(pending_ins.has_promised_num());
        assert(0 == rsp_msg->to());
    }

    // case 3: try prop msg;
    {
        auto pins_state = EmptyPInsState(2);
        auto plog_impl = PLogWithPending(2);

        std::tie(pins_state, *get_pending_ins(plog_impl)) = WaitAccptRsp(2);
        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), pins_state.get(), plog_impl};

        auto& pending_ins = *get_pending_ins(plog_impl);
        Message try_prop_msg;
        try_prop_msg.set_type(MessageType::TRY_PROP);
        try_prop_msg.set_index(pending_ins.index());
        try_prop_msg.set_key(paxos::to_paxos_key(test_logid));
        try_prop_msg.set_to(selfid);
        set_test_accepted_value(try_prop_msg);
        try_prop_msg.mutable_accepted_value()->set_reqid(0);


        std::tie(err, rsp_msg) = plog_wrapper.Step(try_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(MessageType::PROP == rsp_msg->type());
        assert(0 < rsp_msg->proposed_num());
        assert(pending_ins.has_promised_num());
        assert(0 == rsp_msg->to());
    }

    // case 4: begin fast prop msg;
    {
        auto pins_state = EmptyPInsState(2);
        auto plog_impl = PLogWithPending(2);
        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), pins_state.get(), plog_impl};

        auto& pending_ins = *get_pending_ins(plog_impl);
        Message fbegin_prop_msg;
        fbegin_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
        fbegin_prop_msg.set_index(pending_ins.index());
        fbegin_prop_msg.set_key(paxos::to_paxos_key(test_logid));
        fbegin_prop_msg.set_to(selfid);
        set_test_accepted_value(fbegin_prop_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(fbegin_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(MessageType::FAST_ACCPT == rsp_msg->type());
        assert(0 < rsp_msg->proposed_num());
        assert(0 == rsp_msg->to());
    }

    // case 5: prop rsp msg, accpt_rsp
    {
        auto vec_plog_impl = VecPLogOnlyChosen(1);
        auto map_plog_wrapper = MapPLogWrapper(vec_plog_impl);

        auto pins_state = EmptyPInsState(3);
        {
            // add pending_ins
            auto pending_ins = vec_plog_impl[0].add_entries();
            assert(nullptr != pending_ins);
            pending_ins->set_index(3);
            map_plog_wrapper.at(selfid).TestSetPInsState(pins_state.get());
        }

        Message begin_prop_msg;
        begin_prop_msg.set_type(MessageType::BEGIN_PROP);
        begin_prop_msg.set_index(3);
        begin_prop_msg.set_key(paxos::to_paxos_key(test_logid));
        begin_prop_msg.set_to(selfid);
        set_test_accepted_value(begin_prop_msg);

        std::tie(err, rsp_msg) = map_plog_wrapper.at(selfid).Step(begin_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(0 == rsp_msg->to());

        std::vector<std::unique_ptr<Message>> vec_msg;
        for (uint8_t id : {2, 3}) { 
            rsp_msg->set_to(id);
            std::unique_ptr<Message> new_rsp_msg = nullptr;
            std::tie(err, new_rsp_msg) = map_plog_wrapper.at(id).Step(*rsp_msg);
            assert(0 == err);
            assert(MessageType::PROP_RSP == new_rsp_msg->type());
            assert(selfid == new_rsp_msg->to());
            assert(is_promised(*new_rsp_msg));
            vec_msg.emplace_back(std::move(new_rsp_msg));
        }

        assert(size_t{2} == vec_msg.size());
        std::tie(err, rsp_msg) = map_plog_wrapper.at(selfid).Step(*vec_msg[0]);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(MessageType::ACCPT == rsp_msg->type());
        assert(0 == rsp_msg->to());

        vec_msg.clear();
        for (uint8_t id : {2, 3}) {
            rsp_msg->set_to(id);
            std::unique_ptr<Message> new_rsp_msg = nullptr;
            std::tie(err, new_rsp_msg) = map_plog_wrapper.at(id).Step(*rsp_msg);
            assert(0 == err);
            assert(MessageType::ACCPT_RSP == new_rsp_msg->type());
            assert(selfid == new_rsp_msg->to());
            assert(is_accepted(*new_rsp_msg));
            vec_msg.emplace_back(std::move(new_rsp_msg));
        }

        assert(size_t{2} == vec_msg.size());
        std::tie(err, rsp_msg) = map_plog_wrapper.at(selfid).Step(*vec_msg[0]);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(MessageType::CHOSEN == rsp_msg->type());
        assert(0 == rsp_msg->to());
        assert(false == paxos::has_pending_ins(vec_plog_impl[0]));

        vec_msg.clear();
        for (uint8_t id : {2, 3}) {
            rsp_msg->set_to(id);
            std::unique_ptr<Message> new_rsp_msg = nullptr;
            assert(paxos::has_pending_ins(vec_plog_impl[id-1]));
            std::tie(err, new_rsp_msg) = map_plog_wrapper.at(id).Step(*rsp_msg);
            assert(0 == err);
            assert(nullptr == new_rsp_msg);
            assert(false == paxos::has_pending_ins(vec_plog_impl[id-1]));
        }
    }

    // case 6: fast rsp msg;
    {
        auto vec_plog_impl = VecPLogOnlyChosen(1);
        auto map_plog_wrapper = MapPLogWrapper(vec_plog_impl);

        auto pins_state = EmptyPInsState(3);
        {
            auto pending_ins = vec_plog_impl[0].add_entries();
            assert(nullptr != pending_ins);
            pending_ins->set_index(3);
            map_plog_wrapper.at(selfid).TestSetPInsState(pins_state.get());
        }

        Message fbegin_prop_msg;
        fbegin_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
        fbegin_prop_msg.set_index(3);
        fbegin_prop_msg.set_key(paxos::to_paxos_key(test_logid));
        fbegin_prop_msg.set_to(selfid);
        set_test_accepted_value(fbegin_prop_msg);

        std::tie(err, rsp_msg) = map_plog_wrapper.at(selfid).Step(fbegin_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(0 == rsp_msg->to());

        std::vector<std::unique_ptr<Message>> vec_msg;
        for (uint8_t id : {2, 3}) {
            rsp_msg->set_to(id);
            std::unique_ptr<Message> new_rsp_msg = nullptr;
            std::tie(err, new_rsp_msg) = map_plog_wrapper.at(id).Step(*rsp_msg);
            assert(0 == err);
            assert(MessageType::FAST_ACCPT_RSP == new_rsp_msg->type());
            assert(is_fast_accepted(*new_rsp_msg));
            vec_msg.emplace_back(std::move(new_rsp_msg));
        }

        assert(size_t{2} == vec_msg.size());
        std::tie(err, rsp_msg) = map_plog_wrapper.at(selfid).Step(*vec_msg[0]);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(0 == rsp_msg->to());
        assert(MessageType::CHOSEN == rsp_msg->type());
    }
}

TEST(PLogWrapperTest, TestSet)
{
    auto err = 0;
    std::unique_ptr<Message> rsp_msg = nullptr;

    // case 1: error case
    {
        auto plog_impl = PLogOnlyChosen(1);
        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), nullptr, plog_impl};

        std::shared_ptr<paxos::PInsAliveState> pins_state;
        std::tie(err, pins_state, rsp_msg) = plog_wrapper.Set(test_reqid, "", true);
        assert(0 == err);
        assert(nullptr != pins_state);
        assert(nullptr != rsp_msg);
        assert(2 == pins_state->GetIndex());

        plog_impl = PLogWithPending(2);
        plog_wrapper.ClearPInsAliveState();
        std::tie(err, pins_state, rsp_msg) = plog_wrapper.Set(test_reqid, "", true);;
        assert(0 == err);
        assert(nullptr != pins_state);
        assert(2 == pins_state->GetIndex());
        assert(nullptr != rsp_msg);
    }

    // case 2
    {
        auto plog_impl = PLogOnlyChosen(1);
        std::shared_ptr<paxos::PInsAliveState> pins_state;

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), pins_state.get(), plog_impl};

        paxos::get_max_ins(plog_impl)->mutable_accepted_value()->set_reqid(test_reqid);
        std::tie(err, pins_state, rsp_msg) = plog_wrapper.Set(test_reqid, "", true);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(MessageType::PROP == rsp_msg->type());
        assert(get_chosen_index(plog_impl) +1 == rsp_msg->index());
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
    }

    // case 3
    {
        auto plog_impl = PLogOnlyChosen(1);
        std::shared_ptr<paxos::PInsAliveState> pins_state;

        PLogWrapper plog_wrapper{selfid, selfid, paxos::to_paxos_key(test_logid), pins_state.get(), plog_impl};

        std::tie(err, pins_state, rsp_msg) = plog_wrapper.Set(test_reqid, "", true);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(MessageType::FAST_ACCPT == rsp_msg->type());
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_wrapper.NeedDiskWrite());
    }
}


