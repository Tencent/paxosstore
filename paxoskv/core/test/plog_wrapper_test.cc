
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <string>
#include "gtest/gtest.h"
#include "plog_wrapper.h"
#include "paxos.pb.h"
#include "pins_wrapper.h"
#include "test_helper.h"


using namespace paxos;
using namespace test;

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
        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};

        assert(0 == plog_wrapper.GetCommitedIndex());
        assert(false == plog_wrapper.NeedMemWrite());
        assert(false == plog_wrapper.NeedDiskWrite());
    }

    // case 2
    {
        PaxosLog plog_impl = PLogWithPending(10);
        assert(plog_impl.has_pending_ins());

        auto chosen_ins = plog_impl.mutable_chosen_ins();
        assert(nullptr != chosen_ins);
        auto pending_ins = plog_impl.mutable_pending_ins();
        assert(nullptr != pending_ins);

        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};

        assert(pending_ins->index() > plog_wrapper.GetCommitedIndex());
        assert(false == plog_wrapper.NeedMemWrite());
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
        assert(false == plog_impl.has_pending_ins());
        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};
        
        auto commited_index = plog_wrapper.GetCommitedIndex();
        assert(commited_index == plog_impl.chosen_ins().index());

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(pins_wrapper->IsChosen());
        assert(plog_impl.mutable_chosen_ins() == 
                pins_wrapper->TestGetPaxosInstance());

        assert(false == plog_impl.has_pending_ins());
        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index+1);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(plog_wrapper.GetCommitedIndex() == commited_index);
        assert(false == pins_wrapper->IsChosen());
        assert(plog_impl.has_pending_ins());
        assert(plog_impl.mutable_pending_ins() == 
                pins_wrapper->TestGetPaxosInstance());
        assert(plog_impl.pending_ins().index() == commited_index+1);

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index+10);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(plog_wrapper.GetCommitedIndex() == commited_index);
        assert(false == pins_wrapper->IsChosen());
        assert(plog_impl.has_pending_ins());
        assert(plog_impl.mutable_pending_ins() == 
                pins_wrapper->TestGetPaxosInstance());
        assert(plog_impl.pending_ins().index() == commited_index+10);

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(commited_index+1);
        assert(-1 == err);
        assert(nullptr == pins_wrapper);
        assert(plog_wrapper.GetCommitedIndex() == commited_index);
        assert(plog_impl.has_pending_ins());
        assert(plog_impl.pending_ins().index() == commited_index+10);
    }

    // case 2
    {
        auto plog_impl = PLogWithPending(2);
        assert(1 == plog_impl.chosen_ins().index());
        assert(plog_impl.has_pending_ins());
        assert(2 == plog_impl.pending_ins().index());

        uint64_t promised_num = cutils::prop_num_compose(3, 10);
        {
            plog_impl.mutable_pending_ins()->set_promised_num(promised_num); 
        }

        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(1);
        assert(0 == err);
        assert(nullptr != pins_wrapper);

        std::tie(err, pins_wrapper) = plog_wrapper.TestGetInstance(2);
        assert(0 == err);
        assert(nullptr != pins_wrapper);
        assert(pins_wrapper->TestGetPaxosInstance() == 
                plog_impl.mutable_pending_ins());
    }
}


TEST(PLogWrapperTest, TestGet)
{
    auto err = 0;
    std::string value;

    // case 1
    {
        auto plog_impl = PLogOnlyChosen(2);
        assert(false == plog_impl.has_pending_ins());
        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};

        std::tie(err, value) = plog_wrapper.Get(2);
        assert(0 == err);
        assert(value == plog_impl.chosen_ins().accepted_value().data());

        std::tie(err, value) = plog_wrapper.Get(1);
        assert(0 == err);
        assert(value == plog_impl.chosen_ins().accepted_value().data());

        std::tie(err, value) = plog_wrapper.Get(3);
        assert(-2 == err);
    }

    // case 2
    {
        auto plog_impl = PLogWithPending(3);
        assert(plog_impl.has_pending_ins());

        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};

        for (uint64_t index = 1; index < 5; ++index) {
            std::tie(err, value) = plog_wrapper.Get(index);
            assert(-1 == err);
        }
    }
}

TEST(PLogWrapperTest, TestSimpleStep)
{
    auto err = 0;
    std::unique_ptr<Message> rsp_msg = nullptr;

    auto plog_impl = PLogWithPending(10);
    assert(plog_impl.has_pending_ins());

    PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};
    assert(false == plog_wrapper.NeedMemWrite());
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

        msg.set_logid(test_logid);
        assert(msg.index() != plog_impl.chosen_ins().index());
        assert(msg.index() != plog_impl.pending_ins().index());
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
        noop.set_index(plog_impl.chosen_ins().index());
        noop.set_logid(test_logid);
        noop.set_type(MessageType::NOOP);
        std::tie(err, rsp_msg) = plog_wrapper.Step(noop);
        assert(0 == err);
        assert(nullptr == rsp_msg);
        assert(false == plog_wrapper.NeedMemWrite());

        noop.set_index(plog_impl.pending_ins().index());
        std::tie(err, rsp_msg) = plog_wrapper.Step(noop);
        assert(0 == err);
        assert(nullptr == rsp_msg);
        assert(false == plog_wrapper.NeedMemWrite());
    }
    
    // case 3: anymsg: step chosen ins
    {
        Message anymsg;
        anymsg.set_index(plog_impl.chosen_ins().index());
        anymsg.set_logid(test_logid);
        anymsg.set_to(selfid);
        anymsg.set_from(2);
        
        anymsg.set_type(MessageType::CHOSEN);
        anymsg.set_proposed_num(plog_impl.chosen_ins().proposed_num());
        std::tie(err, rsp_msg) = plog_wrapper.Step(anymsg);
        assert(0 == err);
        assert(nullptr == rsp_msg);
        assert(false == plog_wrapper.NeedMemWrite());

        std::vector<MessageType> vec_msg_type = {
            MessageType::PROP_RSP, 
            MessageType::ACCPT_RSP, 
            MessageType::FAST_ACCPT_RSP, 
            MessageType::PROP, 
            MessageType::ACCPT, 
            MessageType::FAST_ACCPT, 
        };

        anymsg.set_proposed_num(
                cutils::PropNumGen(2, 0).Next(plog_impl.chosen_ins().proposed_num()));
        for (auto msg_type : vec_msg_type) {
            anymsg.set_type(msg_type);
            std::tie(err, rsp_msg) = plog_wrapper.Step(anymsg);
            assert(0 == err);
            assert(nullptr!= rsp_msg);  
            assert(false == plog_wrapper.NeedMemWrite());

            assert(MessageType::CHOSEN == rsp_msg->type());
            assert(anymsg.index() == rsp_msg->index());
            assert(anymsg.logid() == rsp_msg->logid());
            assert(selfid == rsp_msg->from());
            assert(anymsg.from() == rsp_msg->to());
            assert(rsp_msg->has_accepted_num());
            assert(plog_impl.chosen_ins().accepted_num() == 
                    rsp_msg->accepted_num());
            assert(rsp_msg->has_accepted_value());
            check_entry_equal(rsp_msg->accepted_value(), 
                    plog_impl.chosen_ins().accepted_value());
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
        assert(plog_impl.has_pending_ins());
        assert(2 == plog_impl.pending_ins().index());

        PLogWrapper plog_wrapper(selfid, test_logid, nullptr, plog_impl);
        assert(false == plog_wrapper.NeedMemWrite());
        assert(false == plog_wrapper.NeedDiskWrite());
        
        Message chosen_msg;
        chosen_msg.set_type(MessageType::CHOSEN);
        chosen_msg.set_to(selfid);
        chosen_msg.set_logid(test_logid);
        chosen_msg.set_from(2);
        chosen_msg.set_proposed_num(
                cutils::prop_num_compose(2, 0));
        set_test_accepted_value(chosen_msg);
        {
            auto pending_ins = plog_impl.mutable_pending_ins();
            assert(nullptr != pending_ins);
            assert(false == pending_ins->has_accepted_num());
            assert(false == pending_ins->has_accepted_value());
            chosen_msg.set_index(pending_ins->index());

            std::tie(err, rsp_msg) = plog_wrapper.Step(chosen_msg);
            assert(0 == err);
            assert(nullptr == rsp_msg);
        }
        assert(plog_wrapper.NeedMemWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(false == plog_impl.has_pending_ins());
        assert(chosen_msg.index() == plog_impl.chosen_ins().index());
        assert(chosen_msg.index() == plog_wrapper.GetCommitedIndex());
        check_entry_equal(
                chosen_msg.accepted_value(), plog_impl.chosen_ins().accepted_value());
    }

    // case 2: prop msg
    {
        auto plog_impl = PLogOnlyChosen(1);
        assert(false == plog_impl.has_pending_ins());

        PLogWrapper plog_wrapper(selfid, test_logid, nullptr, plog_impl);
        assert(false == plog_wrapper.NeedMemWrite());

        Message prop_msg;
        prop_msg.set_type(MessageType::PROP);
        prop_msg.set_index(3);
        prop_msg.set_logid(test_logid);
        prop_msg.set_to(selfid);
        prop_msg.set_from(2);
        prop_msg.set_proposed_num(
                cutils::prop_num_compose(prop_msg.from(), 0));

        std::tie(err, rsp_msg) = plog_wrapper.Step(prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedMemWrite());
        assert(plog_wrapper.NeedDiskWrite());
        assert(plog_impl.has_pending_ins());
        assert(MessageType::PROP_RSP == rsp_msg->type());
        assert(prop_msg.proposed_num() == rsp_msg->proposed_num());
        assert(is_promised(*rsp_msg));
    }

    // case 3: accpt msg;
    {
        auto plog_impl = PLogWithPending(2);
        assert(plog_impl.has_pending_ins());

        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};
        assert(false == plog_wrapper.NeedMemWrite());

        Message accpt_msg;
        accpt_msg.set_type(MessageType::ACCPT);
        accpt_msg.set_index(2);
        accpt_msg.set_logid(test_logid);
        accpt_msg.set_to(selfid);
        accpt_msg.set_from(2);
        accpt_msg.set_proposed_num(
                cutils::PropNumGen(2, 0).Next(plog_impl.pending_ins().proposed_num()));
        set_test_accepted_value(accpt_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(accpt_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedMemWrite());
        assert(plog_wrapper.NeedDiskWrite());
        auto& pending_ins = plog_impl.pending_ins();
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
        assert(plog_impl.has_pending_ins());

        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};
        assert(false == plog_wrapper.NeedMemWrite());

        Message faccpt_msg;
        faccpt_msg.set_type(MessageType::FAST_ACCPT);
        faccpt_msg.set_index(2);
        faccpt_msg.set_logid(test_logid);
        faccpt_msg.set_to(selfid);
        faccpt_msg.set_from(2);
        faccpt_msg.set_proposed_num(
                cutils::PropNumGen(2, 20).Next(plog_impl.pending_ins().proposed_num()));
        set_test_accepted_value(faccpt_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(faccpt_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedMemWrite());
        assert(plog_wrapper.NeedDiskWrite());

        auto& pending_ins = plog_impl.pending_ins();
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
        assert(plog_impl.has_pending_ins());

        PLogWrapper plog_wrapper(selfid, test_logid, nullptr, plog_impl);

        auto& pending_ins = plog_impl.pending_ins();
        Message prop_rsp_msg;
        prop_rsp_msg.set_type(MessageType::PROP_RSP);
        prop_rsp_msg.set_to(selfid);
        prop_rsp_msg.set_logid(test_logid);
        prop_rsp_msg.set_index(pending_ins.index());
        std::tie(err, rsp_msg) = plog_wrapper.Step(prop_rsp_msg);
        assert(0 == err);
        assert(nullptr == rsp_msg);
    }

    // case 2: begin prop msg
    {
        auto pins_state = EmptyPInsState(2);
        auto plog_impl = PLogWithPending(2);

        PLogWrapper plog_wrapper(selfid, test_logid, pins_state.get(), plog_impl);

        auto& pending_ins = plog_impl.pending_ins();
        Message begin_prop_msg;
        begin_prop_msg.set_type(MessageType::BEGIN_PROP);
        begin_prop_msg.set_index(pending_ins.index());
        begin_prop_msg.set_logid(test_logid);
        begin_prop_msg.set_to(selfid);
        set_test_accepted_value(begin_prop_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(begin_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedMemWrite());
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

        std::tie(pins_state, *plog_impl.mutable_pending_ins()) = WaitAccptRsp(2);
        PLogWrapper plog_wrapper{selfid, test_logid, pins_state.get(), plog_impl};

        auto& pending_ins = plog_impl.pending_ins();
        Message try_prop_msg;
        try_prop_msg.set_type(MessageType::TRY_PROP);
        try_prop_msg.set_index(pending_ins.index());
        try_prop_msg.set_logid(test_logid);
        try_prop_msg.set_to(selfid);
        set_test_accepted_value(try_prop_msg);
        try_prop_msg.mutable_accepted_value()->set_reqid(0);


        std::tie(err, rsp_msg) = plog_wrapper.Step(try_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedMemWrite());
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
        PLogWrapper plog_wrapper{selfid, test_logid, pins_state.get(), plog_impl};

        auto& pending_ins = plog_impl.pending_ins();
        Message fbegin_prop_msg;
        fbegin_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
        fbegin_prop_msg.set_index(pending_ins.index());
        fbegin_prop_msg.set_logid(test_logid);
        fbegin_prop_msg.set_to(selfid);
        set_test_accepted_value(fbegin_prop_msg);

        std::tie(err, rsp_msg) = plog_wrapper.Step(fbegin_prop_msg);
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(plog_wrapper.NeedMemWrite());
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
            auto pending_ins = vec_plog_impl[0].mutable_pending_ins();
            assert(nullptr != pending_ins);
            pending_ins->set_index(3);
            map_plog_wrapper.at(selfid).TestSetPInsState(pins_state.get());
        }

        Message begin_prop_msg;
        begin_prop_msg.set_type(MessageType::BEGIN_PROP);
        begin_prop_msg.set_index(3);
        begin_prop_msg.set_logid(test_logid);
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
        assert(false == vec_plog_impl[0].has_pending_ins());

        vec_msg.clear();
        for (uint8_t id : {2, 3}) {
            rsp_msg->set_to(id);
            std::unique_ptr<Message> new_rsp_msg = nullptr;
            assert(vec_plog_impl[id-1].has_pending_ins());
            std::tie(err, new_rsp_msg) = map_plog_wrapper.at(id).Step(*rsp_msg);
            assert(0 == err);
            assert(nullptr == new_rsp_msg);
            assert(false == vec_plog_impl[id-1].has_pending_ins());
        }
    }

    // case 6: fast rsp msg;
    {
        auto vec_plog_impl = VecPLogOnlyChosen(1);
        auto map_plog_wrapper = MapPLogWrapper(vec_plog_impl);

        auto pins_state = EmptyPInsState(3);
        {
            auto pending_ins = vec_plog_impl[0].mutable_pending_ins();
            assert(nullptr != pending_ins);
            pending_ins->set_index(3);
            map_plog_wrapper.at(selfid).TestSetPInsState(pins_state.get());
        }

        Message fbegin_prop_msg;
        fbegin_prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
        fbegin_prop_msg.set_index(3);
        fbegin_prop_msg.set_logid(test_logid);
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
        PLogWrapper plog_wrapper{selfid, test_logid, nullptr, plog_impl};

        std::tie(err, rsp_msg) = plog_wrapper.Set(test_reqid, "");
        assert(-10 == err);

        auto pins_state = EmptyPInsState(2);
        assert(nullptr == plog_wrapper.SetPInsAliveState(pins_state.get()));
        plog_impl = PLogWithPending(2);
        std::tie(err, rsp_msg) = plog_wrapper.Set(test_reqid, "");;
        assert(ErrorCode::BUSY == err);
    }

    // case 2
    {
        auto plog_impl = PLogOnlyChosen(1);
        auto pins_state = EmptyPInsState(2);

        PLogWrapper plog_wrapper{selfid, test_logid, pins_state.get(), plog_impl};

        std::tie(err, rsp_msg) = plog_wrapper.Set(test_reqid, "");
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(MessageType::PROP == rsp_msg->type());
        assert(plog_wrapper.GetCommitedIndex() +1 == rsp_msg->index());
        assert(plog_wrapper.NeedMemWrite());
        assert(plog_wrapper.NeedDiskWrite());
    }

    // case 3
    {
        auto plog_impl = PLogOnlyChosen(1);
        auto pins_state = EmptyPInsState(2);
		assert(false == plog_impl.chosen_ins().is_fast());

        PLogWrapper plog_wrapper{selfid, test_logid, pins_state.get(), plog_impl};

		assert(false == plog_impl.chosen_ins().is_fast());
		plog_impl.mutable_chosen_ins()->set_is_fast(true);
        std::tie(err, rsp_msg) = plog_wrapper.Set(test_reqid, "");
        assert(0 == err);
        assert(nullptr != rsp_msg);
        assert(MessageType::FAST_ACCPT == rsp_msg->type());
        assert(plog_wrapper.NeedMemWrite());
        assert(plog_wrapper.NeedDiskWrite());
    }
}


