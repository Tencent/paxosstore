
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <unistd.h>
#include <stdint.h>
#include <tuple>
#include <map>
#include <string>
#include <memory>
#include <chrono>
#include "cutils/id_utils.h"
#include "paxos.pb.h"


namespace paxos {

// private
enum class PropState : uint8_t {
    NIL = 0, 
    PREPARE = 1, 
    WAIT_PREPARE = 2, 
    ACCEPT = 3, 
    WAIT_ACCEPT = 4, 
    CHOSEN = 5, 
	
	PROP_FROZEN = 6, 

	// RELOAD_
};

enum {
	MAX_PROP_CNT = 3, 
};

class PInsAliveState {

    // test interface
public:
    const Entry* TestProposingValue() const {
        return proposing_value_.get();
    }

    uint64_t TestProposedNum() const {
        return prop_num_gen_.Get();
    }

    const std::map<uint8_t, bool> TestRspVotes() const {
        return rsp_votes_;
    }

    PropState TestPropState() const {
        return prop_state_;
    }

    uint64_t TestMaxAcceptedHintNum() const {
        return max_accepted_hint_num_;
    }

    uint64_t TestMaxHintNum() const {
        return max_hint_num_;
    }

	std::unique_ptr<PInsAliveState> TestClone();

    // copy construct for test:
    PInsAliveState(const PInsAliveState& other) = delete;

    PInsAliveState& operator=(const PInsAliveState& other) = delete;

    // end of test interface
public:

    PInsAliveState(
            const std::string& key, 
			uint64_t index, 
			uint64_t proposed_num);

	~PInsAliveState();

    std::tuple<bool, MessageType> 
        Step(const Message& msg, PaxosInstance& pins_impl);

    bool HasProposingValue() const {
        return nullptr != proposing_value_;
    }

	std::unique_ptr<paxos::Entry> ClearProposingValue() {
		auto proposing_value = std::move(proposing_value_);
		assert(nullptr == proposing_value_);
		return proposing_value;
	}

    bool IsChosen() const {
        return PropState::CHOSEN == prop_state_;
    }

    void MarkChosen();

    int GetNotifyFD() const {
        assert(0 <= pipes_[0]);
        assert(0 <= pipes_[1]);
        return pipes_[0];
    }

	void SendNotify() const;

	PropState GetPropState() const {
		return prop_state_;
	}

	uint64_t GetIndex() const {
		return index_;
	}

    const std::string& GetKey() const {
        return key_;
    }
	bool IsLocalProposeNum(uint64_t prop_num) const {
		return prop_num_gen_.IsLocalNum(prop_num);
	}

	uint64_t GetProposedNum() const {
		return prop_num_gen_.Get();
	}

	uint64_t GetActiveBeginProposedNum() const {
		return active_begin_prop_num_;
	}

	const paxos::Entry& GetProposingValue() const {
		assert(nullptr != proposing_value_);
		return *proposing_value_;
	}
    
private:
    PropState stepPrepareRsp(
            uint8_t peer_id, 
            uint64_t peer_promised_num, 
            uint64_t peer_accepted_num, 
            const Entry* peer_accepted_value);

    PropState stepAcceptRsp(
            uint8_t peer_id, 
            uint64_t peer_accepted_num, 
            bool is_fast_accept_rsp);

    PropState stepTryPropose(
            uint64_t hint_proposed_num, 
            const paxos::Entry& try_proposing_value);

    PropState stepBeginPropose(
            uint64_t hint_proposed_num, 
            const paxos::Entry& proposing_value);

    PropState beginPreparePhase(PaxosInstance& pins_impl);

    PropState beginAcceptPhase(PaxosInstance& pins_impl);

    std::tuple<bool, MessageType>
        updatePropState(PropState next_prop_state, PaxosInstance& pins_impl);

private:
    static const int major_cnt_ = 2; // 2 is major in 3-size group
    std::string key_;
	uint64_t index_ = 0;

	uint16_t active_prop_cnt_ = 0;
	uint64_t active_begin_prop_num_ = 0;

    cutils::PropNumGen prop_num_gen_;
    PropState prop_state_ = PropState::NIL;

    uint64_t max_accepted_hint_num_ = 0ull;
    uint64_t max_hint_num_ = 0ull;

    std::map<uint8_t, bool> rsp_votes_;
    std::unique_ptr<Entry> proposing_value_;

    // TODO
    // pipes_[0]: poll
    // pipes_[1]: notify: write or close
    int pipes_[2] = {-1, -1};
};


class PInsWrapper {

public:
    // test function
    const PaxosInstance* TestGetPaxosInstance() const {
        return &pins_impl_;
    }
    // end of test function

public:
    PInsWrapper(
            PInsAliveState* pins_state, PaxosInstance& pins_impl);

    std::tuple<int, bool, std::unique_ptr<Message>> Step(const Message& msg);

    bool IsChosen() const {
        return pins_impl_.chosen();
    }

private:
    void markChosen();

    std::tuple<int, bool, std::unique_ptr<Message>> 
        stepChosen(const Message& msg);

    std::tuple<int, bool, std::unique_ptr<Message>> 
        stepNotChosen(const Message& msg);

    std::unique_ptr<Message> 
        produceRsp(const Message& msg, MessageType rsp_msg_type);


private:
    PInsAliveState* pins_state_;
    PaxosInstance& pins_impl_;
};


} // namespace paxos


