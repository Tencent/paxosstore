
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <memory>
#include <tuple>
#include <string>
#include <chrono>
#include "pins_wrapper.h"


namespace paxos {

class Message;
class PaxosLog;
class PaxosInstance;
//class PInsAliveState;
//class PInsWrapper;

class PLogWrapper {
    // test function
public:
    std::tuple<int, std::unique_ptr<PInsWrapper>>
        TestGetInstance(uint64_t msg_index) {
            return getInstance(msg_index);
        }

    void TestSetPInsState(PInsAliveState* pins_state) {
        pins_state_ = pins_state;
    }

    // end of test function

public:
    PLogWrapper(
            uint8_t selfid, 
            uint16_t member_id, 
            const std::string& key, 
            PInsAliveState* pins_state, 
            PaxosLog& plog_impl);

    ~PLogWrapper();

    std::tuple<int, std::unique_ptr<Message>> Step(const Message& msg);

    std::tuple<
        int, 
        std::shared_ptr<PInsAliveState>, 
        std::unique_ptr<Message>>
    Set(uint64_t reqid, const std::string& raw_value, bool do_fast_accpt);

    // <err, set_index>
    std::tuple<
        int, 
        std::shared_ptr<PInsAliveState>, 
        std::unique_ptr<Message>> 
    NormalSet(
            uint64_t reqid, 
            const std::string& data, bool do_fast_accpt);

	std::tuple<
        int, 
        std::shared_ptr<PInsAliveState>, 
        std::unique_ptr<Message>>
    PreemptSet(uint64_t reqid, const std::string& data);

	std::tuple<int, std::unique_ptr<Message>> TryRedoProp();

    // assistant function
    PInsAliveState* SetPInsAliveState(PInsAliveState* pins_state);

	PInsAliveState* GetPInsAliveState() const {
		return pins_state_;
	}

    bool NeedDiskWrite() const {
        return need_disk_write_;
    }

	bool NeedUpdateChosen() const {
		return need_update_chosen_;
	}

    const std::string& GetKey() const {
        return key_;
    }

	uint8_t SelfID() const {
		return selfid_;
	}

    paxos::PaxosLog& GetPLog() {
        return plog_impl_;
    }

    void ClearPInsAliveState() {
        pins_state_ = nullptr;
    }

private:

    void setDiskWrite() {
        need_disk_write_ = true;
    }

	void setUpdateChosen() {
		need_update_chosen_ = true;
	}

    std::tuple<int, std::unique_ptr<PInsWrapper>> 
        getInstance(uint64_t msg_index);

	std::tuple<int, std::unique_ptr<Message>>
		stepInvalidIndex(const Message& msg);
private:
    // key: => selfid_;
    uint8_t selfid_ = 0;
    // svr: => member_id_;
    uint16_t member_id_ = 0;
    const std::string key_;

    bool need_disk_write_ = false;
	bool need_update_chosen_ = false;

    PInsAliveState* pins_state_ = nullptr;
    PaxosLog& plog_impl_;
}; 

} // namespace paxos


