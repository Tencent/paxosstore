
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "plog_helper.h"
#include "paxos.pb.h"
#include "err_code.h"


namespace {

bool is_peer_chosen(uint8_t peer_status)
{
    if (0 != (peer_status & paxos::PEER_STATUS::D_CHOSEN)) {
        return true;
    }

    if (0 != (peer_status & paxos::PEER_STATUS::C_CHOSEN)) {
        return true;
    }

    return false;
}


} // namespace 


namespace paxos {

std::string to_paxos_key(uint64_t logid)
{
    std::string key;
    key.resize(sizeof(uint64_t));
    memcpy(&key[0], &logid, sizeof(uint64_t));
    return key;
}



uint64_t get_min_index(const paxos::PaxosLog& plog)
{
    return 0 == plog.entries_size() ? 0: plog.entries(0).index();
}

uint64_t get_max_index(const paxos::PaxosLog& plog)
{
    return 0 == plog.entries_size() ?
        0 : plog.entries(plog.entries_size()-1).index();
}

uint64_t get_chosen_index(const paxos::PaxosLog& plog)
{
    auto chosen_ins = get_chosen_ins(plog);
    return nullptr == chosen_ins ? 0 : chosen_ins->index();
}

uint64_t get_chosen_reqid(const paxos::PaxosLog& plog)
{
    auto chosen_ins = get_chosen_ins(plog);
    return nullptr == chosen_ins ? 
        0 : chosen_ins->accepted_value().reqid();
}

bool has_pending_ins(const paxos::PaxosLog& plog)
{
    auto max_ins = get_max_ins(plog);
    if (nullptr == max_ins) {
        return false;
    }

    return !max_ins->chosen();
}

const paxos::PaxosInstance* 
get_chosen_ins(const paxos::PaxosLog& plog_impl)
{
	const paxos::PaxosInstance* chosen_ins = nullptr;
	for (int idx = plog_impl.entries_size() - 1; idx >= 0; --idx) {
		if (plog_impl.entries(idx).chosen()) {
			chosen_ins = &(plog_impl.entries(idx));
			assert(nullptr != chosen_ins);
			break;
		}
	}

	return chosen_ins;
}

paxos::PaxosInstance* 
get_max_ins(paxos::PaxosLog& plog_impl)
{
	const int entries_size = plog_impl.entries_size();
	return 0 == entries_size ? 
		nullptr : plog_impl.mutable_entries(entries_size - 1);
}

paxos::PaxosInstance*
get_pending_ins(paxos::PaxosLog& plog_impl)
{
    auto ins = get_max_ins(plog_impl);
    if (nullptr == ins || false == ins->chosen()) {
        return ins;
    }

    assert(ins->chosen());
    return nullptr;
}

std::tuple<int, std::string>
get_value(const paxos::PaxosLog& plog, uint64_t index)
{
    for (int idx = 0; idx < plog.entries_size(); ++idx) {
        auto& ins = plog.entries(idx);
        if (ins.index() == index) {
            if (ins.chosen() && ins.has_accepted_value()) {
                return std::make_tuple(0, ins.accepted_value().data());
            }

            return std::make_tuple(-1, "");
        }
    }

    return std::make_tuple(-2, "");
}

bool is_slim(const paxos::PaxosLog& plog)
{
    const int entries_size = plog.entries_size();
    if (2 != entries_size) {
        return 2 > entries_size;
    }

    assert(2 == entries_size);
    const auto& ins = plog.entries(1);
    const auto& prev_ins = plog.entries(0);
    if (ins.chosen() || 
            prev_ins.index() + 1 != ins.index() || 
            false == prev_ins.has_accepted_value()) {
        return false;
    }

    assert(false == ins.chosen() && prev_ins.has_accepted_value());
    if (prev_ins.chosen()) {
        return true;
    }

    assert(false == prev_ins.chosen());
    return false == ins.has_accepted_value();
}

int shrink_plog(paxos::PaxosLog& plog)
{
    if (is_slim(plog)) {
        return 0;
    }

    const int entries_size = plog.entries_size();
    assert(2 <= entries_size);

    paxos::PaxosInstance* arr_ins[2] = {nullptr};
    auto ins = plog.mutable_entries(entries_size-1);
    assert(nullptr != ins);

    arr_ins[0] = ins;
    auto prev_ins = plog.mutable_entries(entries_size-2);
    assert(nullptr != prev_ins);
    if (false == ins->chosen() && 
            prev_ins->index() + 1 == ins->index() && 
            prev_ins->has_accepted_value()) {
        if (prev_ins->chosen() || false == ins->has_accepted_value()) {
            arr_ins[1] = prev_ins;
        }
    }

    PaxosLog new_plog;
    for (int idx = 1; idx >= 0; --idx) {
        if (nullptr == arr_ins[idx]) {
            continue;
        }

        auto new_ins = new_plog.add_entries();
        assert(nullptr != new_ins);
        new_ins->Swap(arr_ins[idx]);
    }

    plog.Swap(&new_plog);
    assert(is_slim(plog));
    return 1;
}

paxos::PaxosLog zeros_plog()
{
    paxos::PaxosLog plog;
    auto zeros_ins = plog.add_entries();
    assert(nullptr != zeros_ins);
    zeros_ins->set_index(0);
	return plog;
}


int can_write(
        const paxos::PaxosLog& plog)
{
    assert(is_slim(plog));
    if (0 == plog.entries_size()) {
        return 0;
    }

    assert(0 < plog.entries_size());
    auto max_ins = get_max_ins(plog);
    assert(nullptr != max_ins);
    if (1 == plog.entries_size()) {
        if (false == max_ins->chosen()) {
            return 1 == max_ins->index() ? 0 : PAXOS_SET_LOCAL_OUT;
        }

        assert(max_ins->chosen());
        return 0;
    }

    assert(2 == plog.entries_size());
    auto chosen_ins = get_chosen_ins(plog);
    assert(chosen_ins->index() + 1 == max_ins->index());
    return 0;
}


int can_read_3svr(
        uint64_t local_chosen_index, 
        uint64_t local_max_index, 
        uint64_t other_max_index, 
        uint8_t peer_status)
{
    // simple version.. 
    if (PEER_STATUS::IGNORE == peer_status) {
        return 0; // ignore check
    }

    if (local_max_index > local_chosen_index + 1 || 
            other_max_index > local_chosen_index + 1 || 
            other_max_index > local_max_index) {
        return PAXOS_GET_LOCAL_OUT;
    }

    assert(local_max_index <= local_chosen_index + 1);
    assert(local_max_index >= local_chosen_index);
    assert(other_max_index <= local_chosen_index + 1);
    assert(other_max_index <= local_max_index);
    if (other_max_index <= local_chosen_index) {
        return 0;
    }
    assert(other_max_index == local_chosen_index + 1);
    assert(other_max_index == local_max_index);
    if (is_peer_chosen(peer_status)) {
        return PAXOS_GET_LOCAL_OUT;
    }

    return PAXOS_GET_MAY_LOCAL_OUT;
}


} // namespace paxos


