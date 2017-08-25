
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <limits>
#include <mutex>
#include <tuple>
#include <stdint.h>
#include <cassert>


namespace cutils {


// generate uniq id
// copy cat of: etcd/pkg/idutil
// => 2bytes: member_id
//    5bytes: timestamp ms
//    1byte : cnt
class IDGenerator {

    enum {
        TIMESTAMP_LEN = 5 * 8, 
        CNT_LEN = 8, 
        SUFFIX_LEN = TIMESTAMP_LEN + CNT_LEN, 
    };

public:
    IDGenerator(uint64_t member_id, uint64_t time_ms)
        : prefix_(member_id << SUFFIX_LEN)
        , suffix_(time_ms << CNT_LEN)
    {

    }

    uint64_t operator()()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++suffix_;
        return prefix_ | low_bit(suffix_, SUFFIX_LEN);
    }

	static std::tuple<uint16_t, uint8_t> decompose(uint64_t reqid)
	{
		uint16_t member_id = reqid >> SUFFIX_LEN;
		uint8_t cnt = reqid & 0xFF;
		return std::make_tuple(member_id, cnt);
	}

private:
    uint64_t low_bit(uint64_t value, int bit_cnt) 
    {
        assert(0 <= bit_cnt && 64 >= bit_cnt);
        return value & (std::numeric_limits<uint64_t>::max() >> (64 - bit_cnt));
    }

private:
    std::mutex mutex_;
    uint64_t prefix_;
    uint64_t suffix_;
}; 


inline uint64_t prop_num_compose(uint16_t id, uint64_t prop_cnt)
{
    return (prop_cnt << 16) + id;
}

inline 
std::tuple<uint16_t, uint64_t> prop_num_decompose(uint64_t prop_num)
{
    return std::make_tuple(prop_num & 0xFFFF, prop_num >> 16);
}

inline uint64_t get_prop_cnt(uint64_t prop_num)
{
	uint16_t id = 0;
	uint64_t prop_cnt = 0;
	std::tie(id, prop_cnt) = prop_num_decompose(prop_num);
	return prop_cnt;
}

// IMPORTANT: NOT thread safe
class PropNumGen {
public:
	// add for test
	void TestReset(uint64_t prop_num) {
		std::tie(selfid_, prop_cnt_) = prop_num_decompose(prop_num);
	}
	// end of test

public:
    PropNumGen(uint16_t selfid, uint64_t prop_cnt) 
        : selfid_(selfid)
        , prop_cnt_(prop_cnt)
    {

    }

    PropNumGen(uint64_t prop_num) {
        std::tie(selfid_, prop_cnt_) = prop_num_decompose(prop_num);
    }

    uint64_t Get() const {
        return prop_num_compose(selfid_, prop_cnt_);
    }

    // after update: Get() >= prop_num
    bool Update(uint64_t prop_num) 
    {
        if (Get() >= prop_num) {
            return false;
        }

        // else assert(Get() < prop_num);
        uint16_t id = 0;
        uint64_t cnt = 0ull;
        std::tie(id, cnt) = prop_num_decompose(prop_num);
        if (id > selfid_) {
            prop_cnt_ = cnt + 1ull;
        }
        else {
            // assert(id <= selfid_);
            prop_cnt_ = cnt;
        }

        assert(Get() >= prop_num);
        return true;
    }

    // after update: Get() > hint_num
    uint64_t Next(uint64_t hint_num) 
    {
        Update(hint_num);
		assert(Get() >= hint_num);
		if (Get() == hint_num)
		{
			++prop_cnt_;
		}
		assert(Get() > hint_num);
        return Get(); 
    }

    bool IsLocalNum(uint64_t prop_num) const {
        auto id_cnt = prop_num_decompose(prop_num);
        return selfid_ == std::get<0>(id_cnt);
    }

	uint64_t Reset(uint64_t prop_num) 
	{
		uint64_t prev_prop_num = Get();
		uint16_t new_self_id = 0;
		uint64_t new_prop_cnt = 0;
		std::tie(new_self_id, new_prop_cnt) = prop_num_decompose(prop_num);
		assert(new_self_id == selfid_);
		prop_cnt_ = new_prop_cnt;
		assert(Get() == prop_num);
		return prev_prop_num;
	}

private:
    uint16_t selfid_;
    uint64_t prop_cnt_;
};




} // namespace cutils


