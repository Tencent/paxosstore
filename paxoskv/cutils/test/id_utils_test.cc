
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>
#include <chrono>
#include <cassert>
#include "gtest/gtest.h"
#include "id_utils.h"
#include "hassert.h"

using namespace std;

uint64_t current_time_ms()
{
    auto now = chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

TEST(IDGeneratorTest, SimpleTest)
{
    auto time_ms = current_time_ms();
    cutils::IDGenerator gen(1, time_ms);
    {
        cutils::IDGenerator new_gen(2, time_ms);
        for (int i = 0; i < 255; ++i) {
            assert(gen() != new_gen());
        }
    }

    auto id = gen();
    {
        auto tmp_id = id;
        for (int i = 0; i < 256; ++i) {
            auto new_id = gen();
            hassert(tmp_id < new_id, "tmp_id %" PRIu64 " new_id %" PRIu64, tmp_id, new_id);
            tmp_id = new_id;
        }
    }
    
    usleep(1 * 1000);
    cutils::IDGenerator new_gen(1, current_time_ms());
    assert(id < new_gen());
    for (int i = 0; i < 300; ++i) {
        assert(gen() != new_gen());
    }
}

TEST(PropNumGenTest, SimpleTest)
{
	uint64_t proposed_num = 131074;
	uint16_t id = 0;
	uint64_t prop_cnt = 0;
	std::tie(id, prop_cnt) = cutils::prop_num_decompose(proposed_num);
	printf ( "%lu %u %lu\n", proposed_num, static_cast<uint32_t>(id), prop_cnt );

	uint64_t new_proposed_num = cutils::PropNumGen(id, 0).Next(proposed_num);
	printf ( "%lu new: %lu\n", proposed_num, new_proposed_num );
}
