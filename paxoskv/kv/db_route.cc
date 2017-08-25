
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <string>
#include <cstring>
#include <cassert>
#include "cutils/hash_utils.h"
#include "cutils/log_utils.h"
#include "core/paxos.pb.h"
#include "comm/svrlist_config_base.h"
#include "comm/svr_route.h"
#include "db_route.h"
#include "db_option.h"

namespace {

uint64_t to_logid(const std::string& key)
{
    assert(sizeof(uint64_t) == key.size());
    uint64_t logid = 0;
    memcpy(&logid, key.data(), key.size());
    return logid;
}

uint32_t hashkey_hash32(const std::string& key)
{
    uint64_t logid = to_logid(key);
    return cutils::dict_int_hash_func(logid);
}

uint32_t strkey_hash32(const std::string& key)
{
    assert(false == key.empty());
    return cutils::bkdr_hash(key.c_str());
}

std::tuple<int, int>
hashkey_member_route(
        clsSvrListConfigBase* config, 
        const paxos::Message& msg)
{
    assert(nullptr != config);
    assert(0 != msg.to());
    assert(3 >= msg.to());
    const auto& key = msg.key();
    if (key.size() != sizeof(uint64_t)) {
        return std::make_tuple(-1, 0);
    }

    assert(sizeof(uint64_t) == key.size());
    int arr_members[3] = {0};
    auto ret = clsSvrRoute::GetSvrMemberIDByKey4All(
            config, key.data(), key.size(), 
            arr_members[0], arr_members[1], arr_members[2]);
    if (0 != ret) {
        logerr("GetSvrMemberIDByKey4All ret %d", ret);
        assert(0 > ret);
        return std::make_tuple(ret, 0);
    }

    assert(0 == ret);
    return std::make_tuple(0, arr_members[msg.to()-1]);
}

std::tuple<int, int>
strkey_member_route(
        clsSvrListConfigBase* config, 
        const paxos::Message& msg)
{
    assert(nullptr != config);
    assert(0 != msg.to());
    assert(3 >= msg.to());
    const auto& key = msg.key();
    if (key.empty()) {
        return -1;
    }

    assert(false == key.empty());
    uint64_t hashkey = cutils::bkdr_64hash(key.c_str());
    int arr_members[3] = {0};
    auto ret = clsSvrRoute::GetSvrMemberIDByKey4All(
            config, reinterpret_cast<char*>(&hashkey), sizeof(hashkey), 
            arr_members[0], arr_members[1], arr_members[2]);
    if (0 != ret) {
        logerr("GetSvrSetByKey4All key %s ret %d", key.c_str(), ret);
        assert(0 > ret);
        return std::make_tuple(ret, 0);
    }

    assert(0 == ret);
    return std::make_tuple(0, arr_members[msg.to()-1]);
}

uint8_t hashkey_selfid_impl(
        clsSvrListConfigBase* config, 
        uint16_t self_member_id, 
        uint64_t hashkey)
{
    int arr_members[3] = {0};
    auto ret = clsSvrRoute::GetSvrMemberIDByKey4All(
            config, reinterpret_cast<char*>(&hashkey), sizeof(hashkey), 
            arr_members[0], arr_members[1], arr_members[2]);
    if (0 != ret) {
        assert(false);
        return 0; // error
    }

    assert(0 == ret);
    for (int idx = 0; idx < 3; ++idx) {
        if (self_member_id == arr_members[idx]) {
            return idx + 1;
        }
    }
    
    assert(false); // error
    return 0;
}

uint8_t hashkey_selfid(
        clsSvrListConfigBase* config, 
        uint16_t self_member_id, 
        const std::string& key)
{
    assert(nullptr != config);
    assert(0 < self_member_id);
    assert(6 >= self_member_id);

    uint64_t logid = to_logid(key);
    return hashkey_selfid_impl(config, self_member_id, logid);
}

uint8_t strkey_selfid(
        clsSvrListConfigBase* config, 
        uint16_t self_member_id, 
        const std::string& key)
{
    assert(nullptr != config);
    assert(0 < self_member_id);
    assert(6 >= self_member_id);
    assert(false == key.empty());

    uint64_t hashkey = cutils::bkdr_64hash(key.c_str());
    return hashkey_selfid_impl(config, self_member_id, hashkey);
}

} // namespace



namespace paxoskv {

// pfn_hash32: uint32_t(const std::string&)
// pfn_selfid: uint8_t(const std::string&)
// pfn_member_route: std::tuple<int, int>(const paxos::Message&)

void update_hashkey_option(
        clsSvrListConfigBase* config, 
        const uint16_t self_member_id, 
        Option& option)
{
    option.pfn_hash32 = hashkey_hash32;
    option.pfn_selfid = 
        [=](const std::string& key) -> uint8_t {
            return hashkey_selfid(config, self_member_id, key);
        };
    option.pfn_member_route = 
        [=](const paxos::Message& msg) -> std::tuple<int, int> {
            return hashkey_member_route(config, msg);
        };
    return ;
}


void update_strkey_option(
        clsSvrListConfigBase* config, 
        const uint16_t self_member_id, 
        Option& option)
{
    option.pfn_hash32 = strkey_hash32;
    option.pfn_selfid = 
        [=](const std::string& key) -> uint8_t {
            return strkey_selfid(config, self_member_id, key);
        };
    option.pfn_member_route = 
        [=](const paxos::Message& msg) -> std::tuple<int, int> {
            return strkey_member_route(config, msg);
        };
    return ;
}





} // namespace paxoskv



