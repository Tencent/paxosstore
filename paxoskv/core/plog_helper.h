
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <string>

namespace paxos {

class PaxosLog;
class PaxosInstance;

std::string to_paxos_key(uint64_t logid);

uint64_t get_min_index(const paxos::PaxosLog& plog);

uint64_t get_max_index(const paxos::PaxosLog& plog);

uint64_t get_chosen_index(const paxos::PaxosLog& plog);

uint64_t get_chosen_reqid(const paxos::PaxosLog& plog);

// check max ins is not chosen !!!
bool has_pending_ins(const paxos::PaxosLog& plog);

const paxos::PaxosInstance* 
get_chosen_ins(const paxos::PaxosLog& plog);

paxos::PaxosInstance* get_max_ins(paxos::PaxosLog& plog);

inline const paxos::PaxosInstance*
get_max_ins(const paxos::PaxosLog& plog) {
    return get_max_ins(const_cast<paxos::PaxosLog&>(plog));
}

bool is_slim(const paxos::PaxosLog& plog);

int shrink_plog(paxos::PaxosLog& plog);


paxos::PaxosLog zeros_plog();

int can_write(const paxos::PaxosLog& plog);


// only use for 3svr..
namespace PEER_STATUS {

enum {
    UNKOWN = 0, 

    D_UNKOWN = 0, 
    D_OUT = 0x01, 
    D_PENDING = 0x02, 
    D_CHOSEN = 0x03, 

    C_UNKOWN = 0, 
    C_OUT = 0x10, 
    C_PENDING = 0x20, 
    C_CHOSEN = 0x30, 

    IGNORE = 0xFF, 
};

};


int can_read_3svr(
        uint64_t local_chosen_index, 
        uint64_t local_max_index, 
        uint64_t other_max_index, 
        uint8_t peer_status);

} // namespace paxos

