
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
#include <functional>


namespace paxos {
    class Message;
    class PaxosLogHeader;
    class PaxosLog;
} // 


namespace paxoskv {


struct Option {

    std::string db_path = "/home/qspace/data/kvsvr/";
    std::string db_plog_path = "/home/qspace/data/kvsvr/data/";
    uint32_t db_lock_size = 100 * 10000;

    uint32_t max_set_timeout = 100;
    uint32_t lo_max_track_size = 1000;
    uint32_t lo_hold_interval_ms = 100;

    uint32_t ascache_timeout_entries_size = 200;

    uint32_t max_concur_wait = 10;
    uint32_t max_get_waittime = 30;


    // memkv : mem
    uint32_t mem_idx_headsize = 100 * 10000;
    uint32_t mem_append_blknum = 2;
    uint32_t mem_load_workercnt = 3;
	int idx_shm_key = 2333333;
	int data_block_shm_key = 6666666;
    // end of memkv : mem

    // memkv : bitcask
    uint32_t ldb_blksize = 4 * 1024;
    uint32_t ldb_dio_bufsize = 21 * 1024 * 1024;
    uint32_t ldb_adjust_strategy = 2;
    // 0.5ms...
    uint32_t ldb_set_waittime = 500;
    // end of memkv : bitcask

    std::function<uint32_t(const std::string&)> pfn_hash32;

    std::function<uint8_t(const std::string&)> pfn_selfid;

    // err, member_id
    std::function<
        std::tuple<int, int>(
                const paxos::Message&)> pfn_member_route;

    std::function<int(
            const std::string&, 
            paxos::PaxosLogHeader&, paxos::PaxosLog&)> pfn_read;

    std::function<int(
            const std::string&, 
            paxos::PaxosLogHeader&)> pfn_read_header;

    std::function<int(
            const std::string&, 
            const paxos::PaxosLogHeader&, 
            const paxos::PaxosLog&)> pfn_write;

}; // struct Option




} // namespace paxoskv


