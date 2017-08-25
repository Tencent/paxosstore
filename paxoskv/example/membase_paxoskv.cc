
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <vector>
#include <memory>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "cutils/async_worker.h"
#include "cutils/mem_utils.h"
#include "core/paxos.pb.h"
#include "msg_svr/msg_comm.h"
#include "msg_svr/msg_svr.h"
#include "msg_svr/msg_helper.h"
#include "memkv/memkv.h"
#include "memkv/memloader.h"
#include "memkv/pmergetor.h"
#include "dbcomm/newstorage.h"
#include "dbcomm/db_comm.h"
#include "kv/db_option.h"
#include "kv/db_impl.h"
#include "kv/hard_memkv.h"


paxoskv::Option fake_option()
{
    paxoskv::Option option;
    option.pfn_hash32 = 
        [](const std::string& key) -> uint32_t {
            return 0;
        };
    option.pfn_selfid = 
        [](const std::string& key) -> uint8_t {
            return 1;
        };
    option.pfn_member_route = 
        [](const paxos::Message& msg) -> std::tuple<int, int> {
            return std::make_tuple(-1, 0);
        };
    option.pfn_read = 
        [](const std::string& key, 
           paxos::PaxosLogHeader& header, 
           paxos::PaxosLog& plog) -> int {
            return -1;
        };
    option.pfn_read_header = 
        [](const std::string& key, 
           paxos::PaxosLogHeader& header) -> int {
            return -1;
        };
    option.pfn_write = 
        [](const std::string& key, 
           const paxos::PaxosLogHeader& header, 
           const paxos::PaxosLog& plog) -> int {
            return -1;
        };

    assert(0 == dbcomm::CheckAndFixDirPath("./example_kvsvr"));
    assert(0 == dbcomm::CheckAndFixDirPath("./example_kvsvr/data"));
    option.db_path = "./example_kvsvr";
    option.db_plog_path = "./example_kvsvr/data";
    assert(0 == dbcomm::CheckAndFixDirPath(option.db_path));
    return option;
}

void StartPLogMerge(
        memkv::PMergetor& mergetor, bool& stop)
{
    while (false == stop) {
        int ret = mergetor.Merge();
        sleep(10);
    }
}


    int
main ( int argc, char *argv[] )
{
    std::vector<std::unique_ptr<cutils::AsyncWorker>> vec_works;

    const char* default_str_ip = "127.0.0.1";
    int default_int_ip = paxos::to_int_ip(default_str_ip);

    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>> map_recv_queue;
    map_recv_queue[default_int_ip] = 
        cutils::make_unique<paxos::PaxosMsgQueue>("default", 1000);
    map_recv_queue[0] = 
        cutils::make_unique<paxos::PaxosMsgQueue>("0", 1000);

    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>> map_send_queue;
    map_send_queue[default_int_ip] = 
        cutils::make_unique<paxos::PaxosMsgQueue>("default", 1000);

    paxos::SmartPaxosMsgRecver msg_reciver(map_recv_queue);

    auto msg_svr = cutils::make_unique<
        cutils::AsyncWorker>(paxos::start_multiple_msg_svr, 
                "127.0.0.1", 13069, 
                1, std::ref(msg_reciver));

    printf ( "start svr 127.0.0.1 : 13069 \n" );

    vec_works.push_back(std::move(msg_svr));

    auto msg_cli = cutils::make_unique<
        cutils::AsyncWorker>(
                paxos::send_msg_worker, 
                "127.0.0.1", 13069, 
                1, std::ref(*map_send_queue[default_int_ip]));

    vec_works.push_back(std::move(msg_cli));

    int ret = 0;
    paxoskv::Option option = fake_option();

    // hmemkv
    paxoskv::clsHardMemKv hmemkv;
    ret = hmemkv.Init(option);
    assert(0 == ret);

    paxoskv::update_option(hmemkv, option);

    // end of memkv
    paxos::SmartPaxosMsgSender msg_sender(
            option.pfn_member_route, map_send_queue); 

    const uint16_t member_id = 1;
    paxoskv::DBImpl db_impl(member_id, option);

    ret = db_impl.Init(&msg_sender);
    assert(0 == ret);

    ret = db_impl.StartGetLocalOutWorker();
    assert(0 == ret);

    ret = db_impl.StartTimeoutWorker();
    assert(0 == ret);

    sleep(10 * 60);


    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
