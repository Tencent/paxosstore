
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




    int
main ( int argc, char *argv[] )
{
    const char* default_str_ip = "127.0.0.1";
    int default_int_ip = paxos::to_int_ip(default_str_ip);

    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>> map_recv_queue;
    map_recv_queue[default_int_ip] = 
        cutils::make_unique<paxos::PaxosMsgQueue>("default", 1000);
    map_recv_queue[0] = 
        cutils::make_unique<paxos::PaxosMsgQueue>("0", 1000);

    paxos::SmartPaxosMsgRecver msg_reciver(map_recv_queue);

    auto msg_svr = cutils::make_unique<
        cutils::AsyncWorker>(paxos::start_multiple_msg_svr, 
                "127.0.0.1", 13069, 
                1, std::ref(msg_reciver));

    printf ( "start svr 127.0.0.1 : 13069 \n" );
    sleep(10);

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
