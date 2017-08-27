
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/





#include <vector>
#include <string>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include "core/paxos.pb.h"
#include "cutils/mem_utils.h"
#include "cutils/time_utils.h"

#define CNT 10000


   int
main ( int argc, char *argv[] )
{
    std::string buf;
    
    paxos::Message fake_msg;
    fake_msg.set_type(paxos::MessageType::PROP);
    fake_msg.set_from(1);
    fake_msg.set_to(1);
    fake_msg.set_key("abc");
    fake_msg.set_index(1);
    fake_msg.set_proposed_num(1);
    {
        auto entry = fake_msg.mutable_accepted_value();
        assert(nullptr != entry);
    
        entry->set_reqid(123);
        entry->set_data(std::string(100, 'a'));
    }

    buf.resize((fake_msg.ByteSize() + sizeof(int)) * CNT);
    char* iter = &buf[0];
    for (int i = 0; i < CNT; ++i) {
        int total_len = fake_msg.ByteSize();
        total_len = htonl(total_len);
        memcpy(iter, &total_len, sizeof(int));
        iter += sizeof(int);
        assert(fake_msg.SerializeToArray(iter, fake_msg.ByteSize()));
        iter += fake_msg.ByteSize();
    }

    // test
    // => unique_msg
    std::vector<std::unique_ptr<paxos::Message>> vec_msg;
    iter = &buf[0];
    uint64_t begin = cutils::get_curr_ms();
    for (int i = 0; i < CNT; ++i) {
        int total_len = *(reinterpret_cast<int*>(iter));
        total_len = ntohl(total_len);
        assert(0 <= total_len);
        std::unique_ptr<paxos::Message> msg;
        msg = cutils::make_unique<paxos::Message>();
        assert(nullptr != msg);
        iter += sizeof(int);
        assert(msg->ParseFromArray(iter, total_len));
        iter += total_len;
        vec_msg.emplace_back(std::move(msg));
    }

    uint64_t costtime = cutils::get_curr_ms() - begin;
    printf ( "Parse costtime %lu\n", costtime );

    begin = cutils::get_curr_ms();
    std::vector<std::unique_ptr<std::string>> vec_recv;
    iter = &buf[0];
    for (int i = 0; i < 1000; ++i) {
        int total_len = *(reinterpret_cast<int*>(iter));
        total_len = ntohl(total_len);
        iter += sizeof(int);
        std::unique_ptr<std::string> recv = cutils::make_unique<std::string>(iter, iter + total_len);
        assert(nullptr != recv);
        assert(total_len == recv->size());
        iter += total_len;
        vec_recv.emplace_back(std::move(recv));
    }

    costtime = cutils::get_curr_ms() - begin;
    printf ( "recv buf cost time %lu\n", costtime );
    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
