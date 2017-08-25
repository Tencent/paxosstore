
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once


#include <vector>
#include <memory>
#include <map>
#include <set>
#include <functional>
#include "cutils/cqueue.h"


namespace paxos {


class Message;


using PaxosMsgQueue = cutils::CQueue<paxos::Message>;

class SmartPaxosMsgRecver {

public:
    SmartPaxosMsgRecver(
            std::map<int, 
                std::unique_ptr<PaxosMsgQueue>>& map_recv_queue);

    ~SmartPaxosMsgRecver();

    void BatchRecvMsg(
            int client_ip, 
            std::vector<std::unique_ptr<paxos::Message>> vec_msg);

private:
    std::map<int, std::unique_ptr<PaxosMsgQueue>>& map_recv_queue_;
};

class SmartPaxosMsgSender {

public:
    SmartPaxosMsgSender(
            // err, member_id
            std::function<
                std::tuple<int, int>(const Message&)> pfn_route, 
            std::map<int, 
                std::unique_ptr<PaxosMsgQueue>>& map_send_queue);

    void SendMsg(std::unique_ptr<paxos::Message> msg);

    void BatchSendMsg(
            std::vector<std::unique_ptr<paxos::Message>> vec_msg);

private:
    void SendMsgImpl(std::unique_ptr<paxos::Message> msg);

private:
    std::function<std::tuple<int, int>(const Message)> pfn_route_;
    std::map<int, std::unique_ptr<PaxosMsgQueue>>& map_send_queue_;
};


void send_msg_worker(
        std::string svr_ip, 
        const int svr_port, 
        size_t max_batch_size, 
		PaxosMsgQueue& msg_queue, 
		bool& stop);

     
} // namespace paxos


