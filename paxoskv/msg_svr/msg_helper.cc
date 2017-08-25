
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "msg_helper.h"
#include "msg_cli.h"
#include "cutils/mem_utils.h"
#include "cutils/log_utils.h"
#include "core/paxos.pb.h"


namespace paxos {

SmartPaxosMsgRecver::SmartPaxosMsgRecver(
        std::map<int, std::unique_ptr<PaxosMsgQueue>>& map_recv_queue)
    : map_recv_queue_(map_recv_queue)
{

}

SmartPaxosMsgRecver::~SmartPaxosMsgRecver() = default;

void SmartPaxosMsgRecver::BatchRecvMsg(
        int client_ip, 
        std::vector<std::unique_ptr<paxos::Message>> vec_msg)
{
    auto iter = map_recv_queue_.find(client_ip);
    if (map_recv_queue_.end() != iter) {
        iter->second->BatchPush(std::move(vec_msg));
        return ;
    }

    map_recv_queue_.at(0)->BatchPush(std::move(vec_msg));
    return ;
}


SmartPaxosMsgSender::SmartPaxosMsgSender(
        std::function<
            std::tuple<int, int>(const Message&)> pfn_route, 
        std::map<int, 
            std::unique_ptr<PaxosMsgQueue>>& map_send_queue)
    : pfn_route_(pfn_route)
    , map_send_queue_(map_send_queue)
{
    assert(nullptr != pfn_route);
}

void SmartPaxosMsgSender::SendMsgImpl(
        std::unique_ptr<paxos::Message> msg)
{
    assert(nullptr != msg);
    assert(0 != msg->to());

    int ret = 0;
    int member_id = 0;
    std::tie(ret, member_id) = pfn_route_(*msg);
    if (0 != ret) {
        logerr("pfn_route_ ret %d", ret);
        return ;
    }

    assert(0 == ret);
    if (map_send_queue_.end() == map_send_queue_.find(member_id)) {
        return ;
    }

    assert(nullptr != map_send_queue_.at(member_id));
    map_send_queue_.at(member_id)->Push(std::move(msg));
    return ;
}

void SmartPaxosMsgSender::SendMsg(
        std::unique_ptr<paxos::Message> msg)
{
    if (nullptr == msg) {
        return ; // do nothing;
    }

    assert(nullptr != msg);
    if (0 != msg->to()) {
        SendMsgImpl(std::move(msg));
        return ;
    }

    assert(0 == msg->to());
    uint32_t arrTo[2] = {1, 2};
    assert(0 < msg->from());
    assert(3 >= msg->from());
    for (int idx = 0; idx < 2; ++idx) {
        if (msg->from() == arrTo[idx]) {
            arrTo[idx] = 3;
            break;
        }
    }

    auto other_msg = cutils::make_unique<paxos::Message>(*msg);
    other_msg->set_to(arrTo[0]);
    SendMsgImpl(std::move(other_msg));
    assert(nullptr == other_msg);
    msg->set_to(arrTo[1]);
    SendMsgImpl(std::move(msg));
    return ;
}

void SmartPaxosMsgSender::BatchSendMsg(
        std::vector<std::unique_ptr<paxos::Message>> vec_msg)
{
    // TODO
    for (auto& msg : vec_msg) {
        SendMsg(std::move(msg));
    }
}

void send_msg_worker(
        std::string svr_ip, 
        const int svr_port, 
        size_t max_batch_size, 
		PaxosMsgQueue& msg_queue, 
		bool& stop)
{
    bind_worker_cpu();

	int ret = 0;
	assert(false == svr_ip.empty());

	auto client = cutils::make_unique<MsgCli>(svr_ip.c_str(), svr_port);
	assert(nullptr != client);

	ret = client->PrepareConnection();
	logerr("TEST %s %d START PrepareConnection ret %d", 
            svr_ip.c_str(), svr_port, ret);
	while (false == stop) {
        auto vec_msg = msg_queue.BatchPop(max_batch_size);
		assert(false == vec_msg.empty());

		ret = client->BatchPostPaxosMsg(vec_msg);
		if (0 != ret) {
			logerr("MsgCli::BatchPostPaxosMsg "
                    "vec_msg.size %zu ret %d", vec_msg.size(), ret);
		}
	}

	logerr("TEST %s END", svr_ip.c_str());
	return ;
}



} // namespace paxos


