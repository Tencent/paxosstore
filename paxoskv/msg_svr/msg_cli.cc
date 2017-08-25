
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cassert>
#include "msg_cli.h"
#include "core/paxos.pb.h"
#include "core/err_code.h"
#include "cutils/mem_utils.h"
#include "cutils/log_utils.h"


namespace {

int do_net_write(int fd, const char* data, int data_len)
{
	assert(0 <= fd);
	assert(NULL != data);
	assert(0 < data_len);

	int write_size = 0;
	while (write_size < data_len) {
		int small_write_size = write(
                fd, data + write_size, data_len - write_size);
		if (0 > small_write_size) {
			return small_write_size;
		}

		assert(0 <= small_write_size);
		write_size += small_write_size;
	}

	assert(write_size == data_len);
	return 0;
}

} // namespace 

namespace paxos {


MsgCli::MsgCli(const char* svr_ip, int svr_port)
	: svr_ip_(svr_ip)
	, svr_port_(svr_port)
	, socket_(-1)
{

}

MsgCli::~MsgCli()
{
	if (-1 != socket_) {
		close(socket_);
		socket_ = -1;
	}
}

int MsgCli::DoConnect()
{
	if (-1 != socket_) {
		assert(0 <= socket_);
		return 0;
	}

	assert(-1 == socket_);
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (0 > fd) {
		logerr("socket ret %d", fd);
		return fd;
	}

	assert(0 <= fd);
	struct sockaddr_in addr = {0};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(svr_port_);
	inet_pton(AF_INET, svr_ip_.c_str(), &(addr.sin_addr.s_addr));
	assert(0 != addr.sin_addr.s_addr);

	int ret = connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
	if (0 != ret) {
		close(fd);
		logerr("connect %s %d ret %d", svr_ip_.c_str(), svr_port_, ret);
		return -1;
	}

	assert(0 == ret);
	socket_ = fd;
	fd = -1;
	assert(0 <= socket_);
	return 0;
}

int MsgCli::PrepareConnection()
{
	return DoConnect();
}

int MsgCli::PostPaxosMsg(const paxos::Message& msg)
{
	int ret = 0;
	if (-1 == socket_) {
		ret = DoConnect();	
		if (0 != ret) {
			assert(-1 == socket_);
			logerr("DoConnect %s %d ret %d", svr_ip_.c_str(), svr_port_, ret);
			return ret;
		}

		assert(0 <= socket_);
	}
	assert(0 <= socket_);

	const size_t msg_size = sizeof(int) + msg.ByteSize();
	assert(0 < msg_size);

	send_buffer_.resize(msg_size);
	size_t one_msg_size = 0;
	ret = dump_msg(
            msg, &send_buffer_[0], send_buffer_.size(), one_msg_size);
	if (0 != ret) {
		logerr("dump_msg ret %d", ret);
		return ret;
	}

	assert(0 == ret);
	assert(msg_size == one_msg_size);
	ret = do_net_write(socket_, send_buffer_.data(), send_buffer_.size());
	if (0 != ret) {
		logerr("do_net_write %s %d socket_ %d send_buffer_.size %zu ret %d", 
				svr_ip_.c_str(), svr_port_, socket_, send_buffer_.size(), ret);
		close(socket_);
		socket_ = -1;
		ret = DoConnect();
		if (0 != ret) {
			assert(-1 == socket_);
			logerr("DoConnect %s %d ret %d", svr_ip_.c_str(), svr_port_, ret);
			return ret;
		}

		assert(-1 != socket_);
		ret = do_net_write(
                socket_, send_buffer_.data(), send_buffer_.size());
		if (0 != ret) {
			logerr("do_net_write AGAIN %s %d socket_ %d send_buffer_.size %zu ret %d", 
					svr_ip_.c_str(), svr_port_, socket_, send_buffer_.size(), ret);
			close(socket_);
			socket_ = -1;
			return ret;
		}
		assert(0 == ret);
	}

	assert(0 == ret);
	return 0;
}

int MsgCli::BatchPostPaxosMsg(
		const std::vector<std::unique_ptr<paxos::Message>>& vec_msg)
{
	if (vec_msg.empty()) {
		return 0;
	}

	int ret = 0;
	if (-1 == socket_) {
		ret = DoConnect();
		if (0 != ret) {
			assert(-1 == socket_);
			logerr("DoConnect %s %d ret %d", svr_ip_.c_str(), svr_port_, ret);
			return ret;
		}

		assert(0 <= socket_);
	}
	assert(0 <= socket_);

	assert(false == vec_msg.empty());
	size_t total_msg_size = 0;
	for (size_t idx = 0; idx < vec_msg.size(); ++idx) {
		assert(NULL != vec_msg[idx]);
		total_msg_size += (sizeof(int) + vec_msg[idx]->ByteSize());
	}

	assert(0 < total_msg_size);
	send_buffer_.resize(total_msg_size);
	size_t write_pos = 0;
	for (size_t idx = 0; idx < vec_msg.size(); ++idx) {
		assert(write_pos < send_buffer_.size());
		size_t one_msg_size = 0;
//		logerr("DEBUG: msgtype %d logid %lu index %lu from %u to %u", 
//				static_cast<int>(vec_msg[idx]->type()), 
//				vec_msg[idx]->logid(), vec_msg[idx]->index(), 
//				vec_msg[idx]->from(), vec_msg[idx]->to());
		ret = dump_msg(
                *(vec_msg[idx]), 
				&(send_buffer_[write_pos]), 
                send_buffer_.size() - write_pos, one_msg_size);
		if (0 != ret) {
			logerr("dump_msg vec_msg.size %zu idx %zu ret %d", 
                    vec_msg.size(), idx, ret);
			return ret;
		}

		assert(0 == ret);
		assert(0 < one_msg_size);
		assert(write_pos + one_msg_size <= send_buffer_.size());
		write_pos += one_msg_size;
	}

	assert(write_pos == total_msg_size);
	ret = do_net_write(socket_, send_buffer_.data(), send_buffer_.size());
	if (0 != ret) {
		logerr("do_net_write %s %d socket_ %d send_buffer_.size %zu ret %d", 
				svr_ip_.c_str(), svr_port_, socket_, send_buffer_.size(), ret);
		close(socket_);
		socket_ = -1;
		ret = DoConnect();
		if (0 != ret) {
			assert(-1 == socket_);
			logerr("DoConnect %s %d ret %d", svr_ip_.c_str(), svr_port_, ret);
			return ret;
		}

		assert(-1 != socket_);
		ret = do_net_write(
                socket_, send_buffer_.data(), send_buffer_.size());
		if (0 != ret) {
			logerr("do_net_write AGAIN %s %d socket_ %d send_buffer_.size %zu ret %d", 
					svr_ip_.c_str(), svr_port_, socket_, send_buffer_.size(), ret);
			close(socket_);
			socket_ = -1;
			return ret;
		}
		assert(0 == ret);
	}

	assert(0 == ret);
	return 0;
}

int send_msg(
        std::function<
            std::tuple<int, int, int>(const Message&)> pfn_route, 
        const Message& msg, 
        std::map<Endpoint, std::unique_ptr<MsgCli>>& map_client)
{
    assert(0 != msg.to());
    
    int ret = 0;
    Endpoint endpoint;
    {
        int ip = 0;
        int port = 0;
        std::tie(ret, ip, port) = pfn_route(msg);
        if (0 != ret) {
            logerr("pfn_route ret %d", ret);
            return ret;
        }

        assert(0 == ret);
        {
            char buf[32] = {0};
            struct sockaddr_in sa;
            sa.sin_addr.s_addr = ip;
            assert(nullptr != inet_ntop(
                        AF_INET, &sa.sin_addr, buf, sizeof(buf)));
            assert(0 < strnlen(buf, sizeof(buf)));
            endpoint = std::make_tuple(buf, port);
        }
    }

    if (map_client.end() == map_client.find(endpoint)) {
        map_client[endpoint] = 
            cutils::make_unique<MsgCli>(
                    std::get<0>(endpoint).c_str(), 
                    std::get<1>(endpoint));
        assert(nullptr != map_client.at(endpoint));
    }

    MsgCli* client = map_client.at(endpoint).get();
    assert(nullptr != client);

    ret = client->PostPaxosMsg(msg);
    if (0 != ret) {
        logerr("PostPaxosMsg type %d endpoint %s %d ret %d", 
                msg.type(), 
                std::get<0>(endpoint).c_str(), std::get<1>(endpoint), ret);
    }

    return ret;
}

} // namespace paxos


