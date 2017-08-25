
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sched.h>
#include <cstring>
#include <cassert>
#include "msg_comm.h"
#include "core/paxos.pb.h"
#include "core/err_code.h"
#include "cutils/log_utils.h"
#include "msg_helper.h"


namespace paxos {


int dump_msg(
		const paxos::Message& msg, 
		char* buffer, size_t buffer_size, size_t& msg_size)
{
	msg_size = 0;
	if (msg.ByteSize() + sizeof(int) > buffer_size) {
		return 1;
	}

	assert(NULL != buffer);
	int total_len = msg.ByteSize();
	total_len = htonl(total_len);
	memcpy(buffer, &total_len, sizeof(int));

	if (false == msg.SerializeToArray(
				buffer + sizeof(int), buffer_size - sizeof(int)))
	{
		return SERIALIZE_PAXOS_MESSAGE_ERR;
	}

	msg_size = sizeof(int) + msg.ByteSize();
//	logerr("TESTINFO msgtype %d logid %lu index %lu from %u to %u", 
//			static_cast<int>(msg.type()), msg.logid(), msg.index(), msg.from(), msg.to());
	return 0;
}

int pickle_msg(
		const char* buffer, 
		size_t buffer_size, 
		size_t& msg_size, 
		paxos::Message& msg)
{
	msg_size = 0;
	if (sizeof(int) > buffer_size) {
		return 1; // too short
	}

	int total_len = *(reinterpret_cast<const int*>(buffer));
	total_len = ntohl(total_len);
	if (0 >= total_len) {
		logerr("ERROR buffer_size %zu "
                "total_len %d", buffer_size, total_len);
		return -1;	
	}

	if (sizeof(int) + total_len > buffer_size) {
		return 1;
	}

	msg_size = total_len + sizeof(int);
	const char* data = buffer + sizeof(int);
	if (false == msg.ParseFromArray(data, msg_size - sizeof(int))) {
		return BROKEN_PAXOS_MESSAGE;
	}

//	logerr("TESTINFO msgtype %d logid %lu index %lu from %u to %u", 
//			static_cast<int>(msg.type()), msg.logid(), msg.index(), msg.from(), msg.to());
	// else
	return 0;
}


size_t enqueue_msg(
		SmartPaxosMsgRecver& msg_reciver, 
		std::map<int, 
			std::vector<std::unique_ptr<
                paxos::Message>>>& map_msg_buffer)
{
	for (auto iter = map_msg_buffer.begin(); 
            iter != map_msg_buffer.end(); ++iter) {
		if (iter->second.empty()) {
			continue;
		}

		//// MEM-LEAK: TEST
		//auto vecMsg = std::move(iter->second);
		msg_reciver.BatchRecvMsg(iter->first, std::move(iter->second));
		assert(iter->second.empty());
	}

	return 0;
}


int get_cpu_count()
{
    return sysconf(_SC_NPROCESSORS_ONLN);
}

void bind_to_cpu(int begin, int end)
{
#if !defined(__APPLE__)
    cpu_set_t mark;
    CPU_ZERO(&mask);
    for (int i = begin; i < end; ++i) {
        CPU_SET(i, &mask);
    }

    // get thread id
    pid_t threadid = gettid();
    sched_setaffinity(threadid, sizeof(mask), &mask);
#endif
}

void bind_worker_cpu()
{
    int cpu_cnt = get_cpu_count();
    assert(3 <= cpu_cnt);
    bind_to_cpu(cpu_cnt-3, cpu_cnt-2);
}


std::string to_string_ip(int ip) 
{
    char buffer[32] = {0};
    {
        struct sockaddr_in sa;
        sa.sin_addr.s_addr = ip;
        assert(nullptr != inet_ntop(
                    AF_INET, &sa.sin_addr, buffer, sizeof(buffer)));
    }

    return std::string(buffer);
}

int to_int_ip(const std::string& ip)
{
    if (ip.empty()) {
        return 0;
    }

    struct in_addr addr;
    int ret = inet_aton(ip.c_str(), &addr);
    if (1 != ret) {
        return -1;
    }

    return static_cast<int>(addr.s_addr);
}


int listen_at(const char* svr_ip, int svr_port, int backlog)
{
    assert(nullptr != svr_ip);
    int listenfd = -1;
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (0 > listenfd) {
        return -1;
    }

    assert(0 <= listenfd);
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(struct sockaddr_in));
    sin.sin_family = PF_INET;
    sin.sin_addr.s_addr = inet_addr(svr_ip); 
    sin.sin_port = htons(svr_port);

    int option = 1;
    if (-1 == setsockopt(
                listenfd, SOL_SOCKET, SO_REUSEADDR, 
                reinterpret_cast<char*>(&option), sizeof(int))) {
        close(listenfd);
        return -1;
    }

    if (0 > bind(listenfd, 
                reinterpret_cast<struct sockaddr*>(&sin), 
                sizeof(sin))) {
        close(listenfd);
        return -1;
    }

    if (0 > listen(listenfd, backlog)) {
        close(listenfd);
        return -1;
    }

    return listenfd;
}

int set_non_block(int socket)
{
    if (0 > socket) {
        return -1;
    }

    assert(0 <= socket);
    int flags = fcntl(socket, F_GETFL);
    if (0 > flags) {
        return flags;
    }

    flags |= O_NONBLOCK;
    if (0 > fcntl(socket, F_SETFL, flags)) {
        return -1;
    }

    return 0;
}

} // namespace paxos

