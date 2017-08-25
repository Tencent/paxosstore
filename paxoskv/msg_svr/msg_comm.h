
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once 

#include <string>
#include <vector>
#include <map>
#include <memory>



namespace paxos {


class Message;
class SmartPaxosMsgRecver;

using Endpoint = std::tuple<std::string, int>;



int dump_msg(
		const paxos::Message& msg, 
		char* buffer, size_t buffer_size, size_t& msg_size);


int pickle_msg(
		const char* buffer, size_t buffer_size, 
		size_t& msg_size, paxos::Message& msg);

size_t enqueue_msg(
		SmartPaxosMsgRecver& msg_reciver, 
		std::map<int, 
			std::vector<std::unique_ptr<
                paxos::Message>>>& map_msg_buffer);


// TODO
int listen_at(const char* svr_ip, int svr_port, int backlog);

int set_non_block(int socket);


int get_cpu_count();

void bind_to_cpu(int begin, int end);

void bind_worker_cpu();


std::string to_string_ip(int ip);

int to_int_ip(const std::string& ip);

} // namespace paxos


