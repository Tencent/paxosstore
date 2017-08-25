
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
#include <memory>
#include <map>
#include <functional>
#include "msg_comm.h"

namespace paxos {

class Message;

class MsgCli {

public:
	MsgCli(const char* svr_ip, int svr_port);

	~MsgCli();

	int PostPaxosMsg(const paxos::Message& msg);

	int BatchPostPaxosMsg(
			const std::vector<std::unique_ptr<paxos::Message>>& vec_msg);

	int PrepareConnection();

private:
	int DoConnect();

private:
	const std::string svr_ip_;
	const int svr_port_;

	int socket_;
	std::string send_buffer_;
}; 


// TODO: resolve route ?
int send_msg(
        std::function<
            std::tuple<int, int, int>(const Message&)> pfn_route, 
        const Message& msg, 
        std::map<Endpoint, std::unique_ptr<MsgCli>>& map_client); 

} // namespace paxos

