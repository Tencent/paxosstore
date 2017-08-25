
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
#include "cutils/async_worker.h"



namespace paxos {

class Message;

class SmartPaxosMsgRecver;

void start_multiple_msg_svr(
		const char* svr_ip, 
		int svr_port, 
		size_t num_of_svr, 
		SmartPaxosMsgRecver& msg_reciver, 
		bool& stop);

} // namespace paxos


