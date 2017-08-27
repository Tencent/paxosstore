
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "cutils/async_worker.h"
#include "cutils/mem_utils.h"
#include "cutils/hash_utils.h"
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

const int MaxQueueSize = 10000;

struct KVSvrConfig
{
	std::string str_ip;
	int port;
	int member_id;
	int int_ip;
	uint64_t uni;

	KVSvrConfig() {}
	KVSvrConfig(const std::string& str_ip, int port, int member_id) :
		str_ip(str_ip), port(port), member_id(member_id)
	{
		int_ip = paxos::to_int_ip(str_ip);
		uni = (((uint64_t)int_ip) << 32) | (uint64_t)port;
	}
};



paxoskv::Option fake_option(int member_id)
{
	assert(member_id >= 1 && member_id <= 3);
	static int h[6][3] = {{1, 2, 3}, {1, 3, 2}, {2, 1, 3}, {2, 3, 1}, {3, 1, 2}, {3, 2, 1}};

    paxoskv::Option option;
    option.pfn_hash32 = 
        [](const std::string& key) -> uint32_t {
    		assert(sizeof(uint64_t) == key.size());
    		uint64_t logid = 0;
    		memcpy(&logid, key.data(), key.size());
            return cutils::dict_int_hash_func(logid);
        };
    option.pfn_selfid = 
        [=](const std::string& key) -> uint8_t {
			uint32_t hash = cutils::bkdr_hash(key.c_str());
			int idx = hash % 6;
			for (int i = 0; i < 3; ++i) {
				if (h[idx][i] == member_id) {
					return i + 1;
				}
			}
			assert(0);
			return 0;
        };
    option.pfn_member_route = 
        [](const paxos::Message& msg) -> std::tuple<int, int> {
			uint32_t hash = cutils::bkdr_hash(msg.key().c_str());
			int idx = hash % 6;
            return std::make_tuple(0, h[idx][msg.to() - 1]);
        };

	// update by hmemkv
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
	// update by hmemkv

	char dbPath[256], dbPlogPath[256];
	snprintf(dbPath, 256, "./example_kvsvr_%d", member_id);
	snprintf(dbPlogPath, 256, "./example_kvsvr_%d/data", member_id);
    assert(0 == dbcomm::CheckAndFixDirPath(dbPath));
    assert(0 == dbcomm::CheckAndFixDirPath(dbPlogPath));
    option.db_path.assign(dbPath);
    option.db_plog_path.assign(dbPlogPath);
    assert(0 == dbcomm::CheckAndFixDirPath(option.db_path));

	option.idx_shm_key = (0x202020 | member_id) << 4;
	option.data_block_shm_key = (0x303030 | member_id) << 4;
	printf("SHM %x %x\n", option.idx_shm_key, option.data_block_shm_key);

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

std::map<int, KVSvrConfig> GetFakeKVSvrConfigs()
{
	std::map<int, KVSvrConfig> ret;

	ret[1] = KVSvrConfig("127.0.0.1", 13301, 1);
	ret[2] = KVSvrConfig("127.0.0.1", 13302, 2);
	//ret[3] = KVSvrConfig("127.0.0.1", 13303, 3);

	return ret;
}

void BuildMsgQueue(const KVSvrConfig& config, const std::map<int, KVSvrConfig>& globalConfig,
    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>>& map_recv_queue,
    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>>& map_send_queue)
{
	for (const auto& item : globalConfig)
	{
		int mid = item.first;
		const auto& otherConfig = item.second;
		if (otherConfig.uni == config.uni) continue;

		// Launch all svrs in one machine
		//map_recv_queue[mid] = cutils::make_unique<paxos::PaxosMsgQueue>(
		//		std::to_string(config.member_id) + "-" + std::to_string(mid), MaxQueueSize);
		map_send_queue[mid] = cutils::make_unique<paxos::PaxosMsgQueue>(
				std::to_string(config.member_id) + "-" + std::to_string(mid), MaxQueueSize);
	}
	map_recv_queue[0] = cutils::make_unique<paxos::PaxosMsgQueue>(
			std::to_string(config.member_id) + "_", MaxQueueSize);
}

void LaunchKVSvr(const KVSvrConfig& config, const std::map<int, KVSvrConfig>& globalConfig, bool& stop)
{
	std::vector<std::unique_ptr<cutils::AsyncWorker>> vec_works;
    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>> map_recv_queue;
    std::map<int, std::unique_ptr<paxos::PaxosMsgQueue>> map_send_queue;
	BuildMsgQueue(config, globalConfig, map_recv_queue, map_send_queue);

	// Launch msgRecvSvr
    paxos::SmartPaxosMsgRecver msg_reciver(map_recv_queue);
	{
		auto msg_svr = cutils::make_unique<cutils::AsyncWorker>(
				paxos::start_multiple_msg_svr, config.str_ip.c_str(),
				config.port, 1, std::ref(msg_reciver));
		vec_works.push_back(std::move(msg_svr));
	}

	// Launch msgSendSvr
	{
		for (const auto& item : globalConfig)
		{
			int mid = item.first;
			const auto& otherConfig = item.second;
			if (otherConfig.uni == config.uni) continue;

    		auto msg_cli = cutils::make_unique<cutils::AsyncWorker>(
                paxos::send_msg_worker, otherConfig.str_ip.c_str(),
				otherConfig.port, 1, std::ref(*map_send_queue[mid]));
			vec_works.push_back(std::move(msg_cli));
		}
	}

	// hmemkv
    paxoskv::Option option = fake_option(config.member_id);
    paxoskv::clsHardMemKv hmemkv;
    int ret = hmemkv.Init(option);
    assert(0 == ret);
    paxoskv::update_option(hmemkv, option);

	// DB
    paxos::SmartPaxosMsgSender msg_sender(option.pfn_member_route, map_send_queue); 
    paxoskv::DBImpl db_impl(config.member_id, option);
    ret = db_impl.Init(&msg_sender);
    assert(0 == ret);
    ret = db_impl.StartGetLocalOutWorker();
    assert(0 == ret);
    ret = db_impl.StartTimeoutWorker();
    assert(0 == ret);

	// Post paxos msg to db
	{
		auto svr = cutils::make_unique<cutils::AsyncWorker>(
				paxos::post_msg_worker, 1, std::ref(*map_recv_queue[0]), 
				[&db_impl](const paxos::Message& msg){return db_impl.PostPaxosMsg(msg);});
		vec_works.push_back(std::move(svr));
	}

	logdebug("Launch kvsvr_%d in %s:%d.", 
			config.member_id, config.str_ip.c_str(), config.port);

	// Try to write
	while (!stop)
	{
		sleep(2);

		if (config.member_id == 1) 
		{
			uint64_t logid = 10;
			std::string key;
			key.resize(sizeof(uint64_t));
			key.assign((char*)(&logid), sizeof(uint64_t));

			uint64_t forward_reqid = 0;
			uint32_t  new_version = 0;
			ret = db_impl.Set(key, "wechat", 0, forward_reqid, new_version);
			printf("set: %d, ret: %d, %lu %u\n", 
					config.member_id, ret, forward_reqid, new_version);
		}
	}
}

int main(int argc, char* agrv[]) 
{
	std::map<int, KVSvrConfig> kvsvrConfigs = GetFakeKVSvrConfigs();

	std::vector<std::unique_ptr<cutils::AsyncWorker>> vec_works;
	for (const auto& config : kvsvrConfigs)
	{
		auto svr = cutils::make_unique<cutils::AsyncWorker>(
				LaunchKVSvr, config.second, kvsvrConfigs);
		vec_works.push_back(std::move(svr));
	}

	sleep(100);

	return 0;
}
