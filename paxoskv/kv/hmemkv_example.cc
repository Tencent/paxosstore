
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/




#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <string>
#include "dbcomm/db_comm.h"
#include "hard_memkv.h"
#include "db_option.h"
#include "core/paxos.pb.h"



    int
main ( int argc, char *argv[] )
{
    paxoskv::clsHardMemKv hmemkv;

    paxoskv::Option option;
    option.db_path = "./example_kvsvr";
    option.db_plog_path = "./example_kvsvr/data";
    assert(0 == dbcomm::CheckAndFixDirPath(option.db_path.c_str()));
    assert(0 == dbcomm::CheckAndFixDirPath(option.db_plog_path.c_str()));

    auto ret = hmemkv.Init(option);
    assert(0 == ret);

	uint64_t logid = rand();
	std::string key;
	key.resize(sizeof(logid));
	memcpy((void*)(key.data()), &logid, sizeof(logid));
	paxos::PaxosLogHeader header, header2;
	paxos::PaxosLog plog, plog2;

	ret = hmemkv.Read(key, header, plog);
	printf("Read %lu: ret %d, max_index: %lu\n", 
			logid, ret, header.max_index());

	header.set_max_index(100);
	plog.clear_entries();
	auto paxosInstance = plog.add_entries();
	paxosInstance->set_index(100);
	assert(plog.entries_size() == 1);

	ret = hmemkv.Write(key, header, plog);
	printf("Write %lu: ret %d\n", logid, ret);

	ret = hmemkv.Read(key, header2, plog2);
	printf("Read %lu: ret %d, max_index: %lu\n", 
			logid, ret, header2.max_index());

	assert(ret == 0);
	assert(header2.max_index() == 100);
	assert(plog2.entries_size() == 1);
	assert(plog2.entries(0).index() == 100);

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
