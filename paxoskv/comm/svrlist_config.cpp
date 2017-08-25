
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <stdio.h>
#include <string.h>
#include <netinet/in.h> 
#include <unistd.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <cassert>
#include "cutils/log_utils.h"
#include "svrlist_config.h"
#include "iconfig.h"
#include "kvsvrcomm.h"


clsSvrListConfig::clsSvrListConfig(
        const char * svrListPath, const char * selfIP)
	: clsSvrListConfigBase()
{
	SetSelfIP(selfIP);	
	SetSvrListPath(svrListPath);

	assert(0 == UpdateConfig());
}

clsSvrListConfig * clsSvrListConfig::GetInstance(
		const char * svrListPath, const char * selfIP)
{
	static clsSvrListConfig instance(svrListPath, selfIP);
	return &instance;
}

static void * UpdateConfigThread(void * arg)
{
	clsSvrListConfig * config = (clsSvrListConfig *)arg;
	while(true)
	{
		int ret = config->UpdateConfig();
		logerr("\033[31m %s: %s:%d clsSvrListConfig::UpdateConfig ret %d \033[0m\n", 
				(ret == 0 ? "INFO" : "ERR"), __FILE__, __LINE__, ret);
		sleep(1);
	}
	return NULL;
}

void clsSvrListConfig::StartUpdateConfigThread()
{
	pthread_t tid;
	assert(0 == pthread_create(&tid, NULL, UpdateConfigThread, this));
}


int clsSvrListConfig::UpdateConfig()
{
    Comm::CConfig reader(GetSvrListPath());
	int ret = reader.Init();
	if(ret < 0)
	{
		logerr("ERR: %s[%d] CConfig::Init %s ret %d\n", 
				__FILE__, __LINE__, GetSvrListPath(), ret);
		return ret;
	}

	return LoadConfigFromCConfig(reader);
}

#if 0
static void PrintGroup(const SvrGroup_t & group)
{
	if(group.iCountAB == 0)
	{
		return;
	}
	printf("SVRCount=%u\n", group.iCountAB);
	for(uint32_t index = 0; index < group.iCountAB; ++index)
	{
		struct in_addr stIP = {(in_addr_t)ntohl(group.tAddrAB[index].iIP)};
		printf("SVR%u=%s\n", index, group.tAddrAB[index].GetIP());
	}
	printf("SVR_C_Count=%u\n", group.iCountC);
	for(uint32_t index = 0; index < group.iCountC; ++index)
	{
		struct in_addr stIP = {(in_addr_t)ntohl(group.tAddrC[index].iIP)};
		printf("SVR%u=%s\n", index, group.tAddrC[index].GetIP());
	}
	printf("Sect_Begin=%u\nSect_End=%u\n", group.iBegin, group.iEnd);
}
int main(int argc, char ** argv)
{
	if(argc < 2)
	{
		printf("%s svr_list.conf local.ip\n", argv[0]);
		return 0;
	}

	clsSvrListConfig * config = clsSvrListConfig::GetInstance(argv[1], argv[2]);
	config->StartUpdateConfigThread();

	struct timeval start, end;
	gettimeofday(&start, NULL);
	while(true)
	{
		SvrGroupList_t grouplist;
		config->GetSvrGroupList(grouplist);
		for(int i=0; i<grouplist.iGroupCnt; i++)
			PrintGroup(grouplist.tSvrGroup[i]);
		printf("MachineC %d\n", grouplist.iMachineC);
		printf("\n\n\n");
		sleep(1);
	}
	gettimeofday(&end, NULL);
	float used = (end.tv_sec - start.tv_sec) * 1000 + float(end.tv_usec - start.tv_usec) / 1000;
	printf("time used %0.2f\n", used);
	return 0;
}
#endif

#if 0
int main(int argc, char ** argv)
{
	if(argc < 2)
	{
		printf("%s svr_list.conf local.ip\n", argv[0]);
		return 0;
	}

	/*
	SvrGroup_t empty;
	bzero(&empty, sizeof(empty));
	PrintGroup(empty);
	*/
	clsSvrListConfig * config = clsSvrListConfig::GetInstance(argv[1], argv[2]);

	struct timeval start, end;
	gettimeofday(&start, NULL);
	for(int index = 0; index < 100 * 10000; ++index)
	{
		SvrGroup_t group;
		config->GetSvrGroup(group);
		if(index % 10000 == 0)
		{
			printf("index %u\n", index);
		}
	}
	gettimeofday(&end, NULL);
	float used = (end.tv_sec - start.tv_sec) * 1000 + float(end.tv_usec - start.tv_usec) / 1000;
	printf("time used %0.2f\n", used);
	return 0;
}
#endif

