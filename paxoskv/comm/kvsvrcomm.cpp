
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <memory.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdio>
#include <cassert>
#include "kvsvrcomm.h"




//uint64_t ntohll(uint64_t i)
//{
//	if (is_little_endian()) 
//	{
//		return (((uint64_t)ntohl((uint32_t)i)) << 32) | (uint64_t)(ntohl((uint32_t)(i >> 32)));
//	}
//	return i;
//}
//
//uint64_t htonll(uint64_t i)
//{
//	if (is_little_endian()) 
//	{
//		return (((uint64_t)htonl((uint32_t)i)) << 32) | ((uint64_t)htonl((uint32_t)(i >> 32)));
//	}
//	return i;
//}
//

bool is_little_endian(void)  
{
	int x = 1;  
	return (bool) (* (char *) &x);  
}  

uint32_t GenerateSectKV6(uint64_t iHashKey)
{
	uint32_t int1, int2;
	memcpy(&int1, &iHashKey, sizeof(uint32_t));
	memcpy(&int2, (char*)(&iHashKey) + sizeof(uint32_t), sizeof(uint32_t));
	return (int1 ^ int2)/(10000); 
}

uint32_t GenerateSect(uint64_t iHashKey)
{
	uint32_t int1, int2;
	memcpy(&int1, &iHashKey, sizeof(uint32_t));
	memcpy(&int2, (char*)(&iHashKey) + sizeof(uint32_t), sizeof(uint32_t));
	return (int1 ^ int2)/(100*10000); 
}


int GetCpuCount()
{
    return sysconf(_SC_NPROCESSORS_ONLN);
}


void BindToCpu(int begin, int end)
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

void BindWorkerCpu()
{
    int cpu_cnt = GetCpuCount();
    assert(3 <= cpu_cnt);
    BindToCpu(cpu_cnt-3, cpu_cnt-2);
}

void BindToLastCpu()
{
    int cpu_cnt = GetCpuCount();
    BindToCpu(cpu_cnt-1, cpu_cnt);
}


