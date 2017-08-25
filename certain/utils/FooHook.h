
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "utils/Header.h"

inline void OssAttrInc(int iKey, int iID, int iCnt)
{

}

inline uint64_t _GetTickCount()
{
    register uint32_t lo, hi;
    register uint64_t o;
    __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
    o = hi;
    o <<= 32;
    return (o | lo);
}

inline pid_t GetThreadPid()
{
    return 0;
}

inline uint64_t getCpuKhz()
{
    FILE* fp = fopen("/proc/cpuinfo", "r");
    if (!fp)
        return 1;
    char buf[4096] = {0};
    fread(buf, 1, sizeof(buf), fp);
    fclose(fp);

    char* lp = strstr(buf, "cpu MHz");
    if (!lp)
        return 1;
    lp += strlen("cpu MHz");
    while (*lp == ' ' || *lp == '\t' || *lp == ':')
    {
        ++lp;
    }

    double mhz = atof(lp);
    uint64_t u = (uint64_t)(mhz * 1000);
    return u;
}

inline uint64_t _GetCpuHz()
{
    static uint64_t u = getCpuKhz() * 1000;
    return u;
}

inline void SetRoutineID(uint32_t iID)
{

}
