
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "cutils/log_utils.h"
#include "memsize_mng.h"

#define _1G (1024L * 1024L * 1024L)


namespace memkv {

clsMemSizeMng::clsMemSizeMng() {
    FILE *fp = fopen("/proc/meminfo", "r");

    m_llUseSize = 0;
    m_llReserveSize = 1 * _1G;
    if (nullptr == fp) {
        m_llReserveSize = 1 * _1G;
        m_llTotalSize = 3 * _1G;
    }
    else {
        assert(fp != NULL);

        char buf[4096] = {0};
        fread(buf, 1, sizeof(buf), fp);
        fclose(fp);

        char *lp = strstr(buf, "MemTotal");
        assert(lp != NULL);

        lp += strlen("MemTotal");
        while (*lp == ' ' || *lp == '\t' || *lp == ':') {
            ++lp;
        }

        m_llTotalSize = strtoull(lp, NULL, 10);
        m_llTotalSize *= 1024;
    }

    printf("Total Memsize %lu G\n", m_llTotalSize / _1G);
}

clsMemSizeMng::~clsMemSizeMng() {}

void clsMemSizeMng::SetReserveMem(int32_t iReserveMem) {
    m_llReserveSize = iReserveMem * _1G;
    //	if(m_llTotalSize/_1G >= 28)
    //	{
    //		m_llReserveSize = iReserveMem * _1G;
    //	}
    //	else
    //	{
    //		m_llReserveSize =  _1G + (512 * 1024 * 1024);
    //	}
    printf("m_llReserveSize %lu G iReserveMem %d\n", m_llReserveSize / _1G,
           iReserveMem);
}

int clsMemSizeMng::GetTotalMemSize() { return m_llTotalSize / _1G; }

clsMemSizeMng *clsMemSizeMng::GetDefault() {
    static clsMemSizeMng oMemSizeMng;
    return &oMemSizeMng;
}

bool clsMemSizeMng::IsMemEnough(uint64_t iAllocSize) {
    if (m_llTotalSize >= m_llUseSize + iAllocSize + m_llReserveSize) {
        return true;
    }

    logerr("ERROR: TotalMemSize %.1lf G Used %.1lf G Reserve %.1lf G\n",
                 m_llTotalSize * 1.0 / _1G, m_llUseSize * 1.0 / _1G,
                 m_llReserveSize * 1.0 / _1G);
    return false;
}

void clsMemSizeMng::AddUseSize(uint64_t iUseSize) {
    m_llUseSize += iUseSize;

    printf("iNewAllocSize %lu m_llUseSize %.1lf G\n", iUseSize,
           m_llUseSize * 1.0 / _1G);

    logerr("MemInfo: TotalMemSize %.1lf G Used %.1lf G Reserve %.1lf G\n",
                 m_llTotalSize * 1.0 / _1G, m_llUseSize * 1.0 / _1G,
                 m_llReserveSize * 1.0 / _1G);
}


} // namespace memkv
