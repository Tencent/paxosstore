#pragma once

#include "utils/Header.h"
//#include "iOssAttr.h"

inline void OssAttrInc(int iKey, int iID, int iCnt)
{

}

#if defined( __LIBCO_RDTSCP__)

inline uint64_t _GetTickCount()
{
    register uint32_t lo, hi;
    register uint64_t o;
    __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi) :: "%rcx");
    o = hi;
    o <<= 32;
    return (o | lo);
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

#endif

inline void SetRoutineID(uint32_t iID)
{

}
