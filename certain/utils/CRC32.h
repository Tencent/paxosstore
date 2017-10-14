#ifndef CERTAIN_UTILS_CRC32_
#define CERTAIN_UTILS_CRC32_

#include "utils/Logger.h"

namespace Certain
{

uint32_t CRC32(uint32_t iPrevCRC32, const char *pcData, uint32_t iLen);

inline uint32_t CRC32(const char* pcData, uint32_t iLen)
{
    return CRC32(0, pcData, iLen);
}

inline uint32_t CRC32(const string &strData)
{
    return CRC32(strData.c_str(), strData.size());
}

} // namespace Certain

#endif
