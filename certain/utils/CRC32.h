#ifndef CERTAIN_UTILS_CRC32_
#define CERTAIN_UTILS_CRC32_

#include "utils/Logger.h"

namespace leveldb
{
namespace crc32c
{

extern uint32_t Extend(uint32_t init_crc, const char* data, size_t n);
inline uint32_t Value(const char* data, size_t n) {
  return Extend(0, data, n);
}

}
}

namespace Certain
{

inline uint32_t ExtendCRC32(uint32_t iPrevCRC32, const char *pcData,
		uint32_t iLen)
{
	return leveldb::crc32c::Extend(iPrevCRC32, pcData, iLen);
}

inline uint32_t CRC32(const char* pcData, uint32_t iLen)
{
	return leveldb::crc32c::Value(pcData, iLen);
}

inline uint32_t CRC32(const string &strData)
{
	return CRC32(strData.c_str(), strData.size());
}

} // namespace Certain

#endif
