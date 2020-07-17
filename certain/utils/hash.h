#pragma once
#include "utils/header.h"

namespace certain {

uint32_t inline Hash(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  while (data + 4 <= limit) {
    uint32_t w = *(uint32_t*)(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  switch (limit - data) {
    case 3:
      h += static_cast<unsigned char>(data[2]) << 16;
    // not break, fall through
    case 2:
      h += static_cast<unsigned char>(data[1]) << 8;
    // not break, fall through
    case 1:
      h += static_cast<unsigned char>(data[0]);
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

inline uint32_t Hash(const char* data, uint32_t len) {
  return Hash(data, len, 20141208);
}

inline uint32_t Hash(const std::string& data) {
  return Hash(data.c_str(), data.size());
}

inline uint32_t Hash(uint64_t data) {
  return Hash((const char*)&data, sizeof(data));
}

}  // namespace certain
