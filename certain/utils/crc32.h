#pragma once

#include <string>

namespace certain {

uint32_t crc32(uint32_t prev_crc32, const char* data, uint32_t len);

inline uint32_t crc32(const char* data, uint32_t len) {
  return crc32(0, data, len);
}

inline uint32_t crc32(const std::string& data) {
  return crc32(data.c_str(), data.size());
}

}  // namespace certain
