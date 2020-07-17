#pragma once

#include "utils/header.h"

namespace certain {

inline uint64_t GetTimeByUsec() {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

inline uint64_t GetTimeByMsec() {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

inline uint64_t GetTimeBySecond() {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  return (uint64_t)tv.tv_sec;
}

inline uint32_t GetCurrentHour() {
  time_t iNow = time(NULL);
  struct tm stm;
  if (localtime_r(&iNow, &stm) == NULL) {
    return uint32_t(-1);
  }
  return stm.tm_hour;
}

}  // namespace certain
