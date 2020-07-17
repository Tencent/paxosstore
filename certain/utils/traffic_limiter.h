#pragma once
#include <cstdint>

#include "utils/time.h"

namespace certain {

class CountLimiter {
 public:
  void UpdateCount(uint32_t max_count_per_second) {
    if (max_count_per_second == max_count_per_second_) {
      return;
    }

    max_count_per_second_ = max_count_per_second;
    assert(max_count_per_second_ > 0);

    next_time_by_count_ = 0;
    remain_count_ = 0;
  }

  bool AcquireOne() {
    if (remain_count_ > 0) {
      remain_count_--;
      return true;
    }

    uint64_t now = GetTimeByMsec();
    if (now < next_time_by_count_) {
      return false;
    }

    remain_count_ = max_count_per_second_ - 1;
    next_time_by_count_ = now + 1000;
    return true;
  }

 private:
  uint32_t max_count_per_second_ = -1;
  uint32_t remain_count_ = 0;
  uint64_t next_time_by_count_ = 0;
};

class TrafficLimiter {
 public:
  void UpdateSpeed(uint64_t max_bytes_per_second) {
    uint64_t max_bytes_per_interval = max_bytes_per_second * kIntervalMS / 1000;

    if (max_bytes_per_interval == max_bytes_per_interval_) {
      return;
    }

    max_bytes_per_interval_ = max_bytes_per_interval;
    assert(max_bytes_per_interval_ > 0);

    next_time_by_bytes_ = 0;
    remain_bytes_ = 0;
  }

  uint64_t UseBytes(uint64_t bytes) {
    if (remain_bytes_ >= bytes) {
      remain_bytes_ -= bytes;
      return 0;
    }

    uint64_t now = GetTimeByMsec();
    if (now < next_time_by_bytes_) {
      return next_time_by_bytes_ - now;
    }

    if (bytes <= max_bytes_per_interval_) {
      remain_bytes_ = max_bytes_per_interval_ - bytes;
      next_time_by_bytes_ = now + kIntervalMS;
      return 0;
    }

    next_time_by_bytes_ = now + kIntervalMS * bytes / max_bytes_per_interval_;
    remain_bytes_ = 0;
    return 0;
  }

  void UpdateCount(uint32_t max_count_per_second) {
    if (max_count_per_second == max_count_per_second_) {
      return;
    }

    max_count_per_second_ = max_count_per_second;
    assert(max_count_per_second_ > 0);

    next_time_by_count_ = 0;
    remain_count_ = 0;
  }

  uint64_t UseCount(uint32_t count = 1) {
    if (remain_count_ >= count) {
      remain_count_ -= count;
      return 0;
    }

    uint64_t now = GetTimeByMsec();
    if (now < next_time_by_count_) {
      return next_time_by_count_ - now;
    }

    if (count <= max_count_per_second_) {
      remain_count_ = max_count_per_second_ - count;
      next_time_by_count_ = now + 1000;
      return 0;
    }

    next_time_by_count_ = now + 1000 * count / max_count_per_second_;
    remain_count_ = 0;
    return 0;
  }

 private:
  static constexpr uint32_t kIntervalMS = 10;

  uint64_t max_bytes_per_interval_ = -1;
  uint64_t remain_bytes_ = 0;
  uint64_t next_time_by_bytes_ = 0;

  uint32_t max_count_per_second_ = -1;
  uint32_t remain_count_ = 0;
  uint64_t next_time_by_count_ = 0;
};

}  // namespace certain
