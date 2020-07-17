#pragma once

#include "utils/header.h"

namespace certain {

// Not thread safe for performance, not to use in multithread.
class SharedLimiter {
 public:
  SharedLimiter(uint64_t max_total_size) {
    max_total_size_ = max_total_size;
    curr_total_size_ = 0;
  }
  ~SharedLimiter() {}

  inline bool AllocBytes(uint64_t n) {
    if (curr_total_size_ + n > max_total_size_) {
      return false;
    }
    curr_total_size_ += n;
    return true;
  }

  inline void FreeBytes(uint64_t n) {
    assert(curr_total_size_ >= n);
    curr_total_size_ -= n;
  }

  std::string ToString() {
    int len = 60;
    char buf[len];
    snprintf(buf, len, "%lu/%lu", curr_total_size_, max_total_size_);
    return buf;
  }

  uint64_t curr_total_size() { return curr_total_size_; }

 private:
  uint64_t max_total_size_;
  uint64_t curr_total_size_;
};

class CapacityLimiter {
 public:
  CapacityLimiter(uint64_t max_capacity, SharedLimiter* shared) {
    max_capacity_ = max_capacity;
    curr_capacity_ = 0;
    shared_ = shared;
  }
  ~CapacityLimiter() {}

  inline bool AllocBytes(uint64_t n) {
    if (curr_capacity_ + n > max_capacity_) {
      return false;
    }

    if (shared_->AllocBytes(n)) {
      curr_capacity_ += n;
      return true;
    }

    return false;
  }

  inline void FreeBytes(uint64_t n) {
    if (n) {
      assert(curr_capacity_ >= n);
      curr_capacity_ -= n;
      shared_->FreeBytes(n);
    }
  }

  std::string ToString() {
    int len = 60;
    char buf[len];
    snprintf(buf, len, "%lu/%lu %s", curr_capacity_, max_capacity_,
             shared_->ToString().c_str());
    return buf;
  }

 private:
  uint64_t max_capacity_;
  uint64_t curr_capacity_;
  SharedLimiter* shared_;
};

}  // namespace certain
