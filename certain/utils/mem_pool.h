#pragma once

#include "utils/header.h"
#include "utils/memory.h"

namespace certain {

class MemPool {
 public:
  MemPool(uint64_t max_count, uint64_t size);
  ~MemPool();

  char* Alloc(uint64_t size);
  void Free(char* data);

  std::string ToString();

  // For test.
  uint64_t pool_alloc_cnt() { return pool_alloc_cnt_; }
  uint64_t os_alloc_cnt() { return os_alloc_cnt_; }

 private:
  uint64_t max_count_;
  uint64_t size_;

  // For stat.
  uint64_t pool_alloc_cnt_;
  uint64_t os_alloc_cnt_;

  std::unique_ptr<uint64_t[]> indexs_;
  uint64_t first_index_;
  uint64_t last_index_;

  std::unique_ptr<char[]> buffer_;
};

}  // namespace certain
