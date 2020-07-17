#include "utils/mem_pool.h"

namespace certain {

MemPool::MemPool(uint64_t max_count, uint64_t size) {
  assert(max_count > 0);
  assert(size > 0);

  max_count_ = max_count;
  size_ = size;

  pool_alloc_cnt_ = 0;
  os_alloc_cnt_ = 0;

  indexs_ = std::make_unique<uint64_t[]>(max_count);
  first_index_ = 0;
  last_index_ = max_count;

  for (uint64_t i = 0; i < max_count; ++i) {
    indexs_[i] = i;
  }

  buffer_ = std::make_unique<char[]>(max_count * size);
}

MemPool::~MemPool() {}

char* MemPool::Alloc(uint64_t size) {
  if (size == 0) {
    return NULL;
  }

  assert(last_index_ >= first_index_);
  assert(last_index_ - first_index_ <= max_count_);
  if (size > size_ || last_index_ - first_index_ == 0) {
    os_alloc_cnt_++;
    return (char*)malloc(size);
  }

  char* data = buffer_.get() + (indexs_[first_index_ % max_count_] * size_);
  first_index_++;

  pool_alloc_cnt_++;

  return data;
}

void MemPool::Free(char* data) {
  if (data == NULL) {
    return;
  }

  char* limit = buffer_.get() + max_count_ * size_;
  if (data < buffer_.get() || limit <= data) {
    free(data);

    assert(os_alloc_cnt_ > 0);
    os_alloc_cnt_--;

    return;
  }

  uint64_t offset = data - buffer_.get();
  assert(offset % size_ == 0);

  uint64_t index = offset / size_;
  indexs_[last_index_ % max_count_] = index;
  last_index_++;

  assert(pool_alloc_cnt_ > 0);
  pool_alloc_cnt_--;
}

std::string MemPool::ToString() {
  char buf[100];
  sprintf(buf, "max_count %lu size %lu pool_alloc_cnt %lu os_alloc_cnt %lu",
          max_count_, size_, pool_alloc_cnt_, os_alloc_cnt_);
  return buf;
}

}  // namespace certain
