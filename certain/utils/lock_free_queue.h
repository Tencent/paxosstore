#pragma once

#include "certain/errors.h"
#include "utils/header.h"
#include "utils/memory.h"

namespace certain {

// Lock-free Queue based on Ring Buffer
template <typename T>
class LockFreeQueue {
 public:
  explicit LockFreeQueue(uint64_t capacity)
      : capacity_(capacity),
        items_(std::make_unique<std::unique_ptr<T>[]>(capacity)) {}

  uint64_t Size() const {
    auto t = tail_.load();
    auto h = head_.load();
    return h - t;
  }

  bool Empty() const { return Size() == 0; }

  bool Full() const { return Size() >= capacity_; }

  template <class O>
  int PopByOneThread(std::unique_ptr<O>* item) {
    static_assert(std::is_same<T, O>::value || std::is_base_of<T, O>::value,
                  "Type Mismatch");

    auto t = tail_.load();
    auto h = head_.load();

    if (h == t) {
      return kUtilsQueueEmpty;
    }

    auto& pop = items_[t % capacity_];
    if (pop == nullptr) {
      return kUtilsQueueConflict;
    }
    unique_cast<T>(*item) = std::move(pop);

    tail_.fetch_add(1);
    return 0;
  }

  template <class O>
  int PushByMultiThread(std::unique_ptr<O>* item, int retry_times = 5) {
    static_assert(std::is_same<T, O>::value || std::is_base_of<T, O>::value,
                  "Type Mismatch");

    if (item->get() == nullptr) {
      return kUtilsInvalidArgs;
    }

    for (int i = 0; i < retry_times || retry_times == -1; ++i) {
      auto t = tail_.load();
      auto h = head_.load();

      if (h - t >= capacity_) {
        return kUtilsQueueFull;
      }

      // CAS
      if (head_.compare_exchange_strong(h, h + 1)) {
        unique_cast<T>(*item).swap(items_[h % capacity_]);
        return 0;
      }
    }
    return kUtilsQueueConflict;
  }

 private:
  static constexpr size_t kCacheLineSize = 64;
  alignas(kCacheLineSize) std::atomic<uint64_t> head_{0};
  alignas(kCacheLineSize) std::atomic<uint64_t> tail_{0};

  alignas(kCacheLineSize) const uint64_t capacity_;
  std::unique_ptr<std::unique_ptr<T>[]> items_;
};

}  // namespace certain
