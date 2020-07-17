#pragma once

#include "utils/header.h"
#include "utils/memory.h"

namespace certain {

struct WriteItem {
  std::unique_ptr<const char[]> raw_data;
  uint32_t len;
  uint32_t offset = 0;
  struct iovec* io_vec = nullptr;

  explicit WriteItem(std::unique_ptr<const char[]> _raw_data, uint32_t _len)
      : raw_data(std::move(_raw_data)), len(_len) {}
};

class WriteItemList {
 public:
  WriteItemList();
  ~WriteItemList();

  // raw_data will be deleted by the WriteItemList object.
  void AddWriteItem(std::unique_ptr<const char[]> raw_data, uint32_t len);

  // If there's one item at least, return a positive number not more then n.
  // The field iovec in write_item_list_'s element is set as &iovec[i].
  // Call CleanWrittenItems to cleanup all the iovec in the write_item_list_.
  // If there's no item to write return 0.
  int GetFirstNIterms(struct iovec* iovec, int n);

  // Call after GetFirstNIterms to Clean/Modify the iterms with iovec.
  // Return how many bytes is free.
  uint64_t CleanWrittenItems();

  bool empty() { return write_item_list_.empty(); }

  // Return how many bytes is free.
  uint64_t FreeAllWriteItems();

 private:
  std::list<std::unique_ptr<WriteItem>> write_item_list_;
};

}  // namespace certain
