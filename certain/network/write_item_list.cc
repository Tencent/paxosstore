#include "network/write_item_list.h"

namespace certain {

WriteItemList::WriteItemList() {}

WriteItemList::~WriteItemList() { assert(write_item_list_.empty()); }

void WriteItemList::AddWriteItem(std::unique_ptr<const char[]> raw_data,
                                 uint32_t len) {
  write_item_list_.push_back(
      std::make_unique<WriteItem>(std::move(raw_data), len));
}

int WriteItemList::GetFirstNIterms(struct iovec* io_vec, int n) {
  if (write_item_list_.empty()) {
    return 0;
  }

  int cnt = 0;

  for (int i = 0; i < n; ++i) {
    if (write_item_list_.front()->io_vec != nullptr) {
      break;
    }

    auto write_item = std::move(write_item_list_.front());
    write_item_list_.pop_front();

    write_item->io_vec = &io_vec[i];
    const char* data = write_item->raw_data.get();
    uint32_t len = write_item->len;

    io_vec[i].iov_base = (char*)data + write_item->offset;
    io_vec[i].iov_len = len - write_item->offset;

    write_item_list_.push_back(std::move(write_item));
    cnt++;
  }

  return cnt;
}

uint64_t WriteItemList::CleanWrittenItems() {
  bool done = false;
  uint64_t total_free_size = 0;

  while (!write_item_list_.empty()) {
    if (write_item_list_.back()->io_vec == nullptr) {
      break;
    }

    auto write_item = std::move(write_item_list_.back());
    write_item_list_.pop_back();

    char* iov_base = static_cast<char*>(write_item->io_vec->iov_base);
    uint32_t iov_len = write_item->io_vec->iov_len;
    assert(iov_base + iov_len == write_item->raw_data.get() + write_item->len);
    assert(iov_len <= write_item->len);

    if (iov_len == 0) {
      total_free_size += write_item->len;
      done = true;
      continue;
    }
    write_item->offset = write_item->len - iov_len;
    write_item->io_vec = nullptr;

    write_item_list_.push_front(std::move(write_item));

    // For check.
    assert(!done);
  }

  return total_free_size;
}

uint64_t WriteItemList::FreeAllWriteItems() {
  uint64_t total_free_size = 0;
  for (auto& write_item : write_item_list_) {
    total_free_size += write_item->len;
  }
  write_item_list_.clear();
  return total_free_size;
}

}  // namespace certain
