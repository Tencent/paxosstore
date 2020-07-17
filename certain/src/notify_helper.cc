#include "src/notify_helper.h"

#include "co_routine.h"

namespace certain {

NotifyHelper::NotifyHelper() { max_num_ = 0; }

int NotifyHelper::Init(Options* options) {
  max_num_ = options->max_notify_num();
  if (eventfds_ != nullptr) {
    return kRetCodeInited;
  }
  idle_nid_list_.clear();
  eventfds_ = std::make_unique<int[]>(max_num_);
  for (uint32_t i = 0; i < max_num_; ++i) {
    eventfds_[i] = -1;
  }

  for (uint32_t i = 0; i < max_num_; ++i) {
    int fd = eventfd(0, EFD_NONBLOCK);
    if (fd == -1) {
      CERTAIN_LOG_FATAL("eventfd failed errno [%s]", strerror(errno));
      return -1;
    }

    eventfds_[i] = fd;
    idle_nid_list_.push_back(i);
  }

  return 0;
}

void NotifyHelper::Destroy() {
  int ret;
  assert(idle_nid_list_.size() == max_num_);
  assert(eventfds_ != nullptr);

  idle_nid_list_.clear();
  for (uint32_t i = 0; i < max_num_; ++i) {
    if (eventfds_[i] == -1) {
      continue;
    }

    ret = close(eventfds_[i]);
    if (ret != 0) {
      CERTAIN_LOG_FATAL("close failed errno [%s]", strerror(errno));
    }
    eventfds_[i] = 0;
  }

  eventfds_ = nullptr;
}

int NotifyHelper::TakeNid(uint32_t& nid) {
  ThreadLock lock(&mutex_);

  if (idle_nid_list_.empty()) {
    return kRetCodeNoIdleNotifier;
  }

  nid = idle_nid_list_.front();
  idle_nid_list_.pop_front();

  return 0;
}

void NotifyHelper::PutBackNid(uint32_t nid) {
  ThreadLock lock(&mutex_);
  idle_nid_list_.push_back(nid);
}

void NotifyHelper::NotifyNid(uint32_t nid) {
  assert(nid < max_num_);

  int fd = eventfds_[nid];
  uint64_t count = 1;
  int ret = write(fd, &count, sizeof(uint64_t));
  if (ret != sizeof(uint64_t)) {
    CERTAIN_LOG_FATAL("write failed ret %d errno [%s]", ret, strerror(errno));
    CERTAIN_PANIC();
  }
}

void NotifyHelper::WaitForNid(uint32_t nid) {
  int fd = eventfds_[nid];

  struct pollfd poll_fd = {0};
  poll_fd.events = POLLIN | POLLERR | POLLHUP;
  poll_fd.fd = fd;

  int ret;
  int timeout = 10000;
  int timeout_cnt = 0;

  while (true) {
    ret = poll(&poll_fd, 1, timeout);
    if (ret > 0) {
      break;
    } else if (ret == 0) {
      timeout_cnt++;
      CERTAIN_LOG_DEBUG("nid %u timeout_cnt %d x 100s", nid, timeout_cnt);
    } else {
      if (errno == EINTR) {
        continue;
      }
      CERTAIN_LOG_FATAL("poll failed errno [%s]", strerror(errno));
      CERTAIN_PANIC();
    }
  }

  uint64_t count = 0;
  ret = read(fd, &count, sizeof(count));
  if (ret != sizeof(count)) {
    CERTAIN_LOG_FATAL("read ret %d errno [%s]", ret, strerror(errno));
    CERTAIN_PANIC();
  }
}

}  // namespace certain
