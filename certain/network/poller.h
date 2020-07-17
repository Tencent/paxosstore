#pragma once

#include "network/fd_obj.h"
#include "network/tcp_socket.h"

namespace certain {

class Poller {
 private:
  int epoll_fd_;

  uint32_t event_size_;
  epoll_event* epoll_events_;

 public:
  static const int kDefaultEventSize = 8096;

  Poller(int events_size = kDefaultEventSize);
  ~Poller();

  int Add(FdObj* fd_obj);
  int Remove(FdObj* fd_obj);
  int RemoveAndCloseFd(FdObj* fd_obj);
  int Modify(FdObj* fd_obj);

  void RunOnce(int msec);
};

}  // namespace certain
