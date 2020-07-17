#pragma once

#include "utils/header.h"
#include "utils/macro_helper.h"

namespace certain {

class FdObj;
class HandlerBase {
 public:
  virtual ~HandlerBase() {}
  virtual int HandleRead(FdObj* fd_obj) = 0;
  virtual int HandleWrite(FdObj* fd_obj) = 0;
};

class FdObj {
 private:
  int fd_;

  HandlerBase* handler_;
  int events_;

  bool readable_;
  bool writable_;
  bool broken_;

 public:
  static const int kDefaultEvents = EPOLLIN | EPOLLOUT | EPOLLET;

  FdObj(int fd, HandlerBase* handler, int events = kDefaultEvents)
      : fd_(fd),
        handler_(handler),
        events_(events),
        readable_(false),
        writable_(false),
        broken_(false) {}

  virtual ~FdObj() {}

  int fd() { return fd_; }

  int events() { return events_; }

  CERTAIN_GET_SET(bool, readable);
  CERTAIN_GET_SET(bool, writable);

  // Set by callers when the fd would be aborted.
  CERTAIN_GET_SET(bool, broken);

  CERTAIN_GET_SET_PTR(HandlerBase*, handler);
};

}  // namespace certain
