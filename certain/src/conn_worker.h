#pragma once

#include "certain/options.h"
#include "network/poller.h"
#include "src/wrapper.h"
#include "utils/header.h"
#include "utils/thread.h"

namespace certain {

class ListenContext : public FdObj, public HandlerBase {
 public:
  ListenContext(std::unique_ptr<TcpSocket> tcp_socket)
      : FdObj(tcp_socket->fd(), this), tcp_socket_(std::move(tcp_socket)) {}

  virtual ~ListenContext() {}

  virtual int HandleRead(FdObj* fd_obj) override {
    assert(dynamic_cast<ListenContext*>(fd_obj) == this);

    int ret;
    while (true) {
      ret = HandleListenEvent();
      if (ret != 0) {
        break;
      }
    }

    if (ret != kNetWorkWouldBlock) {
      return ret;
    }

    return 0;
  }

  virtual int HandleWrite(FdObj* fd_obj) override {
    CERTAIN_UNREACHABLE();
    return -1;
  }

 private:
  std::unique_ptr<TcpSocket> tcp_socket_;

  int HandleListenEvent();
};

class ConnWorker : public ThreadBase {
 public:
  virtual ~ConnWorker();

  ConnWorker(Options* options);

  virtual void Run() override;

 private:
  Options* options_;
  std::unique_ptr<Poller> poller_;
  std::unique_ptr<ListenContext> context_;

  // Calls at the begining of Run().
  int AddListen(const std::string& str_addr);
};

}  // namespace certain
