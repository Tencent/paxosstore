#pragma once

#include "certain/errors.h"
#include "network/inet_addr.h"
#include "utils/log.h"

namespace certain {

class TcpSocket {
 public:
  TcpSocket();
  TcpSocket(int fd, const InetAddr& local_addr, const InetAddr& peer_addr);
  ~TcpSocket();

  void Shutdown();

  // 1) Create socket. 2) Enable SO_REUSEADDR. 3) Close TCP negle algorithm.
  // 4) Set O_NONBLOCK.
  int InitSocket(bool non_blocked = true);

  int Bind(const InetAddr& local_addr);

  int Connect(const InetAddr& peer_addr);

  int Listen();

  // On success, the call return a nonnegative integer that is a descriptor for
  // the accepted socket.  On error, NetWork::kError is returned.
  int Accept(InetAddr& accepted_addr);

  int SetNonBlock(bool blocked);
  int CheckIfNonBlock(bool& blocked);

  int CheckIfValid();

  bool listened() { return listened_; }
  int fd() { return fd_; }

  bool BlockWrite(const char* buffer, uint32_t len);
  bool BlockRead(char* buffer, uint32_t len);

  std::string ToString();

  InetAddr peer_addr() { return peer_addr_; }
  InetAddr local_addr() { return local_addr_; }

 private:
  const int kBackLog = 1024;

  // The file descriptor will close when destruction.
  int fd_;

  bool listened_;
  bool connected_;
  bool non_blocked_;

  InetAddr local_addr_;
  InetAddr peer_addr_;

  CERTAIN_NO_COPYING_ALLOWED(TcpSocket);
};

}  // namespace certain
