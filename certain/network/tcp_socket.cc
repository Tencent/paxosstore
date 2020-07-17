#include "network/tcp_socket.h"

namespace certain {

TcpSocket::TcpSocket() {
  fd_ = -1;
  listened_ = false;
  connected_ = false;
  non_blocked_ = false;
}

TcpSocket::TcpSocket(int fd, const InetAddr& local_addr,
                     const InetAddr& peer_addr) {
  fd_ = fd;
  listened_ = false;
  connected_ = true;
  non_blocked_ = false;
  local_addr_ = local_addr;
  peer_addr_ = peer_addr;
}

TcpSocket::~TcpSocket() {
  if (fd_ != -1) {
    int ret = close(fd_);
    if (ret < 0) {
      CERTAIN_LOG_FATAL("close failed fd %d errno [%s]", fd_, strerror(errno));
    }
  }
}

void TcpSocket::Shutdown() {
  if (fd_ == -1) {
    return;
  }
  int ret = shutdown(fd_, SHUT_RDWR);
  if (ret < 0) {
    CERTAIN_LOG_FATAL("shutdown failed fd %d errno [%s]", fd_, strerror(errno));
  }
}

int TcpSocket::InitSocket(bool non_blocked) {
  int ret, val = 1;

  if (fd_ == -1) {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ == -1) {
      CERTAIN_LOG_FATAL("socket failed fd %d errno [%s]", fd_, strerror(errno));
      return kNetWorkError;
    }
  }

  // Enable SO_REUSEADDR.
  ret = setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
  if (ret == -1) {
    CERTAIN_LOG_FATAL("setsockopt failed fd %d errno [%s]", fd_,
                      strerror(errno));
    return kNetWorkError;
  }

  // Close TCP negle algorithm.
  ret = setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
  if (ret == -1) {
    CERTAIN_LOG_FATAL("setsockopt failed fd %d errno [%s]", fd_,
                      strerror(errno));
    return kNetWorkError;
  }

  // Set O_NONBLOCK.
  ret = SetNonBlock(non_blocked);
  if (ret == -1) {
    CERTAIN_LOG_ERROR("SetNonBlock failed");
    return kNetWorkError;
  }

  return 0;
}

int TcpSocket::Bind(const InetAddr& local_addr) {
  assert(fd_ >= 0);
  assert(!listened_);
  assert(!connected_);

  struct sockaddr* addr = (struct sockaddr*)&local_addr.addr;
  socklen_t len = sizeof(*addr);

  int ret = bind(fd_, addr, len);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("bind failed fd_ %d addr %s errno [%s]", fd_,
                      local_addr.ToString().c_str(), strerror(errno));
    return kNetWorkError;
  }

  local_addr_ = local_addr;

  return 0;
}

int TcpSocket::Connect(const InetAddr& peer_addr) {
  assert(fd_ >= 0);
  assert(!listened_);
  assert(!connected_);

  const struct sockaddr_in* addr = &peer_addr.addr;
  socklen_t len = sizeof(*addr);

  peer_addr_ = peer_addr;

  int ret = connect(fd_, (struct sockaddr*)addr, len);
  if (ret == -1) {
    if (errno == EINPROGRESS) {
      connected_ = true;
      return kNetWorkInProgress;
    }

    CERTAIN_LOG_FATAL("connect failed fd_ %d local %s peer %s errno [%s]", fd_,
                      local_addr_.ToString().c_str(),
                      peer_addr_.ToString().c_str(), strerror(errno));
    return kNetWorkError;
  }
  connected_ = true;

  return 0;
}

int TcpSocket::Listen() {
  assert(fd_ >= 0);
  assert(!listened_);
  assert(!connected_);

  int ret = listen(fd_, kBackLog);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("listen failed fd_ %d local %s errno [%s]", fd_,
                      local_addr_.ToString().c_str(), strerror(errno));
    return kNetWorkError;
  }
  listened_ = true;

  return 0;
}

int TcpSocket::Accept(InetAddr& accepted_addr) {
  assert(listened_);

  int accepted_fd;
  struct sockaddr_in addr;
  socklen_t len = sizeof(addr);

  while (1) {
    accepted_fd = accept(fd_, (struct sockaddr*)(&addr), &len);
    if (accepted_fd == -1) {
      if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN) {
        return kNetWorkWouldBlock;
      }

      CERTAIN_LOG_FATAL("accept failed fd_ %d local %s errno [%s]", fd_,
                        local_addr_.ToString().c_str(), strerror(errno));
      return kNetWorkError;
    } else {
      accepted_addr = InetAddr(addr);
      break;
    }
  }

  return accepted_fd;
}

int TcpSocket::SetNonBlock(bool non_blocked) {
  assert(fd_ >= 0);

  int flag = fcntl(fd_, F_GETFL, 0);
  if (flag == -1) {
    CERTAIN_LOG_FATAL("fcntl failed errno [%s]", strerror(errno));
    return kNetWorkError;
  }

  flag |= O_NONBLOCK;
  if (!non_blocked) {
    flag ^= O_NONBLOCK;
  }

  int ret = fcntl(fd_, F_SETFL, flag);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("fcntl failed errno [%s]", strerror(errno));
    return kNetWorkError;
  }

  non_blocked_ = non_blocked;

  return 0;
}

int TcpSocket::CheckIfNonBlock(bool& non_blocked) {
  assert(fd_ >= 0);

  int flag = fcntl(fd_, F_GETFL, 0);
  if (flag == -1) {
    CERTAIN_LOG_FATAL("fcntl failed errno [%s]", strerror(errno));
    return kNetWorkError;
  }

  non_blocked = flag & O_NONBLOCK;
  return 0;
}

int TcpSocket::CheckIfValid() {
  assert(fd_ >= 0);

  int error = 0;
  socklen_t len = sizeof(error);
  int ret = getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("getsockopt failed errno [%s]", strerror(errno));
    return kNetWorkError;
  }

  if (error != 0) {
    CERTAIN_LOG_FATAL("getsockopt succ errno [%s]", strerror(errno));
    return kNetWorkError;
  }

  return 0;
}

bool TcpSocket::BlockWrite(const char* buffer, uint32_t len) {
  if (non_blocked_) {
    CERTAIN_LOG_FATAL("fd %d is non_block");
    return false;
  }

  uint32_t offset = 0;
  while (offset < len) {
    int ret = write(fd_, buffer + offset, len - offset);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      CERTAIN_LOG_ERROR("write socket: %s errno [%s]", ToString().c_str(),
                        strerror(errno));
      return false;
    }
    offset += ret;
  }

  return true;
}

bool TcpSocket::BlockRead(char* buffer, uint32_t len) {
  if (non_blocked_) {
    CERTAIN_LOG_FATAL("fd %d is non_block");
    return false;
  }

  uint32_t offset = 0;
  while (offset < len) {
    int ret = read(fd_, buffer + offset, len - offset);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      CERTAIN_LOG_ERROR("read socket: %s errno [%s]", ToString().c_str(),
                        strerror(errno));
      return false;
    }
    if (ret == 0) {
      CERTAIN_LOG_ERROR("read socket: %s errno [%s]", ToString().c_str(),
                        strerror(errno));
      return false;
    }
    offset += ret;
  }

  return true;
}

std::string TcpSocket::ToString() {
  char buf[100];
  snprintf(buf, 100, "fd %d li %u co %u nbl %u local %s peer %s", fd_,
           listened_, connected_, non_blocked_, local_addr_.ToString().c_str(),
           peer_addr_.ToString().c_str());
  return buf;
}

}  // namespace certain
