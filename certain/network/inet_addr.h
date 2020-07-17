#pragma once

#include "utils/header.h"

namespace certain {

struct InetAddr {
  struct sockaddr_in addr;

  bool operator==(const InetAddr& other) const {
    const struct sockaddr_in& other_addr = other.addr;

    if (addr.sin_addr.s_addr != other_addr.sin_addr.s_addr) {
      return false;
    }

    if (addr.sin_port != other_addr.sin_port) {
      return false;
    }

    return true;
  }

  bool operator<(const InetAddr& other) const {
    const struct sockaddr_in& other_addr = other.addr;

    if (addr.sin_addr.s_addr != other_addr.sin_addr.s_addr) {
      return addr.sin_addr.s_addr < other_addr.sin_addr.s_addr;
    }

    if (addr.sin_port != other_addr.sin_port) {
      return addr.sin_port < other_addr.sin_port;
    }

    return true;
  }

  InetAddr() { memset(&addr, 0, sizeof(addr)); }

  InetAddr(struct sockaddr_in _addr) { addr = _addr; }

  InetAddr(const char* ip, uint16_t port) {
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);
  }

  InetAddr(uint64_t addr_id) {
    uint32_t ip = (addr_id >> 16);
    uint16_t port = (addr_id & ((1 << 16) - 1));

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ip;
    addr.sin_port = htons(port);
  }

  InetAddr(uint32_t ip, uint16_t port) {
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ip;
    addr.sin_port = htons(port);
  }

  InetAddr(const std::string str_addr) {
    std::string rep = str_addr;
    for (auto& c : rep) {
      if (c == ':') {
        c = ' ';
      }
    }

    char ip[32];
    uint16_t port;
    sscanf(rep.c_str(), "%s%hu", ip, &port);

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);
  }

  std::string ToString() const {
    const char* ip = inet_ntoa(addr.sin_addr);
    uint16_t port = ntohs(addr.sin_port);

    char buf[32];
    snprintf(buf, 32, "%s:%hu", ip, port);

    return buf;
  }

  uint64_t GetAddrId() {
    uint32_t ip = addr.sin_addr.s_addr;
    uint16_t port = ntohs(addr.sin_port);
    return (uint64_t(ip) << 16) | port;
  }

  uint32_t GetIpByUint32() { return addr.sin_addr.s_addr; }
};

}  // namespace certain
