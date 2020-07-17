#pragma once

#include <vector>

#include "certain/route.h"

class RouteImpl : public certain::Route {
 public:
  RouteImpl(std::string local_addr, std::vector<std::string> addrs);

  virtual std::string GetLocalAddr() override;

  virtual int GetLocalAcceptorId(uint64_t entity_id,
                                 uint32_t* acceptor_id) override;

  virtual int GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                              uint64_t* addr_id) override;

 private:
  std::string local_addr_;
  std::vector<std::string> addrs_;
  std::vector<uint64_t> addr_ids_;
  int index_;
};
