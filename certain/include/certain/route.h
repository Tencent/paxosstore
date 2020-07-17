#pragma once

#include <string>

namespace certain {

class Route {
 public:
  virtual std::string GetLocalAddr() = 0;

  virtual int GetLocalAcceptorId(uint64_t entity_id, uint32_t* acceptor_id) = 0;

  virtual int GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                              uint64_t* addr_id) = 0;
};

}  // namespace certain
