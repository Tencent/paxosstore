#include "default/route_impl.h"

#include "network/inet_addr.h"

RouteImpl::RouteImpl(std::string local_addr, std::vector<std::string> addrs) {
  local_addr_ = local_addr;
  addrs_ = addrs;

  index_ = -1;
  addr_ids_.resize(addrs_.size());
  for (size_t i = 0; i < addrs_.size(); ++i) {
    if (addrs_[i] == local_addr_) {
      index_ = i;
    }
    addr_ids_[i] = certain::InetAddr(addrs_[i]).GetAddrId();
  }
  assert(index_ != -1);
}

std::string RouteImpl::GetLocalAddr() { return local_addr_; }

int RouteImpl::GetLocalAcceptorId(uint64_t entity_id, uint32_t* acceptor_id) {
  *acceptor_id = (entity_id % 3 + index_) % 3;
  return 0;
}

int RouteImpl::GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                               uint64_t* addr_id) {
  uint32_t local_acceptor_id = 0;
  int ret = GetLocalAcceptorId(entity_id, &local_acceptor_id);
  if (ret != 0) {
    return ret;
  }

  uint32_t curr_index = (index_ + 3 + acceptor_id - local_acceptor_id) % 3;
  *addr_id = addr_ids_[curr_index];

  return 0;
}
