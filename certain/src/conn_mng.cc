#include "src/conn_mng.h"

namespace certain {

int ConnMng::Init(std::string local_addr, uint32_t io_worker_num) {
  local_addr_ = local_addr;
  io_worker_num_ = io_worker_num;
  balance_table_.resize(io_worker_num_);
  scores_.resize(io_worker_num_);
  return 0;
}

void ConnMng::Destroy() {
  // Cleanup all the pointers in the table.
  balance_table_.clear();
}

void ConnMng::PutByOneThread(std::unique_ptr<TcpSocket> tcp_socket) {
  std::string peer_addr = tcp_socket->peer_addr().ToString();
  assert(peer_addr != local_addr_);

  ThreadLock lock(&mutex_);

  uint64_t min_score = UINT64_MAX;
  uint32_t io_worker_id = 0;
  for (uint32_t i = 0; i < io_worker_num_; ++i) {
    if (min_score <= scores_[i]) {
      continue;
    }
    min_score = scores_[i];
    io_worker_id = i;
  }

  scores_[io_worker_id]++;
  ConnTable& conn_table = balance_table_[io_worker_id];
  conn_table[peer_addr].push_back(std::move(tcp_socket));

  CERTAIN_LOG_INFO("peer_addr %s io_worker_id %u conn_table_size %lu",
                   peer_addr.c_str(), io_worker_id,
                   conn_table[peer_addr].size());
}

std::unique_ptr<TcpSocket> ConnMng::TakeByMultiThread(uint32_t io_worker_id) {
  assert(io_worker_id < io_worker_num_);
  ThreadLock lock(&mutex_);

  ConnTable& conn_table = balance_table_[io_worker_id];
  if (conn_table.empty()) {
    return nullptr;
  }

  auto iter = conn_table.begin();
  assert(!iter->second.empty());

  auto tcp_socket = std::move(iter->second.front());
  iter->second.pop_front();

  if (iter->second.empty()) {
    conn_table.erase(iter);
  }

  return tcp_socket;
}

bool ConnMng::IsBalanceTableEmpty() const {
  for (auto& p : balance_table_) {
    if (!p.empty()) {
      return false;
    }
  }
  return true;
}

}  // namespace certain
