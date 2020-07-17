#pragma once

#include "network/poller.h"
#include "utils/header.h"
#include "utils/memory.h"
#include "utils/singleton.h"
#include "utils/thread.h"

namespace certain {

class ConnMng : public Singleton<ConnMng> {
 public:
  int Init(std::string local_addr, uint32_t io_worker_num);
  void Destroy();

  void PutByOneThread(std::unique_ptr<TcpSocket> tcp_socket);
  std::unique_ptr<TcpSocket> TakeByMultiThread(uint32_t io_worker_id);

  bool IsBalanceTableEmpty() const;

 protected:
  friend class Singleton<ConnMng>;
  ConnMng() {}

  std::string local_addr_;
  uint32_t io_worker_num_;

  Mutex mutex_;

  // peer address -> TcpSocket list
  // In this view, the peer endpoint send packets, while the local endpoint
  // receive packets.
  typedef std::map<std::string, std::list<std::unique_ptr<TcpSocket>>>
      ConnTable;

  // io_worker_id(idx in vector) -> ConnTable/score
  // Use round-robin scheduling algorithm.
  std::vector<ConnTable> balance_table_;
  std::vector<uint64_t> scores_;
};

}  // namespace certain
