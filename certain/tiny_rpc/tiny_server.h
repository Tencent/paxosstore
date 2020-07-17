#pragma once

#include <vector>

#include "tiny_rpc/tiny_rpc.h"
#include "utils/co_lock.h"
#include "utils/routine_worker.h"

namespace certain {

class TinyServer : public RoutineWorker<TcpSocket> {
 public:
  TinyServer(std::string local_addr, google::protobuf::Service* service)
      : RoutineWorker<TcpSocket>("tiny_server", 64),
        local_addr_(local_addr),
        service_(service) {}
  int Init();
  void Stop();

 private:
  std::unique_ptr<TcpSocket> GetJob() final;
  void DoJob(std::unique_ptr<TcpSocket> job) final;
  void Tick() final { Tick::Run(); }

 private:
  std::string local_addr_;
  google::protobuf::Service* service_;
  std::unique_ptr<TcpSocket> listen_socket_;
};

}  // namespace certain
