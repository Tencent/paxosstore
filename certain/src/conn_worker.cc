#include "src/conn_worker.h"

#include "src/conn_mng.h"
#include "utils/memory.h"

namespace certain {

int ListenContext::HandleListenEvent() {
  int ret;

  InetAddr peer_addr;

  int fd = tcp_socket_->Accept(peer_addr);
  if (fd < 0) {
    if (fd == kNetWorkWouldBlock) {
      return kNetWorkWouldBlock;
    }
    CERTAIN_LOG_FATAL("Accept ret %d", fd);
    return -1;
  }

  auto new_tcp_socket =
      std::make_unique<TcpSocket>(fd, tcp_socket_->local_addr(), peer_addr);

  ret = new_tcp_socket->InitSocket();
  if (ret != 0) {
    CERTAIN_LOG_FATAL("InitSocket ret %d", ret);
    return -2;
  }

  CERTAIN_LOG_ZERO("new_tcp_socket: %s", new_tcp_socket->ToString().c_str());

  ConnMng::GetInstance()->PutByOneThread(std::move(new_tcp_socket));
  return 0;
}

int ConnWorker::AddListen(const std::string& str_addr) {
  int ret;
  InetAddr addr(str_addr);
  auto tcp_socket = std::make_unique<TcpSocket>();

  ret = tcp_socket->InitSocket();
  if (ret != 0) {
    CERTAIN_LOG_FATAL("InitSocket ret %d", ret);
    return ret;
  }

  ret = tcp_socket->Bind(addr);
  if (ret != 0) {
    CERTAIN_LOG_FATAL("Bind addr %s ret %d", addr.ToString().c_str(), ret);
    return ret;
  }

  ret = tcp_socket->Listen();
  if (ret != 0) {
    CERTAIN_LOG_FATAL("Listen addr %s ret %d", addr.ToString().c_str(), ret);
    return ret;
  }

  // fork();

  bool non_block = false;
  assert(tcp_socket->CheckIfNonBlock(non_block) == 0);
  assert(non_block);

  CERTAIN_LOG_ZERO("start listen on addr %s", addr.ToString().c_str());

  auto ctx = std::make_unique<ListenContext>(std::move(tcp_socket));
  ret = poller_->Add(ctx.get());
  if (ret != 0) {
    CERTAIN_LOG_FATAL("poller->Add fail addr %s ret %d",
                      addr.ToString().c_str(), ret);
    return ret;
  }

  context_ = std::move(ctx);
  return 0;
}

ConnWorker::ConnWorker(Options* options)
    : ThreadBase("conn"),
      options_(options),
      poller_(std::make_unique<Poller>()) {}

ConnWorker::~ConnWorker() {
  if (context_ != nullptr) {
    poller_->Remove(context_.get());
  }
}

void ConnWorker::Run() {
  int ret = AddListen(options_->local_addr());
  assert(ret == 0);

  while (!ThreadBase::exit_flag()) {
    poller_->RunOnce(1);
  }
}

}  // namespace certain
