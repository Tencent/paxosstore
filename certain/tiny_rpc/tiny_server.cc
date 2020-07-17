#include "tiny_rpc/tiny_server.h"

#include "co_routine.h"
#include "tiny_rpc/tiny_client.h"
#include "utils/memory.h"

namespace certain {

void TinyServer::DoJob(std::unique_ptr<TcpSocket> socket) {
  MsgHeader header(0);
  int ret = TinyRpc::ReceiveHeader(socket.get(), &header);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("ReceiveHeader ret %d", ret);
    return;
  }

  int type = header.msg_id;
  auto descriptor = service_->GetDescriptor();
  if (type >= descriptor->method_count()) {
    CERTAIN_LOG_FATAL("unknown type %d", type);
  }
  auto method = descriptor->method(type);

  std::unique_ptr<::google::protobuf::Message> req(
      service_->GetRequestPrototype(method).New());
  std::unique_ptr<::google::protobuf::Message> rsp(
      service_->GetResponsePrototype(method).New());
  ret = TinyRpc::ReceiveBody(socket.get(), req.get(), &header);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("ReceiveBody ret %d", ret);
    return;
  }

  TinyController controller;
  service_->CallMethod(method, &controller, req.get(), rsp.get(), nullptr);
  TinyRpc::SendMessage(socket.get(), *rsp, type, controller.RetCode());
}

std::unique_ptr<TcpSocket> TinyServer::GetJob() {
  InetAddr peer_addr;
  int fd = listen_socket_->Accept(peer_addr);
  if (fd == kNetWorkWouldBlock) {
    return nullptr;
  }
  if (fd < 0) {
    CERTAIN_LOG_FATAL("Accept fd %d", fd);
    return nullptr;
  }
  return std::make_unique<TcpSocket>(fd, listen_socket_->local_addr(),
                                     peer_addr);
};

int TinyServer::Init() {
  signal(SIGPIPE, SIG_IGN);

  listen_socket_ = std::make_unique<TcpSocket>();
  bool non_blocked = true;
  int ret = listen_socket_->InitSocket(non_blocked);
  if (ret != 0) {
    CERTAIN_LOG_FATAL("InitSocket ret %d", ret);
    return -1;
  }

  InetAddr addr(local_addr_);
  ret = listen_socket_->Bind(addr);
  if (ret != 0) {
    CERTAIN_LOG_FATAL("Bind ret %d", ret);
    return -2;
  }

  ret = listen_socket_->Listen();
  if (ret != 0) {
    CERTAIN_LOG_FATAL("Listen addr %s ret %d", addr.ToString().c_str(), ret);
    return -3;
  }

  CERTAIN_LOG_ZERO("listen socket: %s", listen_socket_->ToString().c_str());
  return 0;
}

void TinyServer::Stop() {
  ThreadBase::set_exit_flag(true);
  listen_socket_->Shutdown();
  while (!ThreadBase::stopped()) {
    usleep(100);
  }
  ThreadBase::WaitExit();
}

}  // namespace certain
