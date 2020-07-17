#include "tiny_rpc/tiny_client.h"

#include "tiny_rpc/tiny_rpc.h"
#include "utils/memory.h"

namespace certain {

void TinyChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                             google::protobuf::RpcController* base_controller,
                             const google::protobuf::Message* request,
                             google::protobuf::Message* response,
                             google::protobuf::Closure* done) {
  auto& controller = dynamic_cast<TinyController&>(*base_controller);
  controller.Reset();

  auto socket = std::make_unique<TcpSocket>();
  bool non_blocked = false;
  int ret = socket->InitSocket(non_blocked);
  if (ret != 0) {
    CERTAIN_LOG_FATAL("init socket failed %d", ret);
    return controller.SetRetCode(-1);
  }

  InetAddr peer_addr(peer_addr_);
  ret = socket->Connect(InetAddr(peer_addr_));
  if (ret != 0) {
    CERTAIN_LOG_ERROR("connect socket failed %d", ret);
    return controller.SetRetCode(-2);
  }

  ret = TinyRpc::SendMessage(socket.get(), *request, method->index());
  if (ret != 0) {
    CERTAIN_LOG_ERROR("SendRequest ret %d", ret);
    return controller.SetRetCode(-3);
  }

  int receive_type = 0;
  ret = TinyRpc::ReceiveMessage(socket.get(), response, &receive_type);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("ReceiveResponse ret %d", ret);
  }
  assert(ret != 0 || receive_type == method->index());

  return controller.SetRetCode(ret);
}

}  // namespace certain
