#pragma once

#include "certain/errors.h"
#include "network/msg_header.h"
#include "network/tcp_socket.h"
#include "proto/tiny_rpc.pb.h"
#include "utils/header.h"
#include "utils/thread.h"

// A simple rpc implementation, which is not care about performance,
// for recovering database or state machine from the other servers.
// So, we can test all the functions without a RPC framework.

namespace certain {

class TinyRpc {
 public:
  static int ReceiveHeader(TcpSocket* socket, MsgHeader* header);

  static int ReceiveBody(TcpSocket* socket, ::google::protobuf::Message* msg,
                         MsgHeader* header);

  static int SendMessage(TcpSocket* socket,
                         const ::google::protobuf::Message& msg, int msg_id,
                         int result = 0);
  static int ReceiveMessage(TcpSocket* socket, ::google::protobuf::Message* msg,
                            int* msg_id);
};

}  // namespace certain
