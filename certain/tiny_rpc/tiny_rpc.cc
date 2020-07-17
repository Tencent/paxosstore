#include "tiny_rpc/tiny_rpc.h"

#include "utils/memory.h"

namespace certain {

int TinyRpc::SendMessage(TcpSocket* socket,
                         const ::google::protobuf::Message& msg, int msg_id,
                         int result) {
  MsgHeader header(0);
  int msg_len = msg.ByteSize();

  header.magic_num = kMagicNum;
  header.msg_id = (uint16_t)msg_id;
  header.len = msg_len;
  header.result = (uint16_t)result;

  uint32_t total_len = kMsgHeaderSize + msg_len;
  auto buffer = std::make_unique<char[]>(total_len);
  header.SerializeTo(buffer.get());

  if (!msg.SerializeToArray(buffer.get() + kMsgHeaderSize, msg_len)) {
    CERTAIN_LOG_FATAL("SerializeToArray failed");
    return -1;
  }

  if (!socket->BlockWrite(buffer.get(), total_len)) {
    CERTAIN_LOG_ERROR("BlockWrite failed");
    return -2;
  }

  return 0;
}

int TinyRpc::ReceiveHeader(TcpSocket* socket, MsgHeader* header) {
  char header_buf[kMsgHeaderSize];

  if (!socket->BlockRead(header_buf, kMsgHeaderSize)) {
    CERTAIN_LOG_ERROR("BlockRead failed");
    return -1;
  }

  header->ParseFrom(header_buf);
  return 0;
}

int TinyRpc::ReceiveBody(TcpSocket* socket, ::google::protobuf::Message* msg,
                         MsgHeader* header) {
  int msg_len = header->len;
  auto buffer = std::make_unique<char[]>(msg_len);
  if (!socket->BlockRead(buffer.get(), msg_len)) {
    CERTAIN_LOG_ERROR("BlockRead failed");
    return -2;
  }

  if (!msg->ParseFromArray(buffer.get(), msg_len)) {
    CERTAIN_LOG_FATAL("ParseFromArray failed");
    return -3;
  }

  int16_t result = int16_t(header->result);
  return result;
}

int TinyRpc::ReceiveMessage(TcpSocket* socket, ::google::protobuf::Message* msg,
                            int* msg_id) {
  MsgHeader header(0);
  char header_buf[kMsgHeaderSize];

  if (!socket->BlockRead(header_buf, kMsgHeaderSize)) {
    CERTAIN_LOG_ERROR("BlockRead failed");
    return -1;
  }

  header.ParseFrom(header_buf);

  int msg_len = header.len;
  auto buffer = std::make_unique<char[]>(msg_len);
  if (!socket->BlockRead(buffer.get(), msg_len)) {
    CERTAIN_LOG_ERROR("BlockRead failed");
    return -2;
  }

  if (!msg->ParseFromArray(buffer.get(), msg_len)) {
    CERTAIN_LOG_FATAL("ParseFromArray failed");
    return -3;
  }

  *msg_id = int16_t(header.msg_id);
  int16_t result = int16_t(header.result);
  return result;
}

}  // namespace certain
