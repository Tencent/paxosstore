#pragma once

#include "network/msg_header.h"
#include "network/poller.h"
#include "network/write_item_list.h"
#include "utils/capacity_limiter.h"
#include "utils/time.h"

namespace certain {

const uint64_t kReadBufferSize = (42 << 20);       // 42MB > 2 * kMaxValueSize
const uint64_t kMaxTotalBufferSize = (128 << 20);  // 128MB
const uint64_t kMaxBufferSize = (64 << 20);        // For single channel.
const uint32_t kMaxBatchSize = (100 << 10);        // 100KB

class MsgCallBackBase {
 public:
  virtual ~MsgCallBackBase() {}
  virtual void OnMessage(uint8_t msg_id, const char* buffer, uint32_t len) = 0;
};

class SerializeCallBackBase {
 public:
  virtual ~SerializeCallBackBase() {}
  virtual uint8_t GetMsgId() = 0;
  virtual int32_t ByteSize() = 0;
  virtual bool SerializeTo(char* buffer, uint32_t len) = 0;
};

class MsgChannel : public FdObj {
 public:
  // tcp_socket will be deleted in destruction, but other pointers will not.
  MsgChannel(std::unique_ptr<TcpSocket> tcp_socket, HandlerBase* handler,
             SharedLimiter* rbuf_shared_limiter,
             SharedLimiter* wbuf_shared_limiter, MsgCallBackBase* msg_cb);

  virtual ~MsgChannel();

  // REQUIRES: the fd is not broken and readable.
  // Call ReadMore until the channel is broken or unreadable.
  void ReadMore(char* buffer, uint32_t len);

  // REQUIRES: the fd is writable.
  void FlushBuffer();

  // Return true iff all messages written successfully. Otherwise return false
  // and the some messages is dropped, so the caller should not call any more
  // before the channel is writable again.
  bool Write(SerializeCallBackBase** serialize_cb, int n);

  CERTAIN_GET_SET(uint64_t, active_timestamp_us);

  uint64_t peer_addr_id() { return tcp_socket_->peer_addr().GetAddrId(); }

  std::string ToString();

 private:
  std::unique_ptr<TcpSocket> tcp_socket_;
  std::unique_ptr<CapacityLimiter> rbuf_limiter_;
  std::unique_ptr<CapacityLimiter> wbuf_limiter_;
  std::unique_ptr<WriteItemList> write_item_list_;
  MsgCallBackBase* msg_cb_;
  uint64_t active_timestamp_us_;

  std::string read_bytes_;

  bool ParseMsg(const char* buffer, uint32_t len, uint32_t* parsed_len);
  bool SerializeMsg(SerializeCallBackBase* serialize_cb, char* buffer,
                    uint32_t len);
  friend class MsgChannelTest_ParseAndSerialize_Test;
};

}  // namespace certain
