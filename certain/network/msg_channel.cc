#include "network/msg_channel.h"

#include "utils/memory.h"

namespace certain {

MsgChannel::MsgChannel(std::unique_ptr<TcpSocket> tcp_socket,
                       HandlerBase* handler, SharedLimiter* rbuf_shared_limiter,
                       SharedLimiter* wbuf_shared_limiter,
                       MsgCallBackBase* msg_cb)
    : FdObj(tcp_socket->fd(), handler),
      tcp_socket_(std::move(tcp_socket)),
      rbuf_limiter_(std::make_unique<CapacityLimiter>(kMaxBufferSize,
                                                      rbuf_shared_limiter)),
      wbuf_limiter_(std::make_unique<CapacityLimiter>(kMaxBufferSize,
                                                      wbuf_shared_limiter)),
      write_item_list_(std::make_unique<WriteItemList>()),
      msg_cb_(msg_cb) {}

MsgChannel::~MsgChannel() {
  rbuf_limiter_->FreeBytes(read_bytes_.size());

  uint64_t free_bytes = write_item_list_->FreeAllWriteItems();
  wbuf_limiter_->FreeBytes(free_bytes);
}

void MsgChannel::ReadMore(char* buffer, uint32_t size) {
  int ret;
  int fd = FdObj::fd();

  assert(!FdObj::broken());
  assert(FdObj::readable());

  // Copy read_bytes_ to buffer.
  uint32_t curr_size = read_bytes_.size();
  assert(curr_size < size);
  memcpy(buffer, read_bytes_.data(), curr_size);

  // Cleanup the memory hold by read_bytes_.
  {
    std::string empty;
    read_bytes_.swap(empty);
    rbuf_limiter_->FreeBytes(curr_size);
  }

  bool active = false;
  while (curr_size < size && !FdObj::broken()) {
    ret = read(fd, buffer + curr_size, size - curr_size);

    if (ret > 0) {
      curr_size += ret;
      active = true;
    } else if (ret == 0) {
      CERTAIN_LOG_ERROR("closed by peer tcp_socket %s",
                        tcp_socket_->ToString().c_str());
      FdObj::set_broken(true);
    } else if (ret == -1) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN) {
        FdObj::set_readable(false);
        break;
      }
      CERTAIN_LOG_ERROR("read failed errno %d tcp_socket %s", errno,
                        tcp_socket_->ToString().c_str());
      FdObj::set_broken(true);
    }
  }

  if (active) {
    active_timestamp_us_ = GetTimeByUsec();
  }

  uint32_t parsed_len = 0;
  if (!ParseMsg(buffer, curr_size, &parsed_len)) {
    CERTAIN_LOG_FATAL("parse failed curr_size %u parsed_len %u tcp_socket %s",
                      curr_size, parsed_len, tcp_socket_->ToString().c_str());
    FdObj::set_broken(true);
    return;
  }

  if (!rbuf_limiter_->AllocBytes(curr_size - parsed_len)) {
    CERTAIN_LOG_FATAL("rbuf_limiter %s tcp_socket %s",
                      rbuf_limiter_->ToString().c_str(),
                      tcp_socket_->ToString().c_str());
    FdObj::set_broken(true);
    return;
  }

  // Save the bytes that not parsed yet.
  read_bytes_ = std::string(buffer + parsed_len, curr_size - parsed_len);
}

void MsgChannel::FlushBuffer() {
  if (FdObj::broken()) {
    return;
  }
  assert(FdObj::writable());

  int ret;
  const int kBatchNum = 5;
  struct iovec buf[kBatchNum];
  int fd = FdObj::fd();
  bool active = false;

  while (!FdObj::broken()) {
    int n = write_item_list_->GetFirstNIterms(buf, kBatchNum);
    if (n == 0) {
      break;
    }

    ret = writev(fd, buf, n);
    // CERTAIN_LOG_INFO("memory fd %d n %d ret %d", fd, n, ret);
    if (ret > 0) {
      active = true;
      for (int i = 0; i < n; ++i) {
        if (buf[i].iov_len <= (unsigned int)ret) {
          ret -= buf[i].iov_len;
          buf[i].iov_base = (char*)buf[i].iov_base + buf[i].iov_len;
          buf[i].iov_len = 0;
        } else {
          buf[i].iov_base = (char*)buf[i].iov_base + ret;
          buf[i].iov_len -= ret;
          ret = 0;
          break;
        }
      }
    }

    uint64_t free_bytes = write_item_list_->CleanWrittenItems();
    wbuf_limiter_->FreeBytes(free_bytes);

    if (ret == -1) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN) {
        FdObj::set_writable(false);
        break;
      }
      CERTAIN_LOG_ERROR("write failed errno %d tcp_socket %s", errno,
                        tcp_socket_->ToString().c_str());
      FdObj::set_broken(true);
      break;
    }
  }

  if (active) {
    active_timestamp_us_ = GetTimeByUsec();
  }
}

bool MsgChannel::Write(SerializeCallBackBase** serialize_cb, int n) {
  int prev = 0;
  uint64_t sizes[n];
  uint64_t total_size = 0;
  for (int i = 0; i < n; ++i) {
    sizes[i] = serialize_cb[i]->ByteSize();
    total_size += sizes[i] + kMsgHeaderSize;
    if (i + 1 < n && total_size < kMaxBatchSize) {
      continue;
    }

    auto buffer = std::make_unique<char[]>(total_size);
    char* offset = buffer.get();
    for (int j = prev; j <= i; ++j) {
      if (!SerializeMsg(serialize_cb[j], offset, sizes[j] + kMsgHeaderSize)) {
        CERTAIN_LOG_FATAL("serializeTo failed sizes[%d] %lu", j, sizes[j]);
        return false;
      }
      offset += sizes[j] + kMsgHeaderSize;
    }

    if (!wbuf_limiter_->AllocBytes(total_size)) {
      return false;
    }

    write_item_list_->AddWriteItem(std::move(buffer), total_size);
    prev = i + 1;
    total_size = 0;
  }

  return true;
}

std::string MsgChannel::ToString() {
  const int len = 200;
  char buf[len];
  snprintf(buf, len, "tcp: %s wbuf: %s rbuf: %s read.sz %lu",
           tcp_socket_->ToString().c_str(), wbuf_limiter_->ToString().c_str(),
           rbuf_limiter_->ToString().c_str(), read_bytes_.size());
  return buf;
}

bool MsgChannel::SerializeMsg(SerializeCallBackBase* serialize_cb, char* buffer,
                              uint32_t len) {
  MsgHeader header(serialize_cb->GetMsgId());
  if (!serialize_cb->SerializeTo(buffer + kMsgHeaderSize,
                                 len - kMsgHeaderSize)) {
    return false;
  }
  header.len = len - kMsgHeaderSize;
  header.SerializeTo(buffer);
  return true;
}

bool MsgChannel::ParseMsg(const char* buffer, uint32_t len,
                          uint32_t* parsed_len) {
  uint32_t curr = 0;
  while (curr < len) {
    const char* curr_buffer = buffer + curr;
    MsgHeader header(0);
    header.ParseFrom(curr_buffer);
    if (header.len + kMsgHeaderSize > len - curr) {
      break;
    } else {
      msg_cb_->OnMessage(header.msg_id, curr_buffer + kMsgHeaderSize,
                         header.len);
    }
    curr += kMsgHeaderSize + header.len;
  }
  *parsed_len = curr;
  return true;
}

}  // namespace certain
