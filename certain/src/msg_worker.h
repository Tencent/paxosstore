#pragma once

#include "network/msg_channel.h"
#include "network/poller.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "src/msg_channel_helper.h"
#include "src/wrapper.h"
#include "utils/thread.h"

namespace certain {

class MsgWorker : public ThreadBase,
                  public HandlerBase,
                  public MsgCallBackBase {
 public:
  MsgWorker(Options* options, uint32_t worker_id)
      : ThreadBase("io_" + std::to_string(worker_id)),
        options_(options),
        monitor_(options->monitor()),
        worker_id_(worker_id),
        read_buf_(std::make_unique<char[]>(kReadBufferSize)),
        queue_mng_(AsyncQueueMng::GetInstance()),
        poller_(),
        channel_helper_(options, &poller_) {}

  virtual void Run() override;

  static void GoAndDeleteIfFailed(std::unique_ptr<PaxosCmd> pcmd);

 private:
  Options* options_;
  Monitor* monitor_;
  uint32_t worker_id_;

  SharedLimiter rbuf_shared_limiter_{kMaxTotalBufferSize};
  SharedLimiter wbuf_shared_limiter_{kMaxTotalBufferSize};

  std::unique_ptr<char[]> read_buf_;
  AsyncQueueMng* queue_mng_;
  Poller poller_;
  MsgChannelHelper channel_helper_;

  void ServeNewConnect();
  void ConsumeMsgReqQueue();
  void ParsePaxosMsgBuffer(const char* buffer, uint32_t len);
  void ParseRangeCatchupMsgBuffer(const char* buffer, uint32_t len);

  MsgChannel* GetConnect(uint64_t peer_addr_id);

  // HandlerBase
  virtual int HandleRead(FdObj* fd_obj) override;
  virtual int HandleWrite(FdObj* fd_obj) override;

  // MsgCallBackBase
  virtual void OnMessage(uint8_t msg_id, const char* buffer,
                         uint32_t len) override;
};

}  // namespace certain
