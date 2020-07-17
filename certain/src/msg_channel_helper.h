#pragma once

#include "certain/errors.h"
#include "certain/options.h"
#include "network/msg_channel.h"
#include "network/poller.h"
#include "utils/header.h"
#include "utils/time.h"

namespace certain {

class MsgChannelHelper {
 public:
  MsgChannelHelper(Options* options, Poller* poller) {
    options_ = options;
    poller_ = poller;
    next_flush_time_usec_ = GetTimeByUsec();
  }

  ~MsgChannelHelper() {}

  int AddWritableChannel(MsgChannel* channel);
  int FlushMsgChannel();
  int CleanUnactiveMsgChannel();

  MsgChannel* GetMsgChannel(uint64_t peer_addr_id);

  int AddMsgChannel(std::unique_ptr<MsgChannel>& channel);
  int RemoveMsgChannel(MsgChannel* channel);
  int RefreshMsgChannel(MsgChannel* channel);

 private:
  Options* options_;
  uint64_t next_flush_time_usec_;
  std::unordered_set<MsgChannel*> writable_set_;
  std::unordered_map<uint64_t, std::unordered_set<std::unique_ptr<MsgChannel>>>
      channel_map_;
  Poller* poller_;
};

}  // namespace certain
