#include "src/msg_channel_helper.h"

namespace certain {

int MsgChannelHelper::AddWritableChannel(MsgChannel* channel) {
  auto peer_addr_id = channel->peer_addr_id();
  auto& cs = channel_map_[peer_addr_id];
  std::unique_ptr<MsgChannel> dummy(channel);
  auto it = cs.find(dummy);
  dummy.release();
  if (it == cs.end()) {
    CERTAIN_LOG_FATAL("not found channel: %s", channel->ToString().c_str());
    return kRetCodeNotFound;
  }
  writable_set_.insert(channel);
  return 0;
}

int MsgChannelHelper::FlushMsgChannel() {
  std::vector<MsgChannel*> unwritables;
  std::vector<MsgChannel*> brokens;

  for (auto iter = writable_set_.begin(); iter != writable_set_.end();) {
    MsgChannel* ch = *iter;
    ch->FlushBuffer();
    if (ch->broken()) {
      brokens.push_back(ch);
    } else if (!ch->writable()) {
      unwritables.push_back(ch);
    }
    iter++;
  }

  // Remove unwritable channels from writable_set_.
  for (auto ch : unwritables) {
    writable_set_.erase(ch);
  }

  // Remove and delete the broken channels.
  for (auto ch : brokens) {
    RemoveMsgChannel(ch);
  }

  return 0;
}

int MsgChannelHelper::CleanUnactiveMsgChannel() {
  std::vector<MsgChannel*> unactives;

  for (auto& pair : channel_map_) {
    auto& channel_set = pair.second;
    for (auto& ch : channel_set) {
      assert(!ch->broken());

      uint64_t timeoutus = options_->active_timeout_sec() * 1000000;
      if (ch->active_timestamp_us() + timeoutus >= GetTimeByUsec()) {
        continue;
      }

      CERTAIN_LOG_ZERO("remove unacive channel: %s", ch->ToString().c_str());
      unactives.push_back(ch.get());
    }
  }

  // Remove unactive channels.
  for (auto ch : unactives) {
    RemoveMsgChannel(ch);
  }

  return 0;
}

MsgChannel* MsgChannelHelper::GetMsgChannel(uint64_t peer_addr_id) {
  // Get the confirmed channel from channel map only.
  auto& channel_set = channel_map_[peer_addr_id];
  if (!channel_set.empty()) {
    return channel_set.begin()->get();
  }
  CERTAIN_LOG_INFO("peer_addr %s not found",
                   InetAddr(peer_addr_id).ToString().c_str());
  return nullptr;
}

int MsgChannelHelper::AddMsgChannel(std::unique_ptr<MsgChannel>& channel) {
  int ret = poller_->Add(channel.get());
  if (ret != 0) {
    CERTAIN_LOG_FATAL("add channel: %s failed with ret %d",
                      channel->ToString().c_str(), ret);
    return -1;
  }
  uint64_t peer_addr_id = channel->peer_addr_id();
  auto ib = channel_map_[peer_addr_id].insert(std::move(channel));
  assert(ib.second == true);
  return 0;
}

int MsgChannelHelper::RemoveMsgChannel(MsgChannel* channel) {
  auto peer_addr_id = channel->peer_addr_id();
  auto& channel_set = channel_map_[peer_addr_id];
  std::unique_ptr<MsgChannel> dummy(channel);
  auto iter = channel_set.find(dummy);
  dummy.release();
  if (iter == channel_set.end()) {
    CERTAIN_LOG_FATAL("peer_addr %s not found, channel: %s",
                      InetAddr(peer_addr_id).ToString().c_str(),
                      channel->ToString().c_str());
    return kRetCodeNotFound;
  }

  CERTAIN_LOG_INFO("remove channel: %s", channel->ToString().c_str());

  writable_set_.erase(channel);
  poller_->Remove(channel);
  channel_set.erase(iter);
  return 0;
}

int MsgChannelHelper::RefreshMsgChannel(MsgChannel* channel) {
  int ret = poller_->Remove(channel);
  if (ret) {
    return ret;
  }
  return poller_->Add(channel);
}

}  // namespace certain
