#include "src/command.h"

#include "utils/memory.h"

namespace certain {

PaxosCmd::PaxosCmd() : MsgBase(kCmdPaxos) {}

PaxosCmd::PaxosCmd(uint64_t entity_id, uint64_t entry) : MsgBase(kCmdPaxos) {
  entity_id_ = entity_id;
  entry_ = entry;
}

std::string PaxosCmd::ToString() {
  char buffer[256];
  std::string local = EntryRecordToString(local_entry_record_);
  std::string peer = EntryRecordToString(peer_entry_record_);

  snprintf(buffer, sizeof(buffer),
           "%u uuid %lu E(%lu, %lu) max %lu "
           "aid(%u, %u) ce %u ca %u l(%s) p(%s) ret %d",
           unsigned(cmd_id_), uuid_, entity_id_, entry_, max_chosen_entry_,
           local_acceptor_id_, peer_acceptor_id_, check_empty_, catchup_,
           local.c_str(), peer.c_str(), result_);
  return buffer;
}

bool PaxosCmd::ParseFromBuffer(const char* buffer, uint32_t len) {
  PaxosMsg paxos_msg;
  if (!paxos_msg.ParseFromArray(buffer, len)) {
    CERTAIN_LOG_FATAL("ParseFromArray fail");
    return false;
  }

  SetFromHeader(&paxos_msg.header());

  local_acceptor_id_ = paxos_msg.local_acceptor_id();
  peer_acceptor_id_ = paxos_msg.peer_acceptor_id();

  paxos_msg.mutable_local_entry_record()->Swap(&local_entry_record_);
  paxos_msg.mutable_peer_entry_record()->Swap(&peer_entry_record_);

  max_chosen_entry_ = paxos_msg.max_chosen_entry();
  check_empty_ = paxos_msg.check_empty();
  catchup_ = paxos_msg.catchup();

  return true;
}

bool PaxosCmd::SerializeToBuffer(char* buffer, uint32_t len,
                                 uint32_t& real_len) {
  PaxosMsg paxos_msg;

  SetToHeader(paxos_msg.mutable_header());

  paxos_msg.set_local_acceptor_id(local_acceptor_id_);
  paxos_msg.set_peer_acceptor_id(peer_acceptor_id_);

  paxos_msg.mutable_local_entry_record()->Swap(&local_entry_record_);
  paxos_msg.mutable_peer_entry_record()->Swap(&peer_entry_record_);

  paxos_msg.set_catchup(catchup_);
  paxos_msg.set_check_empty(check_empty_);
  paxos_msg.set_max_chosen_entry(max_chosen_entry_);

  bool ok = true;
  real_len = paxos_msg.ByteSize();
  if (uint32_t(real_len) > len) {
    CERTAIN_LOG_FATAL("SerializeToArray real_len %d len %u", real_len, len);
    ok = false;
  }

  if (ok && !paxos_msg.SerializeToArray(buffer, len)) {
    CERTAIN_LOG_FATAL("SerializeToArray fail");
    ok = false;
  }

  paxos_msg.mutable_local_entry_record()->Swap(&local_entry_record_);
  paxos_msg.mutable_peer_entry_record()->Swap(&peer_entry_record_);
  return ok;
}

uint32_t PaxosCmd::SerializedByteSize() {
  PaxosMsg paxos_msg;

  SetToHeader(paxos_msg.mutable_header());

  paxos_msg.set_local_acceptor_id(local_acceptor_id_);
  paxos_msg.set_peer_acceptor_id(peer_acceptor_id_);

  paxos_msg.mutable_local_entry_record()->Swap(&local_entry_record_);
  paxos_msg.mutable_peer_entry_record()->Swap(&peer_entry_record_);

  paxos_msg.set_catchup(catchup_);
  paxos_msg.set_check_empty(check_empty_);
  paxos_msg.set_max_chosen_entry(max_chosen_entry_);

  uint64_t byte_size = paxos_msg.ByteSize();

  paxos_msg.mutable_local_entry_record()->Swap(&local_entry_record_);
  paxos_msg.mutable_peer_entry_record()->Swap(&peer_entry_record_);

  return byte_size;
}

bool PaxosCmd::SwitchToLocalView(uint32_t local_acceptor_id) {
  if (peer_acceptor_id_ != local_acceptor_id) {
    return false;
  }
  std::swap(local_acceptor_id_, peer_acceptor_id_);
  std::swap(local_entry_record_, peer_entry_record_);
  return true;
}

void PaxosCmd::RemoveValueInRecord() {
  // Remove in local view.
  if (peer_entry_record_.value_id() == 0) {
    return;
  }
  assert(peer_entry_record_.accepted_num() > 0);

  if (local_entry_record_.value_id() == peer_entry_record_.value_id() &&
      !local_entry_record_.has_value_id_only()) {
    local_entry_record_.clear_value();
    local_entry_record_.clear_uuids();
    local_entry_record_.set_has_value_id_only(true);
  }
  if (!peer_entry_record_.has_value_id_only()) {
    peer_entry_record_.clear_value();
    peer_entry_record_.clear_uuids();
    peer_entry_record_.set_has_value_id_only(true);
  }
}

RangeCatchupCmd::RangeCatchupCmd(uint64_t entity_id, uint64_t entry)
    : MsgBase(kCmdRangeCatchup) {
  entity_id_ = entity_id;
  entry_ = entry;
}

std::string RangeCatchupCmd::ToString() {
  char buffer[128];
  snprintf(buffer, sizeof(buffer), "E(%lu, %lu) aid(%u, %u) range[%lu, %lu)",
           entity_id_, entry_, local_acceptor_id_, peer_acceptor_id_,
           begin_entry_, end_entry_);
  return buffer;
}

bool RangeCatchupCmd::ParseFromBuffer(const char* buffer, uint32_t len) {
  RangeCatchupMsg rangecatchup_msg;
  if (!rangecatchup_msg.ParseFromArray(buffer, len)) {
    CERTAIN_LOG_FATAL("ParseFromArray fail");
    return false;
  }

  SetFromHeader(&rangecatchup_msg.header());

  local_acceptor_id_ = rangecatchup_msg.local_acceptor_id();
  peer_acceptor_id_ = rangecatchup_msg.peer_acceptor_id();

  begin_entry_ = rangecatchup_msg.begin_entry();
  end_entry_ = rangecatchup_msg.end_entry();

  return true;
}

bool RangeCatchupCmd::SerializeToBuffer(char* buffer, uint32_t len,
                                        uint32_t& real_len) {
  RangeCatchupMsg rangecatchup_msg;

  SetToHeader(rangecatchup_msg.mutable_header());

  rangecatchup_msg.set_local_acceptor_id(local_acceptor_id_);
  rangecatchup_msg.set_peer_acceptor_id(peer_acceptor_id_);

  rangecatchup_msg.set_begin_entry(begin_entry_);
  rangecatchup_msg.set_end_entry(end_entry_);

  bool ok = true;
  real_len = rangecatchup_msg.ByteSize();
  if (uint32_t(real_len) > len) {
    CERTAIN_LOG_FATAL("SerializeToArray real_len %d len %u", real_len, len);
    ok = false;
  }

  if (ok && !rangecatchup_msg.SerializeToArray(buffer, len)) {
    CERTAIN_LOG_FATAL("SerializeToArray fail");
    ok = false;
  }

  return ok;
}

uint32_t RangeCatchupCmd::SerializedByteSize() {
  RangeCatchupMsg rangecatchup_msg;

  SetToHeader(rangecatchup_msg.mutable_header());

  rangecatchup_msg.set_local_acceptor_id(local_acceptor_id_);
  rangecatchup_msg.set_peer_acceptor_id(peer_acceptor_id_);

  rangecatchup_msg.set_begin_entry(begin_entry_);
  rangecatchup_msg.set_end_entry(end_entry_);

  return rangecatchup_msg.ByteSize();
}

bool RangeCatchupCmd::SwitchToLocalView(uint32_t local_acceptor_id) {
  if (peer_acceptor_id_ != local_acceptor_id) {
    return false;
  }
  std::swap(local_acceptor_id_, peer_acceptor_id_);
  return true;
}

ClientCmd::ClientCmd(uint8_t cmd_id) : CmdBase(cmd_id) {}

std::string ClientCmd::ToString() {
  char buffer[128];
  snprintf(buffer, sizeof(buffer),
           "%u uuid %lu E(%lu, %lu) vid %lu v.sz %zu "
           "uuids.sz %zu ret %d",
           unsigned(cmd_id_), uuid_, entity_id_, entry_, value_id_,
           value_.size(), uuids_.size(), result_);
  return buffer;
}

std::unique_ptr<MsgBase> CmdFactory::CreateCmd(uint8_t cmd_id,
                                               const char* buffer,
                                               uint32_t len) {
  std::unique_ptr<MsgBase> cmd;

  switch (cmd_id) {
    case kCmdPaxos:
      cmd = std::make_unique<PaxosCmd>();
      break;

    case kCmdRangeCatchup:
      cmd = std::make_unique<RangeCatchupCmd>();
      break;

    default:
      assert(false);
  }

  if (!cmd->ParseFromBuffer(buffer, len)) {
    CERTAIN_LOG_FATAL("ParseFromBuffer cmd_id %d failed", cmd_id);
    cmd.reset();
  }

  return cmd;
}

}  // namespace certain
