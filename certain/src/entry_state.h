#pragma once

#include "proto/certain.pb.h"
#include "src/common.h"
#include "utils/header.h"
#include "utils/log.h"

namespace certain {

enum struct EntryState {
  kNormal = 0,
  kPromiseLocal = 1,
  kPromiseRemote = 2,
  kMajorityPromise = 3,
  kAcceptRemote = 4,
  kAcceptLocal = 5,
  kChosen = 6
};

const uint32_t kMaxAcceptorNum = 5;

class EntryStateMachine {
 public:
  EntryStateMachine(uint64_t entity_id, uint64_t entry, uint32_t acceptor_num,
                    uint32_t local_acceptor_id)
      : entity_id_(entity_id),
        entry_(entry),
        acceptor_num_(acceptor_num),
        local_acceptor_id_(local_acceptor_id),
        entry_state_(EntryState::kNormal) {}

  void CalcEntryState();

  EntryState entry_state() const { return entry_state_; }

  int GetByValueId(uint64_t value_id, std::string& value,
                   std::vector<uint64_t>& uuids);
  void RestoreValueInRecord(EntryRecord& record);
  const std::string& GetTheChosenValue() const;
  void AddTheChosenUuids() const;

  int Update(uint32_t peer_acceptor_id, const EntryRecord& peer_record);

  int Promise(bool pre_auth = false);

  int Accept(const std::string& value, uint64_t value_id,
             const std::vector<uint64_t>& uuids, bool* prepared_value_accepted);

  uint32_t GetLocalPromisedNum() const;
  uint32_t GetLocalAcceptedNum() const;
  bool IsLocalAcceptable() const;

  // For read routine.
  bool IsLocalEmpty() const { return entry_state_ == EntryState::kNormal; }
  void ResetEmptyFlags();
  void SetEmptyFlag(uint32_t peer_acceptor_id);
  bool IsMajorityEmpty() const;

  size_t CalcSize() const;

  // Serialized to a readable string for debug.
  std::string ToString() const;

  static bool IsValidRecord(uint64_t entity_id, uint64_t entry,
                            const EntryRecord& record);

  static bool IsRecordNewer(const EntryRecord& old_record,
                            const EntryRecord& new_record);

  const EntryRecord& GetEntryRecord(uint32_t acceptor_id) const {
    return entry_records_[acceptor_id];
  }

  bool HasPromisedMyProposal(uint32_t peer_acceptor_id) const;
  bool HasAcceptedMyProposal(uint32_t peer_acceptor_id) const;

 private:
  uint64_t entity_id_;
  uint64_t entry_;
  uint32_t acceptor_num_;
  uint32_t local_acceptor_id_;

  // Use array to reduce heap alloc.
  EntryRecord entry_records_[kMaxAcceptorNum];
  bool empty_flags_[kMaxAcceptorNum];

  EntryState entry_state_;
};

}  // namespace certain
