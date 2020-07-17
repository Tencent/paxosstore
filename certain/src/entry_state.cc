#include "entry_state.h"

#include "utils/uuid_mng.h"

namespace certain {

int EntryStateMachine::GetByValueId(uint64_t value_id, std::string& value,
                                    std::vector<uint64_t>& uuids) {
  for (size_t i = 0; i < acceptor_num_; ++i) {
    if (entry_records_[i].value_id() == value_id) {
      value = entry_records_[i].value();
      uuids = {entry_records_[i].uuids().begin(),
               entry_records_[i].uuids().end()};
      return 0;
    }
  }
  return kRetCodeNotFound;
}

void EntryStateMachine::RestoreValueInRecord(EntryRecord& record) {
  assert(record.has_value_id_only());
  record.set_has_value_id_only(false);

  std::string value;
  std::vector<uint64_t> uuids;
  int ret = GetByValueId(record.value_id(), value, uuids);
  if (ret != 0) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) st: %s vid %lu not found", entity_id_,
                      entry_, ToString().c_str(), record.value_id());
    // If GetByValueId failed, it's equals to drop messages.
    record.clear_chosen();
    record.clear_value();
    record.clear_uuids();
    return;
  }
  record.set_value(value);
  *record.mutable_uuids() = {uuids.begin(), uuids.end()};
}

const std::string& EntryStateMachine::GetTheChosenValue() const {
  const EntryRecord& record = entry_records_[local_acceptor_id_];
  assert(record.chosen());
  return record.value();
}

void EntryStateMachine::AddTheChosenUuids() const {
  const EntryRecord& record = entry_records_[local_acceptor_id_];
  assert(record.chosen());
  for (auto uuid : record.uuids()) {
    UuidMng::GetInstance()->Add(entity_id_, uuid);
  }
}

void EntryStateMachine::CalcEntryState() {
  entry_state_ = EntryState::kNormal;
  const EntryRecord& record = entry_records_[local_acceptor_id_];

  if (record.chosen()) {
    entry_state_ = EntryState::kChosen;
    return;
  }

  // To be simple and more efficient, Only count the same proposal as local.
  if (record.accepted_num() > 0) {
    uint32_t accepted_count = 0;
    for (size_t i = 0; i < acceptor_num_; ++i) {
      if (entry_records_[i].accepted_num() == record.accepted_num()) {
        accepted_count++;
      }
    }
    if (accepted_count >= acceptor_num_ / 2 + 1) {
      entry_state_ = EntryState::kChosen;
      entry_records_[local_acceptor_id_].set_chosen(true);
      return;
    }
  }

  if (record.promised_num() > record.prepared_num()) {
    entry_state_ = EntryState::kPromiseRemote;
    if (record.accepted_num() == record.promised_num()) {
      entry_state_ = EntryState::kAcceptRemote;
    } else {
      assert(record.accepted_num() < record.promised_num());
    }
    return;
  }
  assert(record.promised_num() == record.prepared_num());
  assert(record.accepted_num() <= record.promised_num());

  if (record.promised_num() == 0) {
    entry_state_ = EntryState::kNormal;
    return;
  }

  entry_state_ = EntryState::kPromiseLocal;
  if (record.accepted_num() == record.promised_num()) {
    entry_state_ = EntryState::kAcceptLocal;
    return;
  }

  uint32_t promised_count = 0;
  for (size_t i = 0; i < acceptor_num_; ++i) {
    if (entry_records_[i].promised_num() == record.promised_num()) {
      promised_count++;
    }
  }
  if (promised_count >= acceptor_num_ / 2 + 1) {
    entry_state_ = EntryState::kMajorityPromise;
    return;
  }
}

bool EntryStateMachine::IsRecordNewer(const EntryRecord& old_record,
                                      const EntryRecord& new_record) {
  if (old_record.chosen()) {
    return false;
  }

  if (new_record.chosen()) {
    return true;
  }

  if (new_record.prepared_num() > old_record.prepared_num() ||
      new_record.promised_num() > old_record.promised_num() ||
      new_record.accepted_num() > old_record.accepted_num()) {
    return true;
  }

  return false;
}

bool EntryStateMachine::IsValidRecord(uint64_t entity_id, uint64_t entry,
                                      const EntryRecord& record) {
  if (record.prepared_num() > record.promised_num() ||
      record.promised_num() < record.accepted_num()) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) record: %s", entity_id, entry,
                      EntryRecordToString(record).c_str());
    return false;
  }

  // A valid proposal requires: value_id > 0.
  if ((record.accepted_num() == 0 && record.value_id() > 0) ||
      (record.accepted_num() > 0 && record.value_id() == 0)) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) record: %s", entity_id, entry,
                      EntryRecordToString(record).c_str());
    return false;
  }

  if (record.has_value_id_only() && record.value_id() == 0) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) record: %s", entity_id, entry,
                      EntryRecordToString(record).c_str());
    return false;
  }

  if (record.value_id() == 0) {
    if (record.value().size() > 0 || record.uuids_size() > 0 ||
        record.chosen()) {
      CERTAIN_LOG_FATAL("E(%lu, %lu) record: %s", entity_id, entry,
                        EntryRecordToString(record).c_str());
      return false;
    }
  }

  if (record.has_value_id_only()) {
    if (record.value().size() > 0 || record.uuids_size() > 0) {
      CERTAIN_LOG_FATAL("E(%lu, %lu) record: %s", entity_id, entry,
                        EntryRecordToString(record).c_str());
      return false;
    }
  }

  return true;
}

int EntryStateMachine::Update(uint32_t peer_acceptor_id,
                              const EntryRecord& peer_record) {
  if (peer_acceptor_id >= acceptor_num_) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) invalid peer_acceptor_id %u record %s",
                      entity_id_, entry_, peer_acceptor_id,
                      EntryRecordToString(peer_record).c_str());
    return kRetCodeInvalidAcceptorId;
  }

  // Change it to normal record if has_value_id_only.
  if (peer_record.has_value_id_only() ||
      !IsValidRecord(entity_id_, entry_, peer_record)) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) invalid has_value_id_only %u record %s",
                      entity_id_, entry_, peer_record.has_value_id_only(),
                      EntryRecordToString(peer_record).c_str());
    return kRetCodeInvalidRecord;
  }

  if (!IsRecordNewer(entry_records_[peer_acceptor_id], peer_record)) {
    return 0;
  }

  entry_records_[peer_acceptor_id] = peer_record;
  // Self-update after getting info from plog.
  if (peer_acceptor_id == local_acceptor_id_) {
    CalcEntryState();
    return 0;
  }

  if (entry_state_ == EntryState::kChosen) {
    return 0;
  }

  EntryRecord& record = entry_records_[local_acceptor_id_];

  // Update promised_num.
  if (record.promised_num() < peer_record.promised_num()) {
    record.set_promised_num(peer_record.promised_num());
  }

  // Update the proposal accepted by the chosen one or the one with higher
  // proposal number that not less than promised.
  if (peer_record.chosen() ||
      (record.promised_num() <= peer_record.accepted_num() &&
       record.accepted_num() < peer_record.accepted_num())) {
    record.set_accepted_num(peer_record.accepted_num());
    record.set_chosen(peer_record.chosen());

    if (record.value_id() != peer_record.value_id()) {
      record.set_value_id(peer_record.value_id());
      record.set_value(peer_record.value());
      *record.mutable_uuids() = peer_record.uuids();
    }
  }

  CalcEntryState();
  return 0;
}

int EntryStateMachine::Promise(bool pre_auth) {
  EntryRecord& record = entry_records_[local_acceptor_id_];
  uint32_t pn = record.promised_num();
  uint32_t n = acceptor_num_;

  pn = (pn + n - 1) / n * n + local_acceptor_id_ + 1;
  if (!pre_auth && pn <= n) {
    // proposal number not large then n is use for pre-auth only.
    pn += n;
  }

  record.set_prepared_num(pn);
  record.set_promised_num(pn);
  CalcEntryState();

  if (entry_state_ != EntryState::kPromiseLocal) {
    return kRetCodeInvalidEntryState;
  }

  return 0;
}

bool EntryStateMachine::IsLocalAcceptable() const {
  if (entry_state_ != EntryState::kMajorityPromise &&
      !(entry_state_ == EntryState::kPromiseLocal &&
        GetLocalPromisedNum() <= acceptor_num_)) {
    return false;
  }
  return true;
}

uint32_t EntryStateMachine::GetLocalPromisedNum() const {
  return entry_records_[local_acceptor_id_].promised_num();
}

uint32_t EntryStateMachine::GetLocalAcceptedNum() const {
  return entry_records_[local_acceptor_id_].accepted_num();
}

int EntryStateMachine::Accept(const std::string& value, uint64_t value_id,
                              const std::vector<uint64_t>& uuids,
                              bool* prepared_value_accepted) {
  if (entry_state_ != EntryState::kMajorityPromise &&
      !(entry_state_ == EntryState::kPromiseLocal &&
        GetLocalPromisedNum() <= acceptor_num_)) {
    return kRetCodeInvalidEntryState;
  }

  EntryRecord& record = entry_records_[local_acceptor_id_];
  uint32_t promised_num = record.promised_num();
  assert(promised_num > 0);

  // Select the value with max accepted_num.
  uint32_t selected = 0;
  uint32_t max_accepted_num = entry_records_[0].accepted_num();
  for (uint32_t i = 1; i < acceptor_num_; ++i) {
    if (max_accepted_num < entry_records_[i].accepted_num()) {
      selected = i;
      max_accepted_num = entry_records_[i].accepted_num();
    }
  }

  if (max_accepted_num > 0) {
    const auto& selected_record = entry_records_[selected];
    record.set_accepted_num(promised_num);
    record.set_value(selected_record.value());
    record.set_value_id(selected_record.value_id());
    *record.mutable_uuids() = {selected_record.uuids().begin(),
                               selected_record.uuids().end()};
    *prepared_value_accepted = false;
  } else {
    record.set_accepted_num(promised_num);
    record.set_value(value);
    record.set_value_id(value_id);
    *record.mutable_uuids() = {uuids.begin(), uuids.end()};
    *prepared_value_accepted = true;
  }

  CalcEntryState();
  assert(entry_state_ == EntryState::kAcceptLocal);

  return 0;
}

void EntryStateMachine::ResetEmptyFlags() {
  assert(entry_state_ == EntryState::kNormal);
  for (uint32_t i = 0; i < acceptor_num_; ++i) {
    empty_flags_[i] = false;
  }
  empty_flags_[local_acceptor_id_] = true;
}

void EntryStateMachine::SetEmptyFlag(uint32_t peer_acceptor_id) {
  empty_flags_[peer_acceptor_id] = true;
}

bool EntryStateMachine::IsMajorityEmpty() const {
  uint32_t count = 0;
  for (uint32_t i = 0; i < acceptor_num_; ++i) {
    if (empty_flags_[i]) {
      count++;
    }
  }
  return count > acceptor_num_ / 2;
}

size_t EntryStateMachine::CalcSize() const {
  uint32_t size = sizeof(*this);
  for (uint32_t i = 0; i < acceptor_num_; ++i) {
    size += entry_records_[i].ByteSize();
  }
  return size;
}

std::string EntryStateMachine::ToString() const {
  char buffer[1024];
  snprintf(buffer, sizeof(buffer), "st %u local %u r0:[%s] r1:[%s] r2:[%s]",
           entry_state_, local_acceptor_id_,
           EntryRecordToString(entry_records_[0]).c_str(),
           EntryRecordToString(entry_records_[1]).c_str(),
           EntryRecordToString(entry_records_[2]).c_str());
  return buffer;
}

bool EntryStateMachine::HasPromisedMyProposal(uint32_t peer_acceptor_id) const {
  if (entry_records_[local_acceptor_id_].promised_num() !=
      entry_records_[peer_acceptor_id].promised_num()) {
    return false;
  }
  if (entry_records_[local_acceptor_id_].promised_num() !=
      entry_records_[local_acceptor_id_].prepared_num()) {
    return false;
  }
  return true;
}

bool EntryStateMachine::HasAcceptedMyProposal(uint32_t peer_acceptor_id) const {
  if (entry_records_[local_acceptor_id_].accepted_num() !=
      entry_records_[peer_acceptor_id].accepted_num()) {
    return false;
  }
  if (entry_records_[local_acceptor_id_].accepted_num() !=
      entry_records_[local_acceptor_id_].prepared_num()) {
    return false;
  }
  return true;
}

}  // namespace certain
