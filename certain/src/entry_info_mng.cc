#include "entry_info_mng.h"

#include "utils/memory.h"

namespace certain {

const uint32_t kMaxCleanupNum = 20;

std::string ToString(EntryInfo* info) {
  char buffer[128];

  uint32_t msgs = 0;
  for (uint32_t i = 0; i < kMaxAcceptorNum; ++i) {
    msgs *= 10;
    if (info->waiting_msgs[i] != nullptr) {
      msgs += 1;
    }
  }

  snprintf(buffer, sizeof(buffer),
           "E(%lu, %lu) uncertain %u broad %u sync %d comp %u msgs %05u st %u",
           info->entity_info->entity_id, info->entry, info->uncertain,
           info->broadcast, int32_t(info->peer_to_sync), info->compensate_msgs,
           msgs, info->machine->entry_state());
  return buffer;
}

void MemoryLimiter::UpdateTotalSize(EntryInfo* info) {
  if (info == nullptr) {
    CERTAIN_LOG_FATAL("entry info == nullptr");
    return;
  }

  RemoveFromTotalSize(info);

  uint32_t new_size = 0;
  if (info->machine != nullptr) {
    new_size += info->machine->CalcSize();
  }
  for (uint32_t i = 0; i < kMaxAcceptorNum; ++i) {
    if (info->waiting_msgs[i] != nullptr) {
      new_size += info->waiting_msgs[i]->SerializedByteSize();
    }
  }
  info->total_size = new_size;
  total_size_ += info->total_size;
}

void MemoryLimiter::RemoveFromTotalSize(EntryInfo* info) {
  if (info == nullptr) {
    CERTAIN_LOG_FATAL("entry info == nullptr");
    return;
  }

  total_size_ -= info->total_size;
  info->total_size = 0;
}

bool MemoryLimiter::IsOverLoad() { return total_size_ > max_size_; }

EntryInfoMng::~EntryInfoMng() {}

EntryInfo* EntryInfoMng::FindEntryInfo(uint64_t entity_id, uint64_t entry) {
  EntryKey key;
  key.entity_id = entity_id;
  key.entry = entry;
  EntryInfo* entry_info = nullptr;
  lru_table_.Find(key, entry_info);
  return entry_info;
}

EntryInfo* EntryInfoMng::CreateEntryInfo(EntityInfo* entity_info,
                                         uint64_t entry) {
  EntryKey key;
  key.entity_id = entity_info->entity_id;
  key.entry = entry;
  EntryInfo* entry_info = new EntryInfo;

  entry_info->entity_info = entity_info;
  entry_info->entry = entry;
  entry_info->uncertain = false;
  entry_info->broadcast = false;
  entry_info->peer_to_sync = kInvalidAcceptorId;
  entry_info->expired_timestamp_sec = 0;
  entry_info->catchup_timestamp_msec = 0;
  entry_info->catchup_times = 0;
  for (uint32_t i = 0; i < kMaxAcceptorNum; ++i) {
    entry_info->waiting_msgs[i] = nullptr;
  }
  entry_info->machine = std::make_unique<EntryStateMachine>(
      entity_info->entity_id, entry, entity_info->acceptor_num,
      entity_info->local_acceptor_id);
  LIGHTLIST_ENTRY_INIT(entry_info, list_elt);

  LIGHTLIST_INSERT_HEAD(&entity_info->entry_list, entry_info, list_elt);
  lru_table_.Add(key, entry_info);

  create_cnt_++;
  uint64_t delta = create_cnt_ - destroy_cnt_;
  uint32_t num = options_->max_mem_entry_num() / options_->entity_worker_num();
  if (create_cnt_ % num == 0) {
    CERTAIN_LOG_ZERO("worker_id %u create_cnt %lu destroy_cnt %lu delta %lu",
                     entity_worker_id_, create_cnt_, destroy_cnt_, delta);
  }
  memory_limiter_.UpdateTotalSize(entry_info);
  return entry_info;
}

void EntryInfoMng::DestroyEntryInfo(EntryInfo* entry_info) {
  EntryKey key;
  key.entity_id = entry_info->entity_info->entity_id;
  key.entry = entry_info->entry;

  if (!lru_table_.Find(key)) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) not found", key.entity_id, key.entry);
  } else {
    lru_table_.Remove(key);
  }

  LIGHTLIST_REMOVE(&entry_info->entity_info->entry_list, entry_info, list_elt);

  memory_limiter_.RemoveFromTotalSize(entry_info);
  delete entry_info;

  destroy_cnt_++;
  uint64_t delta = create_cnt_ - destroy_cnt_;
  uint32_t num = options_->max_mem_entry_num() / options_->entity_worker_num();
  if (destroy_cnt_ % num == 0 || create_cnt_ == destroy_cnt_) {
    CERTAIN_LOG_ZERO("worker_id %u create_cnt %lu destroy_cnt %lu delta %lu",
                     entity_worker_id_, create_cnt_, destroy_cnt_, delta);
  }
}

bool EntryInfoMng::RefreshEntryInfo(EntryInfo* entry_info) {
  EntryKey key;
  key.entity_id = entry_info->entity_info->entity_id;
  key.entry = entry_info->entry;
  return lru_table_.Refresh(key);
}

bool EntryInfoMng::MakeEnoughRoomInner() {
  uint32_t num = options_->max_mem_entry_num() / options_->entity_worker_num();
  assert(num > 0);

  auto need_remove = [&] {
    return lru_table_.Size() > num || memory_limiter_.IsOverLoad();
  };

  if (!need_remove()) {
    return true;
  }

  uint32_t count = 0;
  // Remove entry_info in old_chosen_list_ first.
  while (!old_chosen_list_.empty() && need_remove() && count < kMaxCleanupNum) {
    auto key = old_chosen_list_.front();
    old_chosen_list_.pop_front();
    count++;

    EntryInfo* entry_info = nullptr;
    if (!lru_table_.Find(key, entry_info)) {
      CERTAIN_LOG_WARN("E(%lu, %lu) key not found", key.entity_id, key.entry);
      continue;
    }

    assert(!entry_info->uncertain);
    DestroyEntryInfo(entry_info);
  }

  if (!old_chosen_list_.empty()) {
    return !need_remove();
  }

  while (need_remove() && count < kMaxCleanupNum) {
    EntryInfo* entry_info = nullptr;
    EntryKey key;
    bool ok = lru_table_.PeekOldest(key, entry_info);
    assert(ok);
    count++;

    if (entry_info->uncertain) {
      CERTAIN_LOG_ERROR("not deletable info: %s", ToString(entry_info).c_str());
      bool ok = RefreshEntryInfo(entry_info);
      assert(ok);
      continue;
    }

    DestroyEntryInfo(entry_info);
  }
  return !need_remove();
}

bool EntryInfoMng::MakeEnoughRoom() {
  if (MakeEnoughRoomInner()) {
    return true;
  }
  if (lru_table_.Size() >
      options_->max_mem_entry_num() / options_->entity_worker_num()) {
    monitor_->ReportEntryCountLimit();
  }
  if (memory_limiter_.IsOverLoad()) {
    monitor_->ReportEntryMemoryLimit();
  }
  return false;
}

bool EntryInfoMng::CleanupExpiredChosenEntry() {
  uint32_t count = 0;
  uint64_t now_sec = GetTimeBySecond();

  while (!old_chosen_list_.empty() && count < kMaxCleanupNum) {
    auto key = old_chosen_list_.front();

    EntryInfo* entry_info = nullptr;
    if (!lru_table_.Find(key, entry_info)) {
      CERTAIN_LOG_WARN("E(%lu, %lu) key not found", key.entity_id, key.entry);
      old_chosen_list_.pop_front();
      continue;
    }

    assert(!entry_info->uncertain);
    if (entry_info->expired_timestamp_sec > now_sec) {
      break;
    }

    old_chosen_list_.pop_front();
    DestroyEntryInfo(entry_info);
    count++;
  }
  if (count > 0) {
    CERTAIN_LOG_INFO("entity_worker_id %u destroy count %lu", entity_worker_id_,
                     count);
    return true;
  }

  return false;
}

void EntryInfoMng::MoveToOldChosenList(EntryInfo* entry_info) {
  EntryKey key;
  key.entity_id = entry_info->entity_info->entity_id;
  key.entry = entry_info->entry;

  bool ok = lru_table_.Find(key);
  if (!ok) {
    CERTAIN_LOG_FATAL("E(%lu, %lu) key not found", key.entity_id, key.entry);
  }

  entry_info->expired_timestamp_sec =
      GetTimeBySecond() + options_->entry_timeout_sec();

  old_chosen_list_.push_back(key);
}

}  // namespace certain
