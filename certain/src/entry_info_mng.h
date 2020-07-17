#pragma once

#include "certain/options.h"
#include "src/entity_info_mng.h"
#include "src/entry_state.h"
#include "utils/hash.h"
#include "utils/header.h"
#include "utils/light_list.h"
#include "utils/lru_table.h"

namespace certain {

struct EntryKey {
  uint64_t entity_id;
  uint64_t entry;

  bool operator<(const EntryKey& other) const {
    if (entity_id != other.entity_id) {
      return entity_id < other.entity_id;
    }
    return entry < other.entry;
  }

  bool operator==(const EntryKey& other) const {
    return entity_id == other.entity_id && entry == other.entry;
  }
};

}  // namespace certain

namespace std {

template <>
struct hash<certain::EntryKey> {
  size_t operator()(const certain::EntryKey& key) const {
    return certain::Hash((const char*)&key, sizeof(key));
  }
};

}  // namespace std

namespace certain {

struct EntryInfo {
  EntityInfo* entity_info;
  uint64_t entry;

  // The entry is uncertain when the info is being loaded/set.
  bool uncertain;

  // Broadcast to the peers when the info is persistent.
  bool broadcast;

  // Compensate the messages that not trigger kMajorityPromise or kChosen.
  bool compensate_msgs;

  // Sync to the peer when the info is persistent.
  // If peer_to_sync equals kInvalidAcceptorId, not need to sync.
  uint32_t peer_to_sync;

  uint32_t total_size = 0;

  uint64_t expired_timestamp_sec;

  // Only after this time, the entry can try to catchup if need.
  uint64_t catchup_timestamp_msec;

  uint64_t catchup_times;

  // The newest msg received for the entry when the entry_info is loading.
  std::unique_ptr<PaxosCmd> waiting_msgs[kMaxAcceptorNum];

  std::unique_ptr<EntryStateMachine> machine;

  // For the list element in EntityInfo.
  LIGHTLIST_ENTRY(EntryInfo) list_elt;
};

std::string ToString(EntryInfo* info);

class MemoryLimiter {
 public:
  MemoryLimiter(Options* options)
      : max_size_(options->max_mem_entry_size() /
                  options->entity_worker_num()) {}

  void UpdateTotalSize(EntryInfo* info);
  void RemoveFromTotalSize(EntryInfo* info);

  bool IsOverLoad();
  CERTAIN_GET_SET(uint64_t, total_size);

 private:
  uint64_t total_size_ = 0;
  uint64_t max_size_ = 0;
};

class EntryInfoMng {
 public:
  EntryInfoMng(Options* options, uint32_t entity_worker_id)
      : options_(options),
        monitor_(options->monitor()),
        entity_worker_id_(entity_worker_id),
        create_cnt_(0),
        destroy_cnt_(0),
        memory_limiter_(options) {}

  ~EntryInfoMng();

  EntryInfo* FindEntryInfo(uint64_t entity_id, uint64_t entry);
  EntryInfo* CreateEntryInfo(EntityInfo* entity_info, uint64_t entry);
  void DestroyEntryInfo(EntryInfo* entry_info);

  // The moments to fresh the entry info:
  // 1. CreateEntryInfo called.
  // 2. Local record is updated and stored.
  // 3. The entry Not deletable but need eliminate.
  bool RefreshEntryInfo(EntryInfo* entry_info);

  bool MakeEnoughRoomInner();
  bool MakeEnoughRoom();

  bool CleanupExpiredChosenEntry();

  // Calls on max_cont_chosen_entry advance.
  void MoveToOldChosenList(EntryInfo* entry_info);

  void UpdateMemorySize(EntryInfo* entry_info) {
    memory_limiter_.UpdateTotalSize(entry_info);
  }

  uint64_t MemoryUsage() const { return memory_limiter_.total_size(); }

 private:
  Options* options_;
  Monitor* monitor_;
  uint32_t entity_worker_id_;
  uint64_t create_cnt_;
  uint64_t destroy_cnt_;

  // Entry <= max_cont_chosen_entry will move to old_chosen_list_.
  LruTable<EntryKey, EntryInfo*> lru_table_;

  // (TODO): use array list.
  std::list<EntryKey> old_chosen_list_;

  MemoryLimiter memory_limiter_;
};

}  // namespace certain
