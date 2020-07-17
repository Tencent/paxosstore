#pragma once

#include <array>

#include "certain/options.h"
#include "network/inet_addr.h"
#include "src/command.h"
#include "utils/array_timer.h"
#include "utils/header.h"
#include "utils/light_list.h"
#include "utils/singleton.h"
#include "utils/thread.h"

namespace certain {

struct EntryInfo;
struct EntityInfo {
  uint64_t entity_id;
  uint32_t acceptor_num;
  uint32_t local_acceptor_id;

  uint64_t max_cont_chosen_entry;
  uint64_t max_catchup_entry;
  uint64_t max_chosen_entry;
  uint64_t max_plog_entry;

  uint64_t pre_auth_entry;
  uint32_t active_peer_acceptor_id;

  // The newest msg received for the entity when the entity_info is loading.
  std::unique_ptr<PaxosCmd> waiting_msg;

  // The current client cmd.
  std::unique_ptr<ClientCmd> client_cmd;

  uint64_t uuid_base;

  int32_t ref_count;
  LIGHTLIST(EntryInfo) entry_list;

  ArrayTimer<EntityInfo>::EltEntry timer_entry;

  uint64_t recover_timestamp_msec = 0;

  bool loading;
  bool range_loading;
  bool recover_pending;
};

std::string ToString(EntityInfo* entity_info);

class EntityInfoMng {
 public:
  EntityInfoMng(Options* options)
      : options_(options), monitor_(options->monitor()) {}
  ~EntityInfoMng() {}

  EntityInfo* FindEntityInfo(uint64_t entity_id);
  EntityInfo* CreateEntityInfo(uint64_t entity_id, uint32_t acceptor_num,
                               uint32_t local_acceptor_id);
  void DestroyEntityInfo(EntityInfo* entity_info);

  EntityInfo* NextEntityInfo();

  bool MakeEnoughRoom() { return true; }

 private:
  Options* options_;
  Monitor* monitor_;

  // entity_id -> entity_info
  std::unordered_map<uint64_t, std::unique_ptr<EntityInfo>> entity_info_table_;
  decltype(entity_info_table_)::iterator table_iter_;
};

class EntityInfoGroup : public Singleton<EntityInfoGroup> {
 public:
  void RegisterEntityInfo(uint64_t entity_id, EntityInfo* info);
  void RemoveEntityInfo(uint64_t entity_id);

  int GetMaxChosenEntry(uint64_t entity_id, uint64_t* max_chosen_entry,
                        uint64_t* max_cont_chosen_entry);

 private:
  struct Shard {
    std::unordered_map<uint64_t, EntityInfo*> table;
    ReadWriteLock lock;
  };
  std::array<Shard, 128> shards_;
};

}  // namespace certain
