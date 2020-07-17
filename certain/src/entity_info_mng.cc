#include "entity_info_mng.h"

#include "utils/hash.h"
#include "utils/memory.h"

namespace certain {

std::string ToString(EntityInfo* e) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer),
           "e %lu an %u local %u entrys: %lu %lu %lu %lu auth %lu active %u "
           "msg %u cli %u ref %d load %u range %u recover %u",
           e->entity_id, e->acceptor_num, e->local_acceptor_id,
           e->max_cont_chosen_entry, e->max_catchup_entry, e->max_chosen_entry,
           e->max_plog_entry, int64_t(e->pre_auth_entry),
           e->active_peer_acceptor_id, (e->waiting_msg != nullptr),
           (e->client_cmd != nullptr), e->ref_count, e->loading,
           e->range_loading, e->recover_pending);
  return buffer;
}

EntityInfo* EntityInfoMng::FindEntityInfo(uint64_t entity_id) {
  auto iter = entity_info_table_.find(entity_id);
  if (iter != entity_info_table_.end()) {
    return iter->second.get();
  } else {
    return nullptr;
  }
}

EntityInfo* EntityInfoMng::CreateEntityInfo(uint64_t entity_id,
                                            uint32_t acceptor_num,
                                            uint32_t local_acceptor_id) {
  monitor_->ReportEntityCreate();
  auto entity_info = std::make_unique<EntityInfo>();

  entity_info->entity_id = entity_id;
  entity_info->acceptor_num = acceptor_num;
  entity_info->local_acceptor_id = local_acceptor_id;

  entity_info->max_cont_chosen_entry = 0;
  entity_info->max_catchup_entry = 0;
  entity_info->max_chosen_entry = 0;
  entity_info->max_plog_entry = 0;
  // Assign pre-auth to the acceptor with id zero.
  entity_info->pre_auth_entry = (local_acceptor_id == 0 ? 0 : kInvalidEntry);
  entity_info->active_peer_acceptor_id = (local_acceptor_id + 1) % acceptor_num;

  entity_info->waiting_msg = nullptr;
  entity_info->client_cmd = nullptr;

  // [ip:32][timestamp:12][inc:20]
  uint32_t ip = InetAddr(options_->local_addr()).GetIpByUint32();
  uint32_t timestamp = GetTimeBySecond() << 20;
  entity_info->uuid_base = (uint64_t(ip) << 32) | timestamp;

  entity_info->ref_count = 0;
  LIGHTLIST_INIT(&entity_info->entry_list);
  entity_info->timer_entry.Init();

  entity_info->loading = false;
  entity_info->range_loading = false;
  entity_info->recover_pending = false;

  auto raw_pointer = entity_info.get();
  entity_info_table_[entity_id] = std::move(entity_info);
  EntityInfoGroup::GetInstance()->RegisterEntityInfo(entity_id, raw_pointer);
  return raw_pointer;
}

void EntityInfoMng::DestroyEntityInfo(EntityInfo* entity_info) {
  monitor_->ReportEntityDestroy();
  CERTAIN_LOG_INFO("entity_info: %s", ToString(entity_info).c_str());

  uint64_t entity_id = entity_info->entity_id;
  auto iter = entity_info_table_.find(entity_id);
  if (iter == entity_info_table_.end()) {
    CERTAIN_LOG_FATAL("entity_id %lu not found", entity_id);
    return;
  }

  if (entity_info->client_cmd != nullptr) {
    CERTAIN_LOG_FATAL("entity_id %lu client_cmd %s", entity_id,
                      entity_info->client_cmd->ToString().c_str());
    return;
  }

  if (entity_info->ref_count != 0) {
    CERTAIN_LOG_FATAL("entity_id %lu ref_count %d", entity_id,
                      entity_info->ref_count);
    return;
  }

  EntityInfoGroup::GetInstance()->RemoveEntityInfo(entity_id);

  if (table_iter_ == iter) {
    table_iter_ = entity_info_table_.erase(iter);
  } else {
    entity_info_table_.erase(iter);
  }
}

EntityInfo* EntityInfoMng::NextEntityInfo() {
  if (entity_info_table_.empty()) {
    return nullptr;
  }

  if (table_iter_ == entity_info_table_.end()) {
    table_iter_ = entity_info_table_.begin();
  }

  auto entity_info = table_iter_->second.get();
  table_iter_++;
  return entity_info;
}

void EntityInfoGroup::RegisterEntityInfo(uint64_t entity_id, EntityInfo* info) {
  auto& item = shards_[Hash(entity_id) % shards_.size()];
  ThreadWriteLock lock(&item.lock);
  item.table[entity_id] = info;
}

void EntityInfoGroup::RemoveEntityInfo(uint64_t entity_id) {
  auto& item = shards_[Hash(entity_id) % shards_.size()];
  ThreadWriteLock lock(&item.lock);
  item.table.erase(entity_id);
}

int EntityInfoGroup::GetMaxChosenEntry(uint64_t entity_id,
                                       uint64_t* max_chosen_entry,
                                       uint64_t* max_cont_chosen_entry) {
  auto& item = shards_[Hash(entity_id) % shards_.size()];
  ThreadReadLock lock(&item.lock);
  auto it = item.table.find(entity_id);
  if (it == item.table.end()) {
    return kRetCodeNotFound;
  }
  *max_chosen_entry = it->second->max_chosen_entry;
  *max_cont_chosen_entry = it->second->max_cont_chosen_entry;
  return 0;
}

}  // namespace certain
