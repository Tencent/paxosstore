#pragma once
#include <array>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "utils/hash.h"
#include "utils/lru_table.h"
#include "utils/singleton.h"
#include "utils/time.h"

namespace certain {

class UuidMng : public Singleton<UuidMng> {
 public:
  static constexpr uint32_t kMaxMemUuidNum = 2000000;
  static constexpr uint32_t kShardNum = 1024;

  bool Exist(uint64_t entity_id, uint64_t uuid) {
    auto& item = shards_[Hash(entity_id) % shards_.size()];
    std::lock_guard<std::mutex> lock(item.mutex);
    CheckTimeout(item.table);

    return item.table.Find(uuid);
  }

  void Add(uint64_t entity_id, uint64_t uuid) {
    auto& item = shards_[Hash(entity_id) % shards_.size()];
    std::lock_guard<std::mutex> lock(item.mutex);
    CheckTimeout(item.table);

    uint32_t timeout = GetTimeBySecond() + 60;  // 1 min
    item.table.Add(uuid, timeout);
  }

 private:
  void CheckTimeout(LruTable<uint64_t, uint32_t>& table) {
    uint64_t uuid;
    uint32_t timeout;
    uint32_t now = GetTimeBySecond();
    while ((table.PeekOldest(uuid, timeout) && timeout <= now) ||
           table.Size() > kMaxMemUuidNum / kShardNum) {
      table.Remove(uuid);
    }
  }

 private:
  struct Shard {
    LruTable<uint64_t, uint32_t> table;
    std::mutex mutex;
  };
  std::array<Shard, kShardNum> shards_;
};

}  // namespace certain
