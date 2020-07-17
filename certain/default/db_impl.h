#pragma once

#include <fstream>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include "certain/db.h"
#include "certain/errors.h"
#include "utils/co_lock.h"
#include "utils/crc32.h"
#include "utils/hash.h"
#include "utils/header.h"
#include "utils/thread.h"

class DbInfo {
 public:
  int Update(uint64_t entity_id, uint64_t entry, const std::string& data) {
    certain::ThreadWriteLock lock(&lock_);
    auto& info = map_[entity_id];
    if (info.entry + 1 != entry) {
      return certain::kImplDbNotMatch;
    }
    info.entry = entry;
    info.crc32 = certain::crc32(info.crc32, data.data(), data.size());
    return 0;
  }

  int Set(uint64_t entity_id, uint64_t entry, uint32_t crc32,
          certain::Db::RecoverFlag flag) {
    certain::ThreadWriteLock lock(&lock_);
    auto& info = map_[entity_id];
    info.entry = entry;
    info.crc32 = crc32;
    info.flag = flag;
    return 0;
  }

  int Get(uint64_t entity_id, uint64_t* entry, uint32_t* crc32,
          certain::Db::RecoverFlag* flag) {
    certain::ThreadReadLock lock(&lock_);
    auto it = map_.find(entity_id);
    if (it == map_.end()) {
      return certain::kImplDbNotFound;
    }
    *entry = it->second.entry;
    *crc32 = it->second.crc32;
    *flag = it->second.flag;
    return 0;
  }

  int Dump(const std::function<int(uint64_t, uint64_t, uint32_t)>& dumper) {
    certain::ThreadReadLock lock(&lock_);
    for (auto& it : map_) {
      int ret = dumper(it.first, it.second.entry, it.second.crc32);
      if (ret != 0) {
        return ret;
      }
    }
    return 0;
  }

 private:
  certain::ReadWriteLock lock_;
  struct Info {
    uint64_t entry = 0;
    uint32_t crc32 = 0;
    certain::Db::RecoverFlag flag = certain::Db::kNormal;
  };
  std::unordered_map<uint64_t, Info> map_;
};

// DB Implementation based on Memory
class DbImpl : public certain::Db {
 public:
  DbImpl(uint32_t bucket_num = 1024);

  virtual ~DbImpl();

  virtual int Commit(uint64_t entity_id, uint64_t entry,
                     const std::string& value) final {
    return Shard(entity_id).Update(entity_id, entry, value);
  }

  virtual int GetStatus(uint64_t entity_id, uint64_t* max_committed_entry,
                        certain::Db::RecoverFlag* flag) final {
    uint32_t crc32 = 0;
    return Shard(entity_id).Get(entity_id, max_committed_entry, &crc32, flag);
  }

  virtual int SnapshotRecover(uint64_t entity_id, uint32_t start_acceptor_id,
                              uint64_t* max_committed_entry) final;

  virtual void LockEntity(uint64_t entity_id) final;

  virtual void UnlockEntity(uint64_t entity_id) final;

  int FromFile(const std::string& path) {
    std::fstream f(path, std::ios::in);
    if (!f) {
      return certain::kImplUnknown;
    }
    return FromStream(f);
  }

  int ToFile(const std::string& path) {
    std::fstream f(path, std::ios::out);
    return ToStream(f);
  }

  DbInfo& Shard(uint64_t entity_id) {
    return buckets_[certain::Hash(entity_id) % buckets_.size()];
  }

 private:
  int Set(uint64_t entity_id, uint64_t entry, uint32_t crc32,
          RecoverFlag flag) {
    return Shard(entity_id).Set(entity_id, entry, crc32, flag);
  }

  int Dump(const std::function<int(uint64_t, uint64_t, uint32_t)>& dumper) {
    for (auto& info : buckets_) {
      int ret = info.Dump(dumper);
      if (ret != 0) {
        return ret;
      }
    }
    return 0;
  }

  int FromStream(std::istream& stream) {
    uint64_t entity_id;
    uint64_t entry;
    uint32_t crc32;
    uint32_t count = 0;
    while (stream >> entity_id >> entry >> crc32) {
      int ret = Set(entity_id, entry, crc32, kNormal);
      if (ret != 0) {
        return ret;
      }
      count++;
    }
    fprintf(stderr, "db's item count %u\n", count);
    return 0;
  }

  int ToStream(std::ostream& stream) {
    auto dumper = [&](uint64_t entity_id, uint64_t entry, uint32_t crc32) {
      stream << entity_id << " " << entry << " " << crc32 << std::endl;
      return 0;
    };
    int ret = Dump(dumper);
    stream.flush();
    return ret;
  }

 private:
  std::vector<DbInfo> buckets_;
  certain::CoHashLock lock_{1024};
};
