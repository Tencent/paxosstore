#include "default/plog_impl.h"

#include <unordered_set>

#include "utils/co_lock.h"
#include "utils/log.h"

namespace {
class __attribute__((packed)) EntryKey {
 public:
  EntryKey(uint64_t entity_id = 0, uint64_t entry = 0, uint64_t value_id = 0)
      : entity_id_(htobe64(entity_id)),
        entry_(htobe64(entry)),
        value_id_(htobe64(value_id)) {}

  uint64_t EntityId() const { return be64toh(entity_id_); }
  uint64_t Entry() const { return be64toh(entry_); }
  uint64_t ValueId() const { return be64toh(value_id_); }

  dbtype::Slice Serialize() {
    return dbtype::Slice(reinterpret_cast<const char*>(this), sizeof(*this));
  }

 private:
  // store in big-endian
  uint64_t entity_id_;
  uint64_t entry_;
  uint64_t value_id_;
};

inline const EntryKey& Parse(const dbtype::Slice& key) {
  assert(key.size() == sizeof(EntryKey));
  return *reinterpret_cast<const EntryKey*>(key.data());
}
}  // namespace

int PlogImpl::LoadMaxEntry(uint64_t entity_id, uint64_t* entry) {
  // enable coroutine
  static dbtype::ReadOptions options;
  std::unique_ptr<dbtype::Iterator> iter(DB(entity_id)->NewIterator(options));

  auto key = EntryKey(entity_id + 1, 0, 0);
  iter->Seek(key.Serialize());
  if (iter->Valid()) {
    iter->Prev();
  } else {
    iter->SeekToLast();
  }

  if (iter->Valid()) {
    auto& entry_key = Parse(iter->key());
    if (entry_key.EntityId() == entity_id) {
      *entry = entry_key.Entry();
      return 0;
    }
  }
  return certain::kImplPlogNotFound;
}

int PlogImpl::GetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       std::string* value) {
  assert(value_id > 0);
  auto key = EntryKey(entity_id, entry, value_id);
  return GetValue(DB(entity_id), key.Serialize(), value);
}

int PlogImpl::SetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       const std::string& value) {
  assert(value_id > 0);
  auto key = EntryKey(entity_id, entry, value_id);
  return SetValue(DB(entity_id), key.Serialize(), value);
}

int PlogImpl::GetRecord(uint64_t entity_id, uint64_t entry,
                        std::string* record) {
  auto key = EntryKey(entity_id, entry, 0);
  return GetValue(DB(entity_id), key.Serialize(), record);
}

int PlogImpl::SetRecord(uint64_t entity_id, uint64_t entry,
                        const std::string& record) {
  auto key = EntryKey(entity_id, entry, 0);
  return SetValue(DB(entity_id), key.Serialize(), record);
}

int PlogImpl::MultiSetRecords(
    uint32_t hash_id, const std::vector<certain::Plog::Record>& records) {
  assert(hash_id < dbs_.size());
  // disable coroutine
  certain::AutoDisableHook guard;

  dbtype::WriteBatch write_batch;
  for (auto& record : records) {
    auto key = EntryKey(record.entity_id, record.entry);
    write_batch.Put(key.Serialize(), record.record);
  }

  static dbtype::WriteOptions options;
  dbtype::Status s = dbs_[hash_id]->Write(options, &write_batch);

#ifdef LEVELDB_COMM
  while (s.IsLimitError()) {
    CERTAIN_LOG_ERROR("PlogImpl DB LimitError");
    poll(nullptr, 0, 100);
    s = dbs_[hash_id]->Write(options, &write_batch);
  }
#endif

  if (!s.ok()) {
    return certain::kImplPlogSetErr;
  }

  return 0;
}

int PlogImpl::RangeGetRecord(
    uint64_t entity_id, uint64_t begin_entry, uint64_t end_entry,
    std::vector<std::pair<uint64_t, std::string>>* records) {
  // enable coroutine
  records->clear();

  static dbtype::ReadOptions options;
  std::unique_ptr<dbtype::Iterator> iter(DB(entity_id)->NewIterator(options));

  auto key = EntryKey(entity_id, begin_entry + 1);
  for (iter->Seek(key.Serialize()); iter->Valid(); iter->Next()) {
    const dbtype::Slice& slice = iter->key();
    const EntryKey& key = Parse(slice);

    if (key.Entry() > end_entry || key.EntityId() > entity_id) {
      break;
    }

    if (key.ValueId() == 0) {
      // value==0 indicate record
      std::string value = iter->value().ToString();
      records->push_back(std::make_pair(key.Entry(), value));
    }
  }
  return 0;
}

int PlogImpl::SetValue(dbtype::DB* db, const dbtype::Slice& key,
                       const std::string& value) {
  // disable coroutine
  certain::AutoDisableHook guard;

  static dbtype::WriteOptions options;
  dbtype::Status s = db->Put(options, key, value);

#ifdef LEVELDB_COMM
  while (s.IsLimitError()) {
    CERTAIN_LOG_ERROR("PlogImpl DB LimitError");
    poll(nullptr, 0, 100);
    s = db->Put(options, key, value);
  }
#endif

  if (!s.ok()) {
    return certain::kImplPlogSetErr;
  }
  return 0;
}

int PlogImpl::GetValue(dbtype::DB* db, const dbtype::Slice& key,
                       std::string* value) {
  // enable coroutine
  static dbtype::ReadOptions options;
  dbtype::Status s = db->Get(options, key, value);

  if (!s.ok()) {
    if (s.IsNotFound()) {
      return certain::kImplPlogNotFound;
    }
    return certain::kImplPlogGetErr;
  }
  return 0;
}

void PlogImpl::ParseKey(const dbtype::Slice& key, uint64_t* entity_id,
                        uint64_t* entry) {
  auto& entry_key = Parse(key);
  *entity_id = entry_key.EntityId();
  *entry = entry_key.Entry();
}
