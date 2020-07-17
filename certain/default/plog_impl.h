#pragma once

#include "certain/errors.h"
#include "certain/plog.h"
#include "default/db_type.h"
#include "utils/hash.h"
#include "utils/header.h"

class PlogImpl : public certain::Plog {
 public:
  static void ParseKey(const dbtype::Slice& key, uint64_t* entity_id,
                       uint64_t* entry);

  PlogImpl(dbtype::DB* db) : dbs_(1, db) {}
  PlogImpl(const std::vector<dbtype::DB*>& dbs) : dbs_(dbs) {}

  virtual int LoadMaxEntry(uint64_t entity_id, uint64_t* entry) override;

  virtual int GetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       std::string* value) override;

  virtual int SetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       const std::string& value) override;

  virtual int GetRecord(uint64_t entity_id, uint64_t entry,
                        std::string* record) override;

  virtual int SetRecord(uint64_t entity_id, uint64_t entry,
                        const std::string& record) override;

  virtual uint32_t HashId(uint64_t entity_id) override {
    return Hash(entity_id);
  };

  virtual int MultiSetRecords(
      uint32_t hash_id,
      const std::vector<certain::Plog::Record>& records) override;

  virtual int RangeGetRecord(
      uint64_t entity_id, uint64_t begin_entry, uint64_t end_entry,
      std::vector<std::pair<uint64_t, std::string>>* records) override;

 private:
  static int SetValue(dbtype::DB* db, const dbtype::Slice& key,
                      const std::string& value);
  static int GetValue(dbtype::DB* db, const dbtype::Slice& key,
                      std::string* value);

  inline uint32_t Hash(uint64_t entity_id) {
    return certain::Hash(entity_id) % dbs_.size();
  }

  // use inline hash function instead of virtual function for performance
  inline dbtype::DB* DB(uint64_t entity_id) { return dbs_[Hash(entity_id)]; }

 private:
  std::vector<dbtype::DB*> dbs_;
};
