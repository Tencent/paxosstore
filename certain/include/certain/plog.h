#pragma once

#include <string>
#include <vector>

namespace certain {

class Plog {
 public:
  struct Record {
    uint64_t entity_id = -1;
    uint64_t entry = -1;
    std::string record;
  };

  virtual int LoadMaxEntry(uint64_t entity_id, uint64_t* entry) = 0;

  virtual int GetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       std::string* value) = 0;

  virtual int SetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       const std::string& value) = 0;

  virtual int GetRecord(uint64_t entity_id, uint64_t entry,
                        std::string* record) = 0;

  virtual int SetRecord(uint64_t entity_id, uint64_t entry,
                        const std::string& record) = 0;

  virtual uint32_t HashId(uint64_t entity_id) = 0;

  virtual int MultiSetRecords(uint32_t hash_id,
                              const std::vector<Record>& records) = 0;

  // Get and set entrys range in [begin_entry, end_entry) to records.
  virtual int RangeGetRecord(
      uint64_t entity_id, uint64_t begin_entry, uint64_t end_entry,
      std::vector<std::pair<uint64_t, std::string>>* records) = 0;
};

}  // namespace certain
