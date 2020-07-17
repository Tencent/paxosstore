#pragma once

#include <string>

namespace certain {

class Db {
 public:
  enum RecoverFlag : uint32_t {
    kNormal = 0,
    kRecover = 1,
  };

  virtual int Commit(uint64_t entity_id, uint64_t entry,
                     const std::string& value) = 0;

  virtual int GetStatus(uint64_t entity_id, uint64_t* max_committed_entry,
                        RecoverFlag* flag) = 0;

  virtual int SnapshotRecover(uint64_t entity_id, uint32_t start_acceptor_id,
                              uint64_t* max_committed_entry) = 0;

  virtual void LockEntity(uint64_t entity_id) = 0;

  virtual void UnlockEntity(uint64_t entity_id) = 0;
};

class DbEntityLock {
 public:
  DbEntityLock(Db* db, uint64_t entity_id) : db_(db), entity_id_(entity_id) {
    db_->LockEntity(entity_id_);
  }

  void Unlock() {
    if (!locked_) {
      return;
    }
    locked_ = false;
    db_->UnlockEntity(entity_id_);
  }

  ~DbEntityLock() { Unlock(); }

 private:
  Db* db_;
  uint64_t entity_id_;
  bool locked_ = true;
};

}  // namespace certain
