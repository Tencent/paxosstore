#pragma once

#include "utils/header.h"
#include "utils/light_list.h"

namespace certain {

template <typename K, typename V>
class LruTable {
 private:
  struct LruElt;
  typedef std::unordered_map<K, LruElt> HashTable;
  typedef typename HashTable::iterator HashTableIter;

  struct LruElt {
    K key;
    V value;
    LIGHTLIST_ENTRY(LruElt) list_entry;

    LruElt() {}

    LruElt(const K& _key, const V& _value) {
      key = _key;
      value = _value;
      LIGHTLIST_ENTRY_INIT(this, list_entry);
    }
  };

  uint64_t max_size_;
  bool auto_eliminate_;

  HashTable hash_table_;

  LIGHTLIST(LruElt) lru_list_;

 public:
  LruTable(uint64_t max_size = 1024, bool auto_eliminate = false) {
    max_size_ = max_size;
    auto_eliminate_ = auto_eliminate;

    hash_table_.reserve(max_size_);

    LIGHTLIST_INIT(&lru_list_);
  }

  virtual ~LruTable() {}

  uint64_t Size() { return hash_table_.size(); }

  bool Add(const K& key, const V& value) {
    bool ok = false;
    LruElt* elt = NULL;
    HashTableIter iter = hash_table_.find(key);

    if (iter != hash_table_.end()) {
      LIGHTLIST_REMOVE(&lru_list_, &iter->second, list_entry);
      iter->second = LruElt(key, value);
      elt = &(iter->second);
    } else {
      elt = &(hash_table_[key] = LruElt(key, value));
      ok = true;
    }

    LIGHTLIST_INSERT_HEAD(&lru_list_, elt, list_entry);

    if (auto_eliminate_ && hash_table_.size() > max_size_) {
      bool removed = RemoveOldest();
      assert(removed);
    }

    return ok;
  }

  bool Remove(const K& key) {
    HashTableIter iter = hash_table_.find(key);
    if (iter == hash_table_.end()) {
      return false;
    }

    LIGHTLIST_REMOVE(&lru_list_, &iter->second, list_entry);
    hash_table_.erase(iter);

    return true;
  }

  bool RemoveOldest() {
    if (hash_table_.size() == 0) {
      return false;
    }

    LruElt* elt = LIGHTLIST_LAST(&lru_list_);
    bool removed = Remove(elt->key);
    assert(removed);

    return true;
  }

  bool PeekOldest(K& key, V& value) {
    if (hash_table_.size() == 0) {
      return false;
    }

    LruElt* elt = LIGHTLIST_LAST(&lru_list_);
    key = elt->key;
    value = elt->value;

    return true;
  }

  bool Find(const K& key, V& value) {
    HashTableIter iter = hash_table_.find(key);

    if (iter == hash_table_.end()) {
      return false;
    }

    value = iter->second.value;
    return true;
  }

  bool Find(const K& key) { return hash_table_.find(key) != hash_table_.end(); }

  bool Refresh(const K& key, bool newest = true) {
    HashTableIter iter = hash_table_.find(key);
    if (iter == hash_table_.end()) {
      return false;
    }

    LIGHTLIST_REMOVE(&lru_list_, &iter->second, list_entry);
    LruElt* elt = &(iter->second);

    if (newest) {
      LIGHTLIST_INSERT_HEAD(&lru_list_, elt, list_entry);
    } else {
      LIGHTLIST_INSERT_TAIL(&lru_list_, elt, list_entry);
    }

    return true;
  }

  bool OverLoad() { return hash_table_.size() > max_size_; }
};

}  // namespace certain
