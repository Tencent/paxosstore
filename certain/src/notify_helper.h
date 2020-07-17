#pragma once

#include "co_routine.h"
#include "src/common.h"
#include "utils/header.h"
#include "utils/lock_free_queue.h"
#include "utils/log.h"
#include "utils/thread.h"

namespace certain {

class NotifyHelper : public Singleton<NotifyHelper> {
 private:
  uint32_t max_num_;
  std::unique_ptr<int[]> eventfds_;

  Mutex mutex_;
  std::list<uint32_t> idle_nid_list_;

 public:
  NotifyHelper();
  virtual ~NotifyHelper() {}

  int Init(Options* options);

  void Destroy();

  int TakeNid(uint32_t& nid);

  void PutBackNid(uint32_t nid);

  void NotifyNid(uint32_t nid);

  void WaitForNid(uint32_t nid);
};

}  // namespace certain
