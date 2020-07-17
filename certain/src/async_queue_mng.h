#pragma once

#include "certain/options.h"
#include "src/command.h"
#include "utils/hash.h"
#include "utils/lock_free_queue.h"
#include "utils/log.h"

namespace certain {

class CmdBase;
typedef LockFreeQueue<CmdBase> AsyncQueue;

class AsyncQueueMng : public Singleton<AsyncQueueMng> {
 private:
  uint32_t msg_worker_num_;
  uint32_t plog_worker_num_;
  uint32_t plog_readonly_worker_num_;
  uint32_t entity_worker_num_;
  uint32_t db_worker_num_;
  uint32_t db_limited_worker_num_;
  uint32_t catchup_worker_num_;
  uint32_t recover_worker_num_;
  uint32_t tools_worker_num_;

  uint32_t user_queue_size_;
  uint32_t msg_queue_size_;
  uint32_t plog_queue_size_;
  uint32_t entity_queue_size_;
  uint32_t db_queue_size_;
  uint32_t catchup_queue_size_;
  uint32_t recover_queue_size_;
  uint32_t tools_queue_size_;

  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> user_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> user_rsp_queues_;

  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> msg_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> entity_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> db_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> db_limited_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> catchup_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> recover_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> recover_rsp_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> tools_req_queues_;

  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> plog_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> plog_readonly_req_queues_;
  std::unique_ptr<std::unique_ptr<AsyncQueue>[]> plog_rsp_queues_;

 public:
  AsyncQueueMng() {}
  virtual ~AsyncQueueMng() {}

  int Init(Options* options);
  void Destroy();

#define CERTAIN_GET_QUEUE(fname, name, queue_cnt)     \
  AsyncQueue* fname##ByIdx(uint32_t id) {             \
    assert(id < queue_cnt);                           \
    assert(name[id] != nullptr);                      \
    return name[id].get();                            \
  }                                                   \
  AsyncQueue* fname##ByEntityId(uint64_t entity_id) { \
    uint32_t id = Hash(entity_id) % queue_cnt;        \
    assert(name[id] != nullptr);                      \
    return name[id].get();                            \
  }

  CERTAIN_GET_QUEUE(GetMsgReqQueue, msg_req_queues_, msg_worker_num_);

  CERTAIN_GET_QUEUE(GetUserReqQueue, user_req_queues_, entity_worker_num_);
  CERTAIN_GET_QUEUE(GetUserRspQueue, user_rsp_queues_, entity_worker_num_);

  CERTAIN_GET_QUEUE(GetEntityReqQueue, entity_req_queues_, entity_worker_num_);
  CERTAIN_GET_QUEUE(GetPlogRspQueue, plog_rsp_queues_, entity_worker_num_);
  CERTAIN_GET_QUEUE(GetRecoverRspQueue, recover_rsp_queues_,
                    entity_worker_num_);

  CERTAIN_GET_QUEUE(GetPlogReqQueue, plog_req_queues_, plog_worker_num_);
  CERTAIN_GET_QUEUE(GetPlogReadonlyReqQueue, plog_readonly_req_queues_,
                    plog_readonly_worker_num_);

  CERTAIN_GET_QUEUE(GetDbReqQueue, db_req_queues_, db_worker_num_);
  CERTAIN_GET_QUEUE(GetDbLimitedReqQueue, db_limited_req_queues_,
                    db_limited_worker_num_);

  CERTAIN_GET_QUEUE(GetRecoverReqQueue, recover_req_queues_,
                    recover_worker_num_);

  CERTAIN_GET_QUEUE(GetCatchupReqQueue, catchup_req_queues_,
                    catchup_worker_num_);

  CERTAIN_GET_QUEUE(GetToolsReqQueue, tools_req_queues_, tools_worker_num_);

  void LogQueueStat(
      const char* tag,
      std::unique_ptr<std::unique_ptr<AsyncQueue>[]>& async_queues,
      uint32_t queue_num, uint32_t queue_size);

  void LogAllQueueStat();
};

}  // namespace certain
