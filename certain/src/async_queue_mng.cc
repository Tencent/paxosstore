#include "src/async_queue_mng.h"

namespace certain {

#define CERTAIN_NEW_QUEUE(name, queue_cnt, queue_size)                 \
  do {                                                                 \
    assert(name == nullptr);                                           \
    name = std::make_unique<std::unique_ptr<AsyncQueue>[]>(queue_cnt); \
    for (uint32_t i = 0; i < queue_cnt; ++i) {                         \
      name[i] = std::make_unique<AsyncQueue>(queue_size);              \
    }                                                                  \
  } while (0);

int AsyncQueueMng::Init(Options* options) {
  msg_worker_num_ = options->msg_worker_num();
  entity_worker_num_ = options->entity_worker_num();
  db_worker_num_ = options->db_worker_num();
  db_limited_worker_num_ = options->db_limited_worker_num();
  plog_worker_num_ = options->plog_worker_num();
  plog_readonly_worker_num_ = options->plog_readonly_worker_num();
  recover_worker_num_ = options->recover_worker_num();
  catchup_worker_num_ = options->catchup_worker_num();
  tools_worker_num_ = options->tools_worker_num();

  user_queue_size_ = options->user_queue_size();
  msg_queue_size_ = options->msg_queue_size();
  entity_queue_size_ = options->entity_queue_size();
  db_queue_size_ = options->db_queue_size();
  catchup_queue_size_ = options->catchup_queue_size();
  plog_queue_size_ = options->plog_queue_size();
  recover_queue_size_ = options->recover_queue_size();
  tools_queue_size_ = options->tools_queue_size();

  // Goto msg workers.
  CERTAIN_NEW_QUEUE(msg_req_queues_, msg_worker_num_, msg_queue_size_);

  // Goto entity workers.
  CERTAIN_NEW_QUEUE(user_req_queues_, entity_worker_num_, user_queue_size_);
  CERTAIN_NEW_QUEUE(user_rsp_queues_, entity_worker_num_, user_queue_size_);
  CERTAIN_NEW_QUEUE(entity_req_queues_, entity_worker_num_, entity_queue_size_);
  CERTAIN_NEW_QUEUE(plog_rsp_queues_, entity_worker_num_, plog_queue_size_);
  CERTAIN_NEW_QUEUE(recover_rsp_queues_, entity_worker_num_,
                    recover_queue_size_);

  // Goto plog workers.
  CERTAIN_NEW_QUEUE(plog_req_queues_, plog_worker_num_, plog_queue_size_);
  CERTAIN_NEW_QUEUE(plog_readonly_req_queues_, plog_readonly_worker_num_,
                    plog_queue_size_);

  // Goto db workers.
  CERTAIN_NEW_QUEUE(db_req_queues_, db_worker_num_, db_queue_size_);
  CERTAIN_NEW_QUEUE(db_limited_req_queues_, db_limited_worker_num_,
                    db_queue_size_);

  // Goto recover workers.
  CERTAIN_NEW_QUEUE(recover_req_queues_, recover_worker_num_,
                    recover_queue_size_);

  // Goto catchup workers.
  CERTAIN_NEW_QUEUE(catchup_req_queues_, catchup_worker_num_,
                    catchup_queue_size_);

  CERTAIN_NEW_QUEUE(tools_req_queues_, tools_worker_num_, tools_queue_size_);

  return 0;
}

void AsyncQueueMng::Destroy() {
  user_req_queues_.reset();
  user_rsp_queues_.reset();

  msg_req_queues_.reset();
  entity_req_queues_.reset();
  db_req_queues_.reset();
  db_limited_req_queues_.reset();
  catchup_req_queues_.reset();
  recover_req_queues_.reset();
  recover_rsp_queues_.reset();
  tools_req_queues_.reset();

  plog_req_queues_.reset();
  plog_readonly_req_queues_.reset();
  plog_rsp_queues_.reset();
}

void AsyncQueueMng::LogQueueStat(
    const char* tag,
    std::unique_ptr<std::unique_ptr<AsyncQueue>[]>& async_queues,
    uint32_t queue_num, uint32_t queue_size) {
  uint32_t size, total = 0;
  char buffer[128];
  std::string stat;

  for (uint32_t i = 0; i < queue_num; ++i) {
    size = async_queues[i]->Size();
    total += size;
    snprintf(buffer, 128, " q[%u] %u", i, size);
    stat += buffer;
  }

  CERTAIN_LOG_ZERO("%s queue_size %u queue_num %u total %u %s", tag, queue_size,
                   queue_num, total, stat.c_str());
}

void AsyncQueueMng::LogAllQueueStat() {
  LogQueueStat("msg_req", msg_req_queues_, msg_worker_num_, msg_queue_size_);

  LogQueueStat("user_req", user_req_queues_, entity_worker_num_,
               user_queue_size_);

  LogQueueStat("user_rsp", user_rsp_queues_, entity_worker_num_,
               user_queue_size_);

  LogQueueStat("entity_req", entity_req_queues_, entity_worker_num_,
               entity_queue_size_);

  LogQueueStat("plog_rsp", plog_rsp_queues_, entity_worker_num_,
               plog_queue_size_);

  LogQueueStat("recover_rsp", recover_rsp_queues_, entity_worker_num_,
               recover_queue_size_);

  LogQueueStat("plog_req", plog_req_queues_, plog_worker_num_,
               plog_queue_size_);

  LogQueueStat("plog_readonly_req", plog_readonly_req_queues_,
               plog_readonly_worker_num_, plog_queue_size_);

  LogQueueStat("db_req", db_req_queues_, db_worker_num_, db_queue_size_);
  LogQueueStat("db_limited_req", db_limited_req_queues_, db_limited_worker_num_,
               db_queue_size_);

  LogQueueStat("recover_req", recover_req_queues_, recover_worker_num_,
               recover_queue_size_);

  LogQueueStat("catchup_req", catchup_req_queues_, catchup_worker_num_,
               catchup_queue_size_);

  LogQueueStat("tools_req", tools_req_queues_, tools_worker_num_,
               tools_queue_size_);
}

}  // namespace certain
