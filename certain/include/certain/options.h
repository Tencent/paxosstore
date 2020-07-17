#pragma once

#include <string>

#include "certain/log.h"
#include "certain/monitor.h"

namespace certain {
#define CERTAIN_OPTION(type, name, value) \
 private:                                 \
  type name##_ = value;                   \
                                          \
 public:                                  \
  type name() const { return name##_; }   \
  void set_##name(type name) { name##_ = name; }

class Options {
  CERTAIN_OPTION(uint32_t, oss_id_key, 0);

  CERTAIN_OPTION(uint32_t, acceptor_num, 3);
  CERTAIN_OPTION(uint32_t, enable_pre_auth, 1);

  CERTAIN_OPTION(uint32_t, msg_worker_num, 48);
  CERTAIN_OPTION(uint32_t, entity_worker_num, 48);
  CERTAIN_OPTION(uint32_t, plog_worker_num, 16);
  CERTAIN_OPTION(uint32_t, plog_readonly_worker_num, 32);
  CERTAIN_OPTION(uint32_t, plog_routine_num, 64);
  CERTAIN_OPTION(uint32_t, db_worker_num, 16);
  CERTAIN_OPTION(uint32_t, db_routine_num, 64);
  CERTAIN_OPTION(uint32_t, db_limited_worker_num, 16);
  CERTAIN_OPTION(uint32_t, recover_worker_num, 7);
  CERTAIN_OPTION(uint32_t, recover_routine_num, 64);
  CERTAIN_OPTION(uint32_t, catchup_worker_num, 8);
  CERTAIN_OPTION(uint32_t, catchup_routine_num, 64);
  CERTAIN_OPTION(uint32_t, tools_worker_num, 1);

  CERTAIN_OPTION(uint32_t, db_max_kb_per_second, 102400);
  CERTAIN_OPTION(uint32_t, db_max_count_per_second, 100000);
  CERTAIN_OPTION(uint32_t, recover_max_count_per_second, 100);
  CERTAIN_OPTION(uint32_t, catchup_max_kb_per_second, 102400);
  CERTAIN_OPTION(uint32_t, catchup_max_count_per_second, 80000);
  CERTAIN_OPTION(uint32_t, catchup_max_get_per_second, 80000);
  CERTAIN_OPTION(uint32_t, catchup_timeout_msec, 2000);

  CERTAIN_OPTION(uint32_t, user_queue_size, 10000);
  CERTAIN_OPTION(uint32_t, msg_queue_size, 30000);
  CERTAIN_OPTION(uint32_t, entity_queue_size, 40000);
  CERTAIN_OPTION(uint32_t, plog_queue_size, 50000);
  CERTAIN_OPTION(uint32_t, db_queue_size, 60000);
  CERTAIN_OPTION(uint32_t, recover_queue_size, 70000);
  CERTAIN_OPTION(uint32_t, catchup_queue_size, 80000);
  CERTAIN_OPTION(uint32_t, tools_queue_size, 50000);

  CERTAIN_OPTION(uint32_t, max_notify_num, 1000);
  CERTAIN_OPTION(uint32_t, context_queue_size, 2000);
  CERTAIN_OPTION(uint32_t, max_plog_batch_size, 20);

  CERTAIN_OPTION(uint32_t, client_cmd_timeout_msec, 600);
  CERTAIN_OPTION(uint32_t, active_timeout_sec, 120);
  CERTAIN_OPTION(uint32_t, max_mem_entry_num, 10000000);
  CERTAIN_OPTION(uint64_t, max_mem_entry_size, (8ul << 30));
  CERTAIN_OPTION(uint32_t, entry_timeout_sec, 3600);
  CERTAIN_OPTION(uint32_t, max_catchup_num, 10);
  CERTAIN_OPTION(uint32_t, max_replay_num, 10);

  CERTAIN_OPTION(std::string, local_addr, "");

  CERTAIN_OPTION(LogBase*, log, nullptr);
  CERTAIN_OPTION(Monitor*, monitor, &Monitor::Instance());
};

template <class T>
class DefaultBase {
 public:
  static const T& Default() {
    static T t;
    return t;
  }
};

class CmdOptions : public DefaultBase<CmdOptions> {
  // If CmdOptions.client_cmd_timeout_msec is zero, it will set as
  // Options.client_cmd_timeout_msec. It allows user to specify timeout for
  // each command.
  CERTAIN_OPTION(uint32_t, client_cmd_timeout_msec, 0);
};

#undef CERTAIN_OPTION
}  // namespace certain
