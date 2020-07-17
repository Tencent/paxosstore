#include "example/task.h"

#include "certain/certain.h"
#include "certain/errors.h"
#include "co_routine.h"
#include "co_routine_inner.h"
#include "gflags/gflags.h"
#include "utils/co_lock.h"
#include "utils/crc32.h"
#include "utils/time.h"

DECLARE_string(task);
DECLARE_int32(index);

namespace {

struct Count {
  std::atomic<uint32_t> total_write_count{0};
  std::atomic<uint32_t> total_write_error_count{0};
  std::atomic<uint32_t> total_read_count{0};
  std::atomic<uint32_t> total_read_error_count{0};
  std::atomic<uint32_t> total_replay_count{0};
  std::atomic<uint32_t> total_replay_error_count{0};
  std::atomic<uint32_t> total_finish_count{0};

  ~Count() {
    printf("Total Write Count: %u Error Count: %u\n", total_write_count.load(),
           total_write_error_count.load());
    printf("Total Read Count: %u Error Count: %u\n", total_read_count.load(),
           total_read_error_count.load());
    printf("Total Replay Count: %u Error Count: %u\n",
           total_replay_count.load(), total_replay_error_count.load());
    printf("Total Finish Count: %u\n", total_finish_count.load());
    printf("Success Rate: %.3lf\n",
           total_finish_count.load() * 1. / total_write_count.load());
  }
};
static Count count;

}  // namespace

int Task::CheckFinish() {
  stat_->Update(local_stat_);
  if (routine_finish_cnt_ == routine_num_) {
    count.total_write_count += write_count_;
    count.total_write_error_count += write_error_count_;
    count.total_read_count += read_count_;
    count.total_read_error_count += read_error_count_;
    count.total_replay_count += replay_count_;
    count.total_replay_error_count += replay_error_count_;
    count.total_finish_count += routine_finish_cnt_ * count_;
    return -1;
  }
  return 0;
}

void Task::WriteTask(uint32_t rid) {
  uint64_t entity_id = ((id_ + 1) * 10000000000ULL + rid * 10000000ULL);
  auto value = std::string(100, 'a' + entity_id % 26);

  for (uint64_t i = 0; i < count_; ++i) {
    uint64_t t0 = certain::GetTimeByUsec();

    std::vector<uint64_t> uuids;
    uuids.push_back(((id_ + 1) * 200000000ULL + rid * 2000000ULL) + i);
    uint64_t entry = i + 1;

    while (true) {
      ++write_count_;
      int ret = certain::Certain::Write(certain::CmdOptions::Default(),
                                        entity_id, entry, value, uuids);
      if (ret != 0 && ret != certain::kRetCodeEntryNotMatch) {
        ++write_error_count_;
        continue;
      }
      break;
    }

    uint64_t t1 = certain::GetTimeByUsec();
    local_stat_.Update(t1 - t0);
  }
}

void Task::ReadTask(uint32_t rid) {
  uint64_t entity_id = ((id_ + 1) * 10000000000ULL + rid * 10000000ULL);
  uint64_t t0 = certain::GetTimeByUsec();

  while (true) {
    uint64_t entry;
    ++replay_count_;
    int ret = certain::Certain::Replay(certain::CmdOptions::Default(),
                                       entity_id, &entry);
    if (ret != 0) {
      ++replay_error_count_;
      CERTAIN_LOG_ERROR("replay e(%lu, %lu) ret %d", entity_id, entry, ret);
      poll(nullptr, 0, 1000);
      continue;
    }

    ++read_count_;
    ret = certain::Certain::Read(certain::CmdOptions::Default(), entity_id,
                                 ++entry);
    if (ret != 0) {
      ++read_error_count_;
      CERTAIN_LOG_ERROR("read e(%lu, %lu) ret %d", entity_id, entry, ret);
      poll(nullptr, 0, 1000);
      continue;
    }
    break;
  }

  uint64_t t1 = certain::GetTimeByUsec();
  local_stat_.Update(t1 - t0);
}

void Task::ConflictTask(uint32_t rid) {
  uint64_t entity_id = ((id_ + 1) * 10000000000ULL + rid * 10000000ULL);
  auto value = std::string(100, 'a' + (entity_id + FLAGS_index) % 26);

  for (uint64_t i = 0; i < count_; ++i) {
    uint64_t t0 = certain::GetTimeByUsec();

    std::vector<uint64_t> uuids;
    uuids.push_back(((id_ + 1) * 200000000ULL + rid * 2000000ULL) + i);
    uint64_t entry = i + 1;

    while (true) {
      uint64_t curr_entry = 0;
      ++replay_count_;
      int ret = certain::Certain::Replay(certain::CmdOptions::Default(),
                                         entity_id, &curr_entry);
      if (ret != 0) {
        ++replay_error_count_;
        CERTAIN_LOG_ERROR("replay e(%lu, %lu) ret %d", entity_id, entry, ret);
        poll(nullptr, 0, 1000);
        continue;
      }
      if (curr_entry >= entry) {
        break;
      }

      ++write_count_;
      assert(curr_entry + 1 == entry);
      ret = certain::Certain::Write(certain::CmdOptions::Default(), entity_id,
                                    entry, value, uuids);
      if (ret != 0) {
        ++write_error_count_;
        CERTAIN_LOG_ERROR("write E(%lu, %lu) ret %d", entity_id, entry, ret);
        continue;
      }
      break;
    }

    uint64_t t1 = certain::GetTimeByUsec();
    local_stat_.Update(t1 - t0);
  }
}

void Task::CoRun(uint32_t rid) {
  if (FLAGS_task == "write") {
    WriteTask(rid);
  } else if (FLAGS_task == "read") {
    ReadTask(rid);
  } else if (FLAGS_task == "conflict") {
    ConflictTask(rid);
  } else {
    assert(false);
  }
  CERTAIN_LOG_ZERO("finish id %u rid %u", id_, rid);
  routine_finish_cnt_++;
}

struct CoRunArgs {
  Task* task;
  uint32_t rid;
};

static void* CoRunEntry(void* args) {
  co_enable_hook_sys();
  CoRunArgs* corun_args = static_cast<CoRunArgs*>(args);
  corun_args->task->CoRun(corun_args->rid);
  return NULL;
}

static int EventLoopCallBackEntry(void* args) {
  certain::Tick::Run();
  Task* task = static_cast<Task*>(args);
  return task->CheckFinish();
}

void Task::Run() {
  co_init_curr_thread_env();

  CoRunArgs corun_args[routine_num_];
  for (uint32_t i = 0; i < routine_num_; ++i) {
    corun_args[i].task = this;
    corun_args[i].rid = i;

    stCoRoutine_t* co = NULL;
    co_create(&co, NULL, CoRunEntry, (void*)&corun_args[i]);
    co_resume(co);
  }

  co_eventloop(co_get_epoll_ct(), EventLoopCallBackEntry, this);
  stop_->AddCount(1);
}
