#pragma once

#include "utils/header.h"
#include "utils/log.h"
#include "utils/thread.h"
#include "utils/usetime_stat.h"

class Task : public certain::ThreadBase {
 public:
  Task(uint32_t id, uint32_t routine_num, uint64_t count,
       certain::UseTimeStat* stat, certain::StopHelper* stop)
      : ThreadBase("task_" + std::to_string(id)),
        id_(id),
        routine_num_(routine_num),
        count_(count),
        routine_finish_cnt_(0),
        stat_(stat),
        stop_(stop),
        local_stat_("local") {}

  int CheckFinish();
  void CoRun(uint32_t rid);

  void WriteTask(uint32_t rid);
  void ReadTask(uint32_t rid);
  void ConflictTask(uint32_t rid);

  void Run();

 private:
  uint32_t id_;
  uint32_t routine_num_;
  uint64_t count_;
  uint32_t routine_finish_cnt_;
  uint32_t write_count_ = 0;
  uint32_t write_error_count_ = 0;
  uint32_t read_count_ = 0;
  uint32_t read_error_count_ = 0;
  uint32_t replay_count_ = 0;
  uint32_t replay_error_count_ = 0;
  certain::UseTimeStat* stat_;
  certain::StopHelper* stop_;
  certain::LocalUseTimeStat local_stat_;
};
