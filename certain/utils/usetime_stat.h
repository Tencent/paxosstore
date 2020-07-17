#pragma once

#include "utils/header.h"
#include "utils/time.h"

namespace certain {

class TimeDelta {
 public:
  TimeDelta() { Reset(); }
  ~TimeDelta() {}
  uint64_t DeltaUsec() {
    uint64_t prev_us_ = now_us_;
    now_us_ = GetTimeByUsec();
    return now_us_ - prev_us_;
  }
  void Reset() { now_us_ = GetTimeByUsec(); }

 private:
  uint64_t now_us_;
};

class StopHelper {
 public:
  StopHelper(std::string tag, uint64_t limit_count) {
    tag_ = tag;
    limit_count_ = limit_count;
    count_.store(0);
  }

  ~StopHelper() {}

  void AddCount(uint64_t delta) {
    count_.fetch_add(delta);
    if (count_.load() >= limit_count_) {
      CERTAIN_LOG_ZERO("%s stop count %lu", tag_.c_str(), count_.load());
    }
  }

  bool IsStopNow() {
    if (count_.load() >= limit_count_) {
      return true;
    }
    return false;
  }

 private:
  std::string tag_;
  std::atomic<uint64_t> count_;
  uint64_t limit_count_;
};

class LocalUseTimeStat {
 public:
  LocalUseTimeStat(std::string tag) {
    tag_ = tag;
    max_use_timeus_ = 0;
    total_use_timeus_ = 0;
    count_ = 0;
    enabled_ = true;
  }

  ~LocalUseTimeStat() {}
  CERTAIN_GET_SET(bool, enabled);
  CERTAIN_GET_SET(uint64_t, max_use_timeus);
  CERTAIN_GET_SET(uint64_t, total_use_timeus);
  CERTAIN_GET_SET(uint64_t, count);

  void Update(uint64_t use_timeus) {
    if (!enabled_) {
      return;
    }

    if (max_use_timeus_ < use_timeus) {
      max_use_timeus_ = use_timeus;
    }

    total_use_timeus_ += use_timeus;
    count_++;
  }

  void LogStat() {
    if (count_ == 0) {
      CERTAIN_LOG_ZERO("certain_stat %s cnt 0", tag_.c_str());
    } else {
      uint64_t avg_us = total_use_timeus_ / count_;
      CERTAIN_LOG_ZERO("certain_stat %s max_us %lu avg_us %lu cnt %lu",
                       tag_.c_str(), max_use_timeus_, avg_us, count_);
    }

    max_use_timeus_ = 0;
    total_use_timeus_ = 0;
    count_ = 0;
  }

 private:
  std::string tag_;
  bool enabled_;

  uint64_t max_use_timeus_;
  uint64_t total_use_timeus_;
  uint64_t count_;
};

class UseTimeStat {
 public:
  UseTimeStat(std::string tag) {
    tag_ = tag;
    max_use_timeus_.store(0);
    total_use_timeus_.store(0);
    count_.store(0);
    enabled_ = true;
  }

  ~UseTimeStat() {}
  CERTAIN_GET_SET(bool, enabled);

  void Update(uint64_t use_timeus) {
    if (!enabled_) {
      return;
    }

    if (max_use_timeus_ < use_timeus) {
      max_use_timeus_ = use_timeus;
    }

    total_use_timeus_.fetch_add(use_timeus);
    count_.fetch_add(1);
  }

  void Update(LocalUseTimeStat& stat) {
    if (!enabled_) {
      return;
    }

    if (max_use_timeus_ < stat.max_use_timeus()) {
      max_use_timeus_ = stat.max_use_timeus();
    }

    total_use_timeus_.fetch_add(stat.total_use_timeus());
    count_.fetch_add(stat.count());

    stat.set_max_use_timeus(0);
    stat.set_total_use_timeus(0);
    stat.set_count(0);
  }

  void LogStat() {
    uint64_t total_use_timeus = total_use_timeus_.fetch_and(0);
    uint64_t count = count_.fetch_and(0);
    uint64_t max_use_timeus = max_use_timeus_.fetch_and(0);

    if (count == 0) {
      CERTAIN_LOG_ZERO("certain_stat %s cnt 0", tag_.c_str());
    } else {
      uint64_t avg_us = total_use_timeus / count;
      CERTAIN_LOG_ZERO("certain_stat %s max_us %lu avg_us %lu cnt %lu",
                       tag_.c_str(), max_use_timeus, avg_us, count);
    }
  }

 private:
  std::string tag_;
  bool enabled_;

  std::atomic<uint64_t> max_use_timeus_;
  std::atomic<uint64_t> total_use_timeus_;
  std::atomic<uint64_t> count_;
};

}  // namespace certain
