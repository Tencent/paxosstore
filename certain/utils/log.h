#pragma once

#include "include/certain/log.h"
#include "include/certain/monitor.h"
#include "utils/header.h"
#include "utils/singleton.h"

namespace certain {

class Log : public Singleton<Log> {
 public:
  void Init(LogBase* log_impl, Monitor* monitor_impl = nullptr) {
    log_impl_ = log_impl;
    monitor_impl_ = monitor_impl;
  }

  LogBase* GetLogImpl() const { return log_impl_; }

  void Flush() {
    if (log_impl_) {
      log_impl_->Flush();
    }
  }

  void ReportFatalError() {
    if (monitor_impl_) {
      monitor_impl_->ReportFatalError();
    }
  }

  void Reset() {
    log_impl_ = nullptr;
    monitor_impl_ = nullptr;
  }

 private:
  LogBase* log_impl_ = nullptr;
  Monitor* monitor_impl_ = nullptr;

  Log() {}
  friend class Singleton<Log>;
};

}  // namespace certain

// Determine by log level before evaluation, so if a low log level is set, no
// extra cpu used by logging.

#define CERTAIN_LOG(level, fmt, args...)                                    \
  do {                                                                      \
    if (certain::LogLevel::k##level == certain::LogLevel::kFatal) {         \
      certain::Log::GetInstance()->ReportFatalError();                      \
    }                                                                       \
    auto log = certain::Log::GetInstance()->GetLogImpl();                   \
    if (log && log->GetLogLevel() >= certain::LogLevel::k##level) {         \
      static const int buffer_len = 256;                                    \
      static const auto file_len = strlen(__FILE__) + 1;                    \
      static const auto func_len = strlen(__func__);                        \
      static_assert(file_len + func_len < buffer_len, "buffer too short");  \
      char buffer[buffer_len] = __FILE__ ":";                               \
      memcpy(buffer + file_len, __func__, func_len);                        \
      buffer[file_len + func_len] = '\0';                                   \
      log->Log(certain::LogLevel::k##level, buffer, __LINE__, fmt, ##args); \
    }                                                                       \
  } while (0)

#define CERTAIN_LOG_ZERO(fmt, args...) CERTAIN_LOG(Zero, fmt, ##args)
#define CERTAIN_LOG_FATAL(fmt, args...) CERTAIN_LOG(Fatal, fmt, ##args)
#define CERTAIN_LOG_ERROR(fmt, args...) CERTAIN_LOG(Error, fmt, ##args)
#define CERTAIN_LOG_WARN(fmt, args...) CERTAIN_LOG(Warn, fmt, ##args)
#define CERTAIN_LOG_DEBUG(fmt, args...) CERTAIN_LOG(Debug, fmt, ##args)
#define CERTAIN_LOG_INFO(fmt, args...) CERTAIN_LOG(Info, fmt, ##args)
