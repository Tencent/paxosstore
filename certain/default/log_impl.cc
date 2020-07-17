#include "default/log_impl.h"

#include <stdarg.h>
#include <sys/stat.h>

#include "glog/logging.h"
#include "utils/log.h"

namespace {
#define LEVEL(level)                \
  case certain::LogLevel::k##level: \
    return #level

const char* ToString(certain::LogLevel level) {
  switch (level) {
    LEVEL(Zero);
    LEVEL(Fatal);
    LEVEL(Error);
    LEVEL(Warn);
    LEVEL(Info);
    LEVEL(Debug);
  }
  return "Unknown";
}

#undef LEVEL
}  // namespace

LogImpl::LogImpl(const char* module, const char* log_path,
                 certain::LogLevel log_level, bool log_to_stderr) {
  initialized_ = false;
  module_ = module;
  log_path_ = log_path;
  log_level_ = log_level;

  FLAGS_log_dir = log_path_.c_str();
  FLAGS_logtostderr = log_to_stderr;
}

LogImpl::~LogImpl() {
  if (initialized_) {
    google::ShutdownGoogleLogging();
  }
  if (certain::Log::GetInstance()->GetLogImpl() == this) {
    certain::Log::GetInstance()->Reset();
  }
}

int LogImpl::Init() {
  if (!FLAGS_logtostderr) {
    int ret = mkdir(log_path_.c_str(), 0777);
    if (ret == -1 && errno != EEXIST) {
      fprintf(stderr, "path %s error: %s\n", log_path_.c_str(),
              strerror(errno));
      return -1;
    }
  }
  google::InitGoogleLogging(module_.c_str());
  initialized_ = true;
  return 0;
}

void LogImpl::Log(certain::LogLevel level, const char* file, int line,
                  const char* fmt, ...) {
  char buf[1024];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);

  google::LogMessage(file, line, google::GLOG_INFO).stream()
      << ToString(level) << " " << buf;
}

void LogImpl::Flush() { google::FlushLogFiles(google::INFO); }
