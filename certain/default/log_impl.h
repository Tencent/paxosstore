#pragma once

#include "certain/log.h"

class LogImpl : public certain::LogBase {
 public:
  LogImpl(const char* module, const char* path, certain::LogLevel log_level,
          bool log_to_stderr = false);
  ~LogImpl() final;

  int Init();

  certain::LogLevel GetLogLevel() final { return log_level_; };

  void Log(certain::LogLevel level, const char* file, int line, const char* fmt,
           ...) final;

  void Flush() final;

 private:
  bool initialized_;
  std::string module_;
  std::string log_path_;
  certain::LogLevel log_level_;
};
