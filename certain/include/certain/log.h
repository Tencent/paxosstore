#pragma once

#include <string>

namespace certain {

enum struct LogLevel {
  kZero = 0,
  kFatal = 1,
  kError = 2,
  kWarn = 3,
  kInfo = 4,
  kDebug = 5,
};

class LogBase {
 public:
  virtual ~LogBase() {}

  virtual LogLevel GetLogLevel() = 0;

  virtual void Log(LogLevel level, const char* file, int line, const char* fmt,
                   ...) = 0;

  virtual void Flush() = 0;
};

}  // namespace certain
