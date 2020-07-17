#include "default/log_impl.h"

#include "gtest/gtest.h"
#include "utils/log.h"

TEST(LogTest, Basic) {
  LogImpl log("mm", "./test_log.o", certain::LogLevel::kError);
  ASSERT_EQ(log.Init(), 0);
  log.Log(certain::LogLevel::kZero, "mock.cc", 123, "zero_%d", 1);
  log.Log(certain::LogLevel::kFatal, "mock.cc", 123, "fatal_%d", 2);
  log.Log(certain::LogLevel::kError, "mock.cc", 123, "error_%d", 3);
  log.Log(certain::LogLevel::kWarn, "mock.cc", 123, "warn_%d", 4);
  log.Log(certain::LogLevel::kInfo, "mock.cc", 123, "info_%d", 5);

  certain::Log::GetInstance()->Init(&log);
  ASSERT_EQ(certain::Log::GetInstance()->GetLogImpl()->GetLogLevel(),
            certain::LogLevel::kError);
  CERTAIN_LOG_ZERO("zero_%d", 11);
  CERTAIN_LOG_FATAL("fatal_%d", 22);
  CERTAIN_LOG_ERROR("error_%d", 33);
  CERTAIN_LOG_WARN("warn_%d", 44);
  CERTAIN_LOG_INFO("info_%d", 55);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
