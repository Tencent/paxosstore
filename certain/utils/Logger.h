#ifndef CERTAIN_UTILS_LOGGER_H_
#define CERTAIN_UTILS_LOGGER_H_

#include "utils/Header.h"
#include "utils/OSSReport.h"

const uint32_t kLogBufferSize = 8192;
extern __thread char g_acCertainLogBuffer[kLogBufferSize];
extern uint32_t g_iCertainLogLevel;
extern uint32_t g_iCertainUseLog;
extern uint32_t g_iCertainUseConsole;

namespace Certain
{

enum enumLogLevel
{
	eLogLevelZero	= 0,
	eLogLevelFatal	= 1,
	eLogLevelImpt	= 1,
	eLogLevelError	= 2,
	eLogLevelWarn	= 3,
	eLogLevelInfo	= 4,
	eLogLevelDebug	= 5,
};

#define REDEFINE_KVLOG_ZERO(format, ...) assert(false)
#define REDEFINE_KVLOG_FATAL(format, ...) assert(false)
#define REDEFINE_KVLOG_IMPT(format, ...) assert(false)
#define REDEFINE_KVLOG_ERROR(format, ...) assert(false)
#define REDEFINE_KVLOG_INFO(format, ...) assert(false)
#define REDEFINE_KVLOG_DEBUG(format, ...) assert(false)

int OpenLog(const char *pcFilePath, uint32_t iLogLevel, uint32_t iUseConsole, uint32_t iUseCertainLog);
void WriteLog(const char *pcBuffer, uint32_t iLen);

#define CertainLog(__level, fmt, args...) \
	do { \
		struct timeval __now_tv; \
		gettimeofday(&__now_tv, NULL); \
		const time_t __seconds = __now_tv.tv_sec; \
		struct tm __t; \
		localtime_r(&__seconds, &__t); \
		int __iLen = snprintf(g_acCertainLogBuffer, kLogBufferSize, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s %s:%s:%u " fmt "\n", \
				__t.tm_year + 1900, __t.tm_mon + 1, __t.tm_mday, __t.tm_hour, __t.tm_min, __t.tm_sec, static_cast<int>(__now_tv.tv_usec), \
				__level, __FILE__, __FUNCTION__, __LINE__, ##args); \
		if (__iLen > int(kLogBufferSize)) __iLen = kLogBufferSize; \
		if (strcmp(__level, "Fatal") == 0) { \
			Certain::OSS::ReportFatalErr(); \
		} \
		if (g_iCertainUseLog) Certain::WriteLog(g_acCertainLogBuffer, __iLen); \
	} while (0);

#define CertainLogDebug(fmt, args...) \
	do { \
		if (g_iCertainLogLevel < Certain::eLogLevelDebug) break; \
		if (g_iCertainUseLog) { CertainLog("Debug", fmt, ##args); } \
		else { REDEFINE_KVLOG_DEBUG(fmt, ##args); } \
	} while (0);

#define CertainLogInfo(fmt, args...) \
	do { \
		if (g_iCertainLogLevel < Certain::eLogLevelInfo) break; \
		if (g_iCertainUseLog) { CertainLog("Info", fmt, ##args); } \
		else { REDEFINE_KVLOG_INFO(fmt, ##args); } \
	} while (0);

#define CertainLogWarn(fmt, args...) \
	do { \
		if (g_iCertainLogLevel < Certain::eLogLevelWarn) break; \
		if (g_iCertainUseLog) { CertainLog("Warn", fmt, ##args); } \
		else { REDEFINE_KVLOG_ERROR(fmt, ##args); } \
	} while (0);

#define CertainLogError(fmt, args...) \
	do { \
		if (g_iCertainLogLevel < Certain::eLogLevelError) break; \
		if (g_iCertainUseLog) { CertainLog("Error", fmt, ##args); } \
		else { REDEFINE_KVLOG_ERROR(fmt, ##args); } \
	} while (0);

#define CertainLogFatal(fmt, args...) \
	do { \
		if (g_iCertainLogLevel < Certain::eLogLevelFatal) break; \
		CertainLog("Fatal", fmt, ##args); \
		if (!g_iCertainUseLog) { REDEFINE_KVLOG_FATAL(fmt, ##args); } \
	} while (0);

#define CertainLogZero(fmt, args...) \
	do { \
		if (g_iCertainUseLog) { CertainLog("Zero", fmt, ##args); } \
		else { REDEFINE_KVLOG_ZERO(fmt, ##args); } \
	} while (0);

#define CertainLogImpt(fmt, args...) \
	do { \
		if (g_iCertainLogLevel < Certain::eLogLevelImpt) break; \
		if (g_iCertainUseLog) { CertainLog("Impt", fmt, ##args); } \
		else { REDEFINE_KVLOG_IMPT(fmt, ##args); } \
	} while (0);

} // namespace Certain

#endif // CERTAIN_LOGGER_H_
