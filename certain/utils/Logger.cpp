#include "Logger.h"

__thread char g_acCertainLogBuffer[kCertainLogBufSize];
uint32_t g_iCertainLogLevel;
uint32_t g_iCertainUseLog;
uint32_t g_iCertainUseConsole;

namespace Certain
{

static int s_iLogFD;

int OpenLog(const char *pcFilePath, uint32_t iLogLevel, uint32_t iUseConsole,
        uint32_t iUseCertainLog)
{
	g_iCertainLogLevel = iLogLevel;
	g_iCertainUseLog = iUseCertainLog;
	g_iCertainUseConsole = iUseConsole;

	if (iUseConsole)
	{
		s_iLogFD = 1;
		return 0;
	}

	s_iLogFD = open(pcFilePath, O_CREAT | O_WRONLY | O_APPEND, 0666);
	if (s_iLogFD == -1)
	{
		fprintf(stderr, "open %s failed errno %d\n", pcFilePath, s_iLogFD);
		return -1;
	}
	return 0;
}

void WriteLog(const char *pcBuffer, uint32_t iLen)
{
	if (s_iLogFD == 0)
	{
		fprintf(stderr, "%s", pcBuffer);
		return;
	}

	int iRet = write(s_iLogFD, pcBuffer, iLen);
	if ((uint32_t)iRet != iLen)
	{
		fprintf(stderr, "write s_iLogFD %d iRet %d iLen %u errno %d\n",
                s_iLogFD, iRet, iLen, errno);
		assert(false);
	}
}

} // namespace Certain
