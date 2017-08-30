#include "utils/AOF.h"

namespace Certain
{

int MakeCertainDir(const char *pcPath)
{
	int iRet;

	iRet = access(pcPath, F_OK | R_OK | W_OK);
	if (iRet == 0)
	{
		return 0;
	}

	iRet = mkdir(pcPath, 0777);
	if (iRet != 0)
	{
		CertainLogFatal("mkdir failed %s errno %d", pcPath, errno);
		assert(false);
	}

	// For check only.
	iRet = access(pcPath, F_OK | R_OK | W_OK);
	if (iRet == -1)
	{
		CertainLogFatal("access failed %s errno %d", pcPath, errno);
		assert(false);
	}

	return 0;
}

void clsAppendOnlyFile::SyncBuffer(Buffer_t *ptBuffer)
{
	uint64_t iCnt = ptBuffer->iWritenCnt;

	// (TODO)rock: use a backgroud thread to flush
	ssize_t iRet = write(m_iFD, ptBuffer->pcData, iCnt);
	if (iRet == -1)
	{
		CertainLogFatal("write failed fd %d errno %d", m_iFD, errno);
	}
	AssertEqual(iRet, iCnt);

	// Set ptBuffer->iWritenCnt to zero.
	// Use __sync_bool_compare_and_swap for check only.
	assert(__sync_bool_compare_and_swap(&ptBuffer->iWritenCnt, iCnt, 0));

	// Set ptBuffer->iCurrPos to zero.
	__sync_fetch_and_and(&ptBuffer->iCurrPos, 0);

	__sync_fetch_and_add(&ptBuffer->iTotalSyncCnt, iCnt);
}

void clsAppendOnlyFile::FlushInner(Buffer_t *ptBuffer, bool bAsync)
{
	clsThreadLock oLock(&m_oFlushMutex);

	uint64_t iTotalSyncCnt = 0;
	iTotalSyncCnt = __sync_fetch_and_add(&ptBuffer->iTotalSyncCnt, 0);

	uint64_t iPos = __sync_fetch_and_add(&ptBuffer->iCurrPos,
			ptBuffer->iSyncSize);
	if (iPos >= ptBuffer->iSyncSize)
	{
		// The Buffer is being flushed by another thread,
		// return immediately if asynchronous.
		if (bAsync)
		{
			return;
		}

		// Wait until another thread finish flushing.
		while (ptBuffer->iTotalSyncCnt == iTotalSyncCnt)
		{
			assert(ptBuffer->iTotalSyncCnt >= iTotalSyncCnt);
			sched_yield();
		}
	}
	else
	{
		// Wait until all bytes is writen.
		while (ptBuffer->iWritenCnt != iPos)
		{
			assert(ptBuffer->iWritenCnt <= iPos);
			sched_yield();
		}

		SyncBuffer(ptBuffer);
	}
}

int clsAppendOnlyFile::TryAppend(Buffer_t *ptBuffer, const char *pcData,
		uint32_t iLen)
{
	if (iLen > ptBuffer->iMaxSize - ptBuffer->iSyncSize + 1)
	{
		return -1;
	}

	uint64_t iPos = __sync_fetch_and_add(&ptBuffer->iCurrPos, iLen);
	if (iPos >= ptBuffer->iSyncSize)
	{
		return -2;
	}

	assert(iLen <= ptBuffer->iMaxSize - iPos);
	memcpy(ptBuffer->pcData + iPos, pcData, iLen);

	uint64_t iLeastCurrPos = iPos + iLen;
	uint64_t iCnt = __sync_add_and_fetch(&ptBuffer->iWritenCnt, iLen);
	assert(iCnt <= ptBuffer->iMaxSize);

	if (iLeastCurrPos < ptBuffer->iSyncSize)
	{
		return 0;
	}

	while (1)
	{
		// Wait until another thread finish appending.
		if (iLeastCurrPos != ptBuffer->iWritenCnt)
		{
			assert(iLeastCurrPos >= ptBuffer->iWritenCnt);
			sched_yield();
			continue;
		}

		SyncBuffer(ptBuffer);
		break;
	}

	return 0;
}

clsAppendOnlyFile::clsAppendOnlyFile(const char * pcPath, uint64_t iSyncSize,
		uint64_t iBufferSize)
{
	m_strPath = pcPath;

	m_iFD = open(pcPath, O_CREAT | O_WRONLY | O_APPEND, 0666);
	if (m_iFD < 0)
	{
		CertainLogFatal("open %s ret %d", pcPath, m_iFD);
		assert(false);
	}

	memset(m_atBuffer, 0, sizeof(m_atBuffer));
	assert(iSyncSize <= iBufferSize);
	for (uint32_t i = 0; i < kBufferNum; ++i)
	{
		m_atBuffer[i].pcData = (char *)malloc(iBufferSize);
		m_atBuffer[i].iMaxSize = iBufferSize;
		m_atBuffer[i].iSyncSize = iSyncSize;
	}

	assert(kTryTime >= kBufferNum);
	for (uint32_t i = 0; i < kTryTime; ++i)
	{
		m_apBuffer[i] = &m_atBuffer[i % kBufferNum];
	}

	m_iFileSize = 0;
}

clsAppendOnlyFile::~clsAppendOnlyFile()
{
	Flush();

	for (uint32_t i = 0; i < kBufferNum; ++i)
	{
		m_atBuffer[i].iMaxSize = 0;
		free(m_atBuffer[i].pcData), m_atBuffer[i].pcData = NULL;
	}

	int iRet = close(m_iFD);
	if (iRet == -1)
	{
		CertainLogFatal("close failed fd %d errno %d", m_iFD, errno);
	}
}

int clsAppendOnlyFile::Write(const char *pcData, uint32_t iLen)
{
	if (iLen == 0)
	{
		return 0;
	}

	for (uint32_t i = 0; i < kTryTime; ++i)
	{
		if (TryAppend(m_apBuffer[i], pcData, iLen) == 0)
		{
			__sync_fetch_and_add(&m_iFileSize, iLen);
			return 0;
		}
	}

	return -1;
}

void clsAppendOnlyFile::Flush(bool bAsync)
{
	for (uint32_t i = 0; i < kBufferNum; ++i)
	{
		FlushInner(&m_atBuffer[i], bAsync);
	}
}

uint64_t clsAppendOnlyFile::GetFileSize()
{
	return m_iFileSize;
}

} // namespace Certain
