#ifndef CERTAIN_UTILS_THREAD_H_
#define CERTAIN_UTILS_THREAD_H_

#include "utils/AutoHelper.h"
#include "utils/Logger.h"
#include "utils/Assert.h"

namespace Certain
{

class clsThreadBase
{
private:
    pthread_t m_tID;
    uint64_t m_iStopStartTimeMS;

    volatile bool m_bExiting;
    volatile bool m_bExited;
    volatile bool m_bStopFlag;

public:
    clsThreadBase() : m_iStopStartTimeMS(0),
    m_bExiting(false),
    m_bExited(false),
    m_bStopFlag(false) { }

    virtual ~clsThreadBase() { }

    static void *ThreadEntry(void *pArg)
    {
        clsThreadBase *poThreadWorker = static_cast<clsThreadBase *>(pArg);
        poThreadWorker->Run();
        return NULL;
    }

    void Start()
    {
        AssertEqual(pthread_create(&m_tID, NULL, ThreadEntry, this), 0);
    }

    bool IsExited();

    // Called by the workers.
    bool CheckIfExiting(uint64_t iStopWaitTimeMS);

    // SetExiting Called by the workers only.
    void SetExiting() { m_bExiting = true; }
    bool IsExiting() { return m_bExiting; }

    // SetStopFlag Called by the thread called Start() only.
    void SetStopFlag() { m_bStopFlag = true; }
    bool IsStopFlag() { return m_bStopFlag; }

    virtual void Run() = 0;
};

class clsMutex
{
public:
    clsMutex() { tMutex = PTHREAD_MUTEX_INITIALIZER; }
    ~clsMutex() { }

    void Lock()
    {
        AssertEqual(pthread_mutex_lock(&tMutex), 0);
    }

    void Unlock()
    {
        AssertEqual(pthread_mutex_unlock(&tMutex), 0);
    }

private:
    pthread_mutex_t tMutex;

    NO_COPYING_ALLOWED(clsMutex);
};

class clsThreadLock
{
public:
    explicit clsThreadLock(clsMutex *poMutex) : m_poMutex(poMutex)
    {
        m_poMutex->Lock();
    }
    ~clsThreadLock() { m_poMutex->Unlock(); }

private:
    clsMutex *const m_poMutex;

    NO_COPYING_ALLOWED(clsThreadLock);
};

class clsRWLock
{
public:
    clsRWLock() { m_tRWLock = PTHREAD_RWLOCK_INITIALIZER; }
    ~clsRWLock() { }

    void ReadLock()
    {
        AssertEqual(pthread_rwlock_rdlock(&m_tRWLock), 0);
    }

    void WriteLock()
    {
        AssertEqual(pthread_rwlock_wrlock(&m_tRWLock), 0);
    }

    void Unlock()
    {
        AssertEqual(pthread_rwlock_unlock(&m_tRWLock), 0);
    }

private:
    pthread_rwlock_t m_tRWLock;

    NO_COPYING_ALLOWED(clsRWLock);
};

class clsThreadReadLock
{
public:
    explicit clsThreadReadLock(clsRWLock *poRWLock) : m_poRWLock(poRWLock)
    {
        m_poRWLock->ReadLock();
    }
    ~clsThreadReadLock() { m_poRWLock->Unlock(); }

private:
    clsRWLock *const m_poRWLock;

    NO_COPYING_ALLOWED(clsThreadReadLock);
};

class clsThreadWriteLock
{
public:
    explicit clsThreadWriteLock(clsRWLock *poRWLock) : m_poRWLock(poRWLock)
    {
        m_poRWLock->WriteLock();
    }
    ~clsThreadWriteLock() { m_poRWLock->Unlock(); }

private:
    clsRWLock *const m_poRWLock;

    NO_COPYING_ALLOWED(clsThreadWriteLock);
};

inline void BindThreadAffinity(uint32_t iCoreID)
{
    cpu_set_t tSet;

    CPU_ZERO(&tSet);
    CPU_SET(iCoreID, &tSet);

    pthread_t tThread = pthread_self();
    AssertEqual(pthread_setaffinity_np(tThread, sizeof(tSet), &tSet), 0);
}

inline void SetThreadTitle(const char* fmt, ...)
{
    char pcTitle[16] = {0};

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(pcTitle, sizeof (pcTitle), fmt, ap);
    va_end(ap);

    AssertSyscall(prctl(PR_SET_NAME, pcTitle));
}

} // namespace Certain

#endif
