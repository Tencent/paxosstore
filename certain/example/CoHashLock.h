#pragma once
#include "certain/Certain.h"
#include "co_routine.h"

class clsAutoDisableHook
{
private:
    bool m_bDisabled;

public:
    clsAutoDisableHook()
    {
        m_bDisabled = false;

        if (co_is_enable_sys_hook())
        {
            m_bDisabled = true;
            co_disable_hook_sys();
        }
    }

    ~clsAutoDisableHook()
    {
        if (m_bDisabled)
        {
            co_enable_hook_sys();
        }
    }
};

struct LockItem_t
{
    stCoRoutine_t *co;
    uint32_t iBucketIdx;
};

// (TODO) now must only one clsCoHash, replace by others?
extern __thread list<LockItem_t> *g_plstLockItem;

class clsCoHashLock
{
private:
    uint32_t m_iBucketNum;
    Certain::clsMutex *m_poMutex;

    bool CheckIfInside(stCoRoutine_t *co);

public:
    clsCoHashLock(uint32_t iBucketNum);

    void Lock(uint32_t iHash);

    void Unlock(uint32_t iHash);

    // Call in the CoEpollTick func.
    void CheckAllLock();
};
