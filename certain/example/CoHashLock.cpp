#include "CoHashLock.h"

__thread list<LockItem_t> *g_plstLockItem = NULL;

bool clsCoHashLock::CheckIfInside(stCoRoutine_t *co)
{
    for (auto iter = g_plstLockItem->begin();
            iter != g_plstLockItem->end(); ++iter)
    {
        if (co == iter->co)
        {
            return true;
        }
    }
    return false;
}

clsCoHashLock::clsCoHashLock(uint32_t iBucketNum)
{
    assert(iBucketNum > 0);
    m_iBucketNum = iBucketNum;
    m_poMutex = new Certain::clsMutex[m_iBucketNum];
}

void clsCoHashLock::Lock(uint32_t iHash)
{
    if (g_plstLockItem == NULL)
    {
        g_plstLockItem = new list<LockItem_t>;
    }

    uint32_t iBucketIdx = iHash % m_iBucketNum;
    stCoRoutine_t *co = co_self();

    assert(!CheckIfInside(co));

    if (m_poMutex[iBucketIdx].TryLock())
    {
        return;
    }

    LockItem_t tItem = { 0 };
    tItem.co = co;
    tItem.iBucketIdx = iBucketIdx;

    g_plstLockItem->push_back(tItem);

    co_yield_ct();
}

void clsCoHashLock::Unlock(uint32_t iHash)
{
    uint32_t iBucketIdx = iHash % m_iBucketNum;
    m_poMutex[iBucketIdx].Unlock();
}

// Call in the CoEpollTick func.
void clsCoHashLock::CheckAllLock()
{
    if (g_plstLockItem == NULL)
    {
        g_plstLockItem = new list<LockItem_t>;
    }

    auto iter = g_plstLockItem->begin();
    while (iter != g_plstLockItem->end())
    {
        auto curr = iter;
        ++iter;

        stCoRoutine_t *co = curr->co;
        if (m_poMutex[curr->iBucketIdx].TryLock())
        {
            g_plstLockItem->erase(curr);
            co_resume(co);
        }
    }
}
