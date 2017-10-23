#include "CoHashLock.h"

#include <string>
#include <gtest/gtest.h>

TEST(HashLockTest, Normal)
{
    {
        clsCoHashLock oCoHashLock(1);
        oCoHashLock.Lock(0);
        oCoHashLock.Unlock(0);
    }

    {
        clsCoHashLock oCoHashLock(2);
        oCoHashLock.Lock(0);
        oCoHashLock.Lock(1);
        oCoHashLock.Unlock(0);
        oCoHashLock.Unlock(1);
    }
}

struct TestInfo_t
{
    stCoRoutine_t *co;
    clsCoHashLock *po;
    int iCoID;
    bool bDone;
};

const int N = 10;
static TestInfo_t t[N] = { 0 };

void *CoRoutineTest(void *args)
{
    co_enable_hook_sys();
    TestInfo_t *pt = (TestInfo_t *)args;
    pt->po->Lock(0);
    pt->bDone = true;
    poll(NULL, 0, pt->iCoID % 7);
    pt->po->Unlock(0);
    return NULL;
}

int CoEpollTick(void *args)
{
    clsCoHashLock *po = (clsCoHashLock *)args;
    po->CheckAllLock();
    for (int i = 0; i < N; ++i)
    {
        if (!t[i].bDone)
        {
            printf("iCoID %d not done\n", i);
            return 0;
        }
    }
    return -1;
}

TEST(HashLockTest, CoRoutine)
{
    clsCoHashLock oCoHashLock(1);

    stCoEpoll_t * ev = co_get_epoll_ct();

    for (int i = 0; i < N; ++i)
    {
        t[i].iCoID = i;
        t[i].po = &oCoHashLock;

        co_create(&t[i].co, NULL, CoRoutineTest, &t[i]);
        co_resume(t[i].co);
    }

    co_eventloop(ev, CoEpollTick, &oCoHashLock);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
