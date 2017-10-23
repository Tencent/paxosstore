#include "UserWorker.h"

uint32_t clsUserWorker::m_iWorkerNum = 0;
clsUserWorker::clsUserQueue **clsUserWorker::m_ppUserQueue = NULL;

int clsUserWorker::CoEpollTick(void * arg)
{
    clsUserWorker * pUserWorker = (clsUserWorker*)arg;
    std::stack<UserRoutine_t *> & IdleCoList = *(pUserWorker->m_poCoWorkList);

    while( !IdleCoList.empty() )
    {
        clsCallDataBase *poCallData = NULL;
        int iRet = pUserWorker->m_poUserQueue->TakeByOneThread(&poCallData);
        if(iRet == 0)
        {
            assert(poCallData != NULL);

            UserRoutine_t * w = IdleCoList.top();
            w->pData = (void*)poCallData;
            w->bHasJob = true;
            IdleCoList.pop();
            co_resume( (stCoRoutine_t*)(w->pCo) );
        }
        else
        {
            break;
        }
    }

    pUserWorker->m_poCertainUserImpl->TickHandleCallBack();

    return 0;
}

void * clsUserWorker::UserRoutine(void * arg)
{
    UserRoutine_t * pUserRoutine = (UserRoutine_t *)arg;
    clsUserWorker * pUserWorker = (clsUserWorker *)pUserRoutine->pSelf;
    co_enable_hook_sys();

    pUserWorker->m_poCertainUserImpl->SetRoutineID(pUserRoutine->iRoutineID);

    while (1)
    {
        if (!pUserRoutine->bHasJob)
        {
            pUserWorker->m_poCoWorkList->push(pUserRoutine);
            co_yield_ct();
            continue;
        }
        pUserRoutine->bHasJob = false;

        if (pUserRoutine->pData == NULL)
        {
            continue;
        }

        clsCallDataBase *poCallData = (clsCallDataBase *)pUserRoutine->pData;
        poCallData->Proceed();

        co_disable_hook_sys();
        poCallData->Finish(false);
        co_enable_hook_sys();

        pUserRoutine->pData = NULL;
    }

    return NULL;
}

void clsUserWorker::Run()
{
    Certain::SetThreadTitle("user_%u", m_iWorkerID);
    CertainLogInfo("user_%u run", m_iWorkerID);

    co_enable_hook_sys();
    stCoEpoll_t * ev = co_get_epoll_ct();

    uint32_t m_iStartRoutineID = 0;
    for (int i = 0; i < 50; ++i)
    {
        UserRoutine_t *w = (UserRoutine_t*)calloc( 1,sizeof(UserRoutine_t) );
        stCoRoutine_t *co = NULL;
        co_create( &co, NULL, UserRoutine, w );

        int iRoutineID = m_iStartRoutineID + i;
        w->pCo = (void*)co;
        w->pSelf = this;
        w->pData = NULL;
        w->bHasJob = false;
        w->iRoutineID = iRoutineID;
        co_resume( (stCoRoutine_t *)(w->pCo) );
    }

    printf("UserWorker idx %d 50 Routine\n", m_iWorkerID);
    CertainLogImpt("UserWorker idx %d 50 Routine", m_iWorkerID);

    co_eventloop(ev, CoEpollTick, this);
}
