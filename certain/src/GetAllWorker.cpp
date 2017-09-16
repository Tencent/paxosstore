#include "GetAllWorker.h"
#include "EntityWorker.h"

#include "co_routine.h"

namespace Certain
{

void * clsGetAllWorker::GetAllRoutine(void * arg)
{
    GetAllRoutine_t * pGetAllRoutine = (GetAllRoutine_t *)arg;
    co_enable_hook_sys();

    clsCertainUserBase * pCertainUser = clsCertainWrapper::GetInstance()->GetCertainUser();
    pCertainUser->SetRoutineID(pGetAllRoutine->iRoutineID);

    while(1)
    {
        clsGetAllWorker * pGetAllWorker = (clsGetAllWorker * )pGetAllRoutine->pSelf;

        if(!pGetAllRoutine->bHasJob)
        {
            pGetAllWorker->m_poCoWorkList->push(pGetAllRoutine);
            co_yield_ct();
            continue;
        }
        pGetAllRoutine->bHasJob = false;
        if(pGetAllRoutine->pData == NULL)
        {
            continue;
        }

        pGetAllWorker->HandleInQueue((clsPaxosCmd*) pGetAllRoutine->pData);
    }

    return NULL;
}

void clsGetAllWorker::Run()
{
    int cpu_cnt = GetCpuCount();

    if (cpu_cnt == 48)
    {
        SetCpu(8, cpu_cnt);
    }
    else
    {
        SetCpu(4, cpu_cnt);
    }

    uint32_t iLocalServerID = m_poConf->GetLocalServerID();
    SetThreadTitle("getall_%u_%u", iLocalServerID, m_iWorkerID);
    CertainLogInfo("getall_%u_%u run", iLocalServerID, m_iWorkerID);

    co_enable_hook_sys();
    stCoEpoll_t * ev = co_get_epoll_ct();

    for(int i=0; i<int(m_poConf->GetGetAllRoutineCnt()); i++)
    {
        GetAllRoutine_t *w = (GetAllRoutine_t*)calloc( 1,sizeof(GetAllRoutine_t) );
        stCoRoutine_t *co = NULL;
        co_create( &co, NULL, GetAllRoutine, w );

        int iRoutineID = m_iStartRoutineID + i;
        w->pCo = (void*)co;
        w->pSelf = this;
        w->pData = NULL;
        w->bHasJob = false;
        w->iRoutineID = iRoutineID;
        co_resume( (stCoRoutine_t *)(w->pCo) );
    }

    printf("GetAllWorker idx %d %u Routine\n", m_iWorkerID, m_poConf->GetGetAllRoutineCnt());
    CertainLogImpt("GetAllWorker idx %d %u Routine", m_iWorkerID, m_poConf->GetGetAllRoutineCnt());

    co_eventloop( ev, CoEpollTick, this);
}

int clsGetAllWorker::CoEpollTick(void * arg)
{
    clsGetAllWorker * pGetAllWorker = (clsGetAllWorker*)arg;
    std::stack<GetAllRoutine_t *> & IdleCoList = *(pGetAllWorker->m_poCoWorkList);

    if (pGetAllWorker->CheckIfExiting(0))
    {
        return -1;
    }

    static __thread uint64_t iLoopCnt = 0;

    clsGetAllReqQueue *poGetAllReqQueue = pGetAllWorker->m_poQueueMng->GetGetAllReqQueue(pGetAllWorker->GetWorkerID());

    static __thread uint64_t iLastSleepTimeMS = 0;

    while( !IdleCoList.empty() )
    {
        clsPaxosCmd *poCmd = NULL;
        int iRet = poGetAllReqQueue->TakeByOneThread(&poCmd);
        if(iRet == 0 && poCmd)
        {
            if( ( (++iLoopCnt) % 10 ) == 0)
            {
                CertainLogError("GetAllQueue size %u", poGetAllReqQueue->Size());
            }

            uint32_t iGetAllMaxNum = pGetAllWorker->m_poConf->GetGetAllMaxNum();
            if (iGetAllMaxNum > 0 && iLoopCnt % iGetAllMaxNum == 0)
            {
                uint64_t iCurrTimeMS = GetCurrTimeMS();
                uint64_t iTimeoutMS = iCurrTimeMS - iLastSleepTimeMS;
                if (iTimeoutMS >= 1000)
                {
                    iTimeoutMS = 0;
                }
                else
                {
                    iTimeoutMS = 1000 - iTimeoutMS;
                }

                if (iTimeoutMS > 0)
                {
                    poll(NULL, 0, iTimeoutMS);
                }

                iLastSleepTimeMS = GetCurrTimeMS();
            }

            GetAllRoutine_t * w = IdleCoList.top();
            w->pData = (void*)poCmd;
            w->bHasJob = true;
            IdleCoList.pop();
            co_resume( (stCoRoutine_t*)(w->pCo) );
        }
        else
        {
            break;
        }
    }

    clsCertainUserBase * pCertainUser = clsCertainWrapper::GetInstance()->GetCertainUser();
    pCertainUser->TickHandleCallBack();

    return 0;
}


int clsGetAllWorker::HandleInQueue(clsPaxosCmd * poCmd)
{
    OSS::ReportGetAllReq();

    clsDBBase * pDataDB = clsCertainWrapper::GetInstance()->GetDBEngine();
    uint64_t iCommitPos = 0;
    int iRet = pDataDB->GetAllAndSet(poCmd->GetEntityID(), poCmd->GetSrcAcceptorID(), iCommitPos);
    if(iRet != 0)
    {
        CertainLogError("EntityID %lu GetAllAndSet iRet %d", poCmd->GetEntityID(), iRet);
        OSS::ReportGetAllFail();
    }

    poCmd->SetResult(iRet);

    poCmd->SetEntry(iCommitPos);
    uint64_t iEntityID = poCmd->GetEntityID();
    uint32_t iSrcAcceptorID = poCmd->GetSrcAcceptorID();

    int iLoop = 0;
    while(1)
    {
        iRet= clsEntityWorker::EnterGetAllRspQueue(poCmd);
        if(iRet != 0)
        {
            CertainLogError("EntityID %lu EnterGetAllRspQueue iRet %d iLoop %d", poCmd->GetEntityID(), iRet, iLoop);
            poll(NULL, 0, 10);   
            iLoop++;
        }
        else
        {
            break;
        }
    }

    CertainLogError("EntityID %lu srcAcceptorid %u CommitPos %lu iRet %d",
            iEntityID, iSrcAcceptorID, iCommitPos, iRet);

    return 0;
}

int clsGetAllWorker::EnterReqQueue(clsPaxosCmd *poCmd)
{
    uint64_t iEntityID = poCmd->GetEntityID();

    int iGetAllWorkerNum = clsCertainWrapper::GetInstance()->GetConf()->GetGetAllWorkerNum();

    clsGetAllReqQueue *poQueue = clsAsyncQueueMng::GetInstance()->GetGetAllReqQueue(
            Hash(iEntityID) % iGetAllWorkerNum );

    int iRet = poQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        OSS::ReportGetAllQueueErr();
        CertainLogError("PushByMultiThread ret %d cmd: %s",
                iRet, poCmd->GetTextCmd().c_str());
        return -1;
    }

    return 0;
}

} // namespace Certain
