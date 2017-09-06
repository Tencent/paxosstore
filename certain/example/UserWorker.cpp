#include "UserWorker.h"
#include "utils/Hash.h"


namespace Certain
{

uint32_t clsUserWorker::m_iWorkerNum = 0;
clsUserWorker::clsUserQueue **clsUserWorker::m_ppUserQueue = NULL;

void clsUserWorker::DoWithUserCmd(clsClientCmd *poCmd)
{
    int iRet;
    clsCertainWrapper *poWrapper = clsCertainWrapper::GetInstance();

    CertainLogInfo("cmd %s", poCmd->GetTextCmd().c_str());

    uint64_t iMaxCommitedEntry = 0;
    iRet = poWrapper->EntityCatchUp(poCmd->GetEntityID(), iMaxCommitedEntry);
    if (iRet != 0)
    {
        CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
        poCmd->SetResult(iRet);
        assert(clsIOWorkerRouter::GetInstance()->Go(poCmd) == 0);
        return ;
    }
    poCmd->SetEntry(iMaxCommitedEntry + 1);
    vector<uint64_t> vecWBUUID;

    if (poCmd->GetSubCmdID() == clsSimpleCmd::kGet)
    {
        string strWriteBatch;
        iRet = poWrapper->RunPaxos(poCmd->GetEntityID(), poCmd->GetEntry(),
                iMaxCommitedEntry + 1, vecWBUUID, strWriteBatch);
        if (iRet != 0)
        {
            CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
        }
        else
        {
            iRet = poWrapper->GetDBEngine()->ExcuteCmd(poCmd, strWriteBatch);
            if (iRet != 0)
            {
                CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
            }
        }

        poCmd->SetResult(iRet);
    }
    else if (poCmd->GetSubCmdID() == clsSimpleCmd::kSet)
    {
        string strWriteBatch;
        iRet = poWrapper->GetDBEngine()->ExcuteCmd(poCmd, strWriteBatch);
        if (iRet != 0)
        {
            CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
        }
        else
        {
            iRet = poWrapper->RunPaxos(poCmd->GetEntityID(), poCmd->GetEntry(),
                    iMaxCommitedEntry + 1, vecWBUUID, strWriteBatch);
            if (iRet != 0)
            {
                CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
            }
        }

        poCmd->SetResult(iRet);
    }
    else
    {
        assert(false);
    }

    assert(clsIOWorkerRouter::GetInstance()->Go(poCmd) == 0);
}

int clsUserWorker::CoEpollTick(void * arg)
{
	clsUserWorker * pUserWorker = (clsUserWorker*)arg;
	std::stack<UserRoutine_t *> & IdleCoList = *(pUserWorker->m_poCoWorkList);

	while( !IdleCoList.empty() )
	{
        clsClientCmd *poCmd = NULL;
        int iRet = pUserWorker->m_poUserQueue->TakeByOneThread(&poCmd);
        if(iRet == 0)
        {
            assert(poCmd != NULL);

            UserRoutine_t * w = IdleCoList.top();
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

	return 0;
}

void * clsUserWorker::UserRoutine(void * arg)
{
	UserRoutine_t * pUserRoutine = (UserRoutine_t *)arg;
	co_enable_hook_sys();

	SetRoutineID(pUserRoutine->iRoutineID);

	while(1)
	{
		clsUserWorker * pUserWorker = (clsUserWorker * )pUserRoutine->pSelf;

		if(!pUserRoutine->bHasJob)
		{
			pUserWorker->m_poCoWorkList->push(pUserRoutine);
			co_yield_ct();
			continue;
		}
		pUserRoutine->bHasJob = false;
		if(pUserRoutine->pData == NULL)
		{
			continue;
		}

        pUserWorker->DoWithUserCmd((clsClientCmd *)pUserRoutine->pData);
        pUserRoutine->pData = NULL;
	}

	return NULL;
}

void clsUserWorker::Run()
{
    SetThreadTitle("user_%u", m_iWorkerID);
    CertainLogInfo("user_%u run", m_iWorkerID);

	co_enable_hook_sys();
	stCoEpoll_t * ev = co_get_epoll_ct();

    uint32_t m_iStartRoutineID = 0;
    for (int i = 0; i < 48; ++i)
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
        printf("UserWorker idx %d Routine idx %d\n", m_iWorkerID,  iRoutineID);
        CertainLogError("UserWorker idx %d Routine idx %d", m_iWorkerID,  iRoutineID);
	}

	co_eventloop( ev, CoEpollTick, this);
}

}; // namespace Certain
