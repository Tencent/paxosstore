#ifndef CERTAIN_DBWORKER_H_
#define CERTAIN_DBWORKER_H_

#include "Certain.h"
#include "IOWorker.h"

namespace Certain
{

class clsDBWorker : public clsThreadBase
{
private:
    uint32_t m_iWorkerID;
    clsConfigure *m_poConf;

    clsIOWorkerRouter *m_poIOWorkerRouter;
    clsDBReqQueue *m_poDBReqQueue;
    clsCertainWrapper *m_poCertain;

    struct DBRoutine_t
    {
        void * pCo;
        void * pData;
        bool bHasJob;
        int iRoutineID;
        clsDBWorker * pSelf;
    };

    clsDBBase *m_poDBEngine;
    stack<DBRoutine_t*> * m_poCoWorkList;

    set<uint64_t> m_tBusyEntitySet;

    uint32_t m_iStartRoutineID;

    void RunApplyTask(clsClientCmd *poCmd, uint64_t &iPLogGetCnt);

public:
    clsDBWorker(uint32_t iWorkerID, clsConfigure *poConf,
            clsDBBase *poDBEngine, uint32_t iStartRoutineID)
        : m_iWorkerID(iWorkerID),
        m_poConf(poConf),
        m_poDBEngine(poDBEngine),
        m_iStartRoutineID(iStartRoutineID)
    {
        m_poIOWorkerRouter = clsIOWorkerRouter::GetInstance();
        m_poDBReqQueue = clsAsyncQueueMng::GetInstance()->GetDBReqQueue(m_iWorkerID);
        m_poCertain = clsCertainWrapper::GetInstance();

        m_poCoWorkList = new stack<DBRoutine_t*>;
    }

    virtual ~clsDBWorker()
    {
    }

    void Run();

    clsConfigure * GetConfig()
    {
        return m_poConf;
    }

    static int EnterDBReqQueue(clsClientCmd *poCmd);

    static int CoEpollTick(void * arg);
    static int DBSingle(void * arg);
    static void * DBRoutine(void * arg);
    static int NotifyDBWorker(uint64_t iEntityID);
};

} // namespace Certain

#endif
