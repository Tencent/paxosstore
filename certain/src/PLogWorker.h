#ifndef CERTAIN_PLogWORKER_H_
#define CERTAIN_PLogWORKER_H_

#include "certain/Certain.h"
#include "Configure.h"
#include "AsyncQueueMng.h"
#include "IOWorker.h"
#include "WakeUpPipeMng.h"

namespace Certain
{

class clsPLogWorker : public clsThreadBase
{
private:
    uint32_t m_iWorkerID;
    clsConfigure *m_poConf;

    clsIOWorkerRouter *m_poIOWorkerRouter;

    clsPLogReqQueue *m_poPLogReqQueue;

    clsPLogBase *m_poPLogEngine;
    clsDBBase *m_poDBEngine;

    struct PLogRoutine_t
    {
        void * pCo;
        void * pData;
        bool bHasJob;
        int iRoutineID;
        clsPLogWorker * pSelf;
    };

    stack<PLogRoutine_t*> *m_poCoWorkList;

    uint32_t m_iStartRoutineID;
    int m_iNotifyFd;

    int DoWithPLogRequest(clsPaxosCmd *poPaxosCmd);
    int LoadEntry(clsPaxosCmd *poPaxosCmd);

    int FillRecoverCmd(clsRecoverCmd *poRecoverCmd);

    void DoWithRecoverCmd(clsRecoverCmd *poRecoverCmd);
    void DoWithPaxosCmd(clsPaxosCmd *poPaxosCmd);
    void SendToWriteWorker(clsPaxosCmd *poPaxosCmd);

public:
    clsPLogWorker(uint32_t iWorkerID, clsConfigure *poConf,
            clsPLogBase *poPLogEngine, uint32_t iStartRoutineID) : m_iWorkerID(iWorkerID),
    m_poConf(poConf),
    m_poPLogEngine(poPLogEngine),
    m_iStartRoutineID(iStartRoutineID)
    {
        clsAsyncQueueMng *poQueueMng = clsAsyncQueueMng::GetInstance();
        m_poPLogReqQueue = poQueueMng->GetPLogReqQueue(m_iWorkerID);

        m_poIOWorkerRouter = clsIOWorkerRouter::GetInstance();
        m_poDBEngine = clsCertainWrapper::GetInstance()->GetDBEngine();

        m_poCoWorkList = new stack<PLogRoutine_t*>;

        //clsWakeUpPipeMng::GetInstance()->NewPipe(m_iNotifyFd);
    }

    virtual ~clsPLogWorker()
    {
        // delete m_poIOWorkerRouter, m_poIOWorkerRouter = NULL;
    }

    void Run();

    static int EnterPLogReqQueue(clsCmdBase *poCmd);
    static void *PLogRoutine(void * arg);
    static int EnterPLogRspQueue(clsCmdBase *poCmd);

    static int CoEpollTick(void * arg);

    static void *WakeUpRoutine(void * arg);
};

} // namespace Certain

#endif
