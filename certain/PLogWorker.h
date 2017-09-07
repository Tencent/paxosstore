#ifndef CERTAIN_PLogWORKER_H_
#define CERTAIN_PLogWORKER_H_

#include "Certain.h"
#include "Configure.h"
#include "AsyncQueueMng.h"
#include "IOWorker.h"
#include "PerfLog.h"
#include "WakeUpPipeMng.h"

namespace Certain
{

class clsUseTimeStat
{
private:
    clsConfigure *m_poConf;
    string m_strTag;
    volatile uint64_t m_iMaxUseTimeUS;
    volatile uint64_t m_iTotalUseTimeUS;
    volatile uint64_t m_iCnt;

public:

    clsUseTimeStat(string strTag)
    {
        m_poConf = clsCertainWrapper::GetInstance()->GetConf();
        assert(m_poConf != NULL);
        m_strTag = strTag;
        Reset();
    }

    ~clsUseTimeStat() { }

    void Reset()
    {
        m_iMaxUseTimeUS = 0;
        m_iTotalUseTimeUS = 0;
        m_iCnt = 0;
    }

    void Update(uint64_t iUseTimeUS)
    {
        //if (m_poConf->GetEnableTimeStat() == 0)
        //{
        //    return;
        //}

        if (m_iMaxUseTimeUS < iUseTimeUS)
        {
            m_iMaxUseTimeUS = iUseTimeUS;
        }

        __sync_fetch_and_add(&m_iTotalUseTimeUS, iUseTimeUS);
        __sync_fetch_and_add(&m_iCnt, 1);
    }

    void Print()
    {
        uint64_t iTotalUseTimeUS = __sync_fetch_and_and(&m_iTotalUseTimeUS, 0);
        uint64_t iCnt = __sync_fetch_and_and(&m_iCnt, 0);
        uint64_t iMaxUseTimeUS = __sync_fetch_and_and(&m_iMaxUseTimeUS, 0);

        if (iCnt == 0)
        {
            CertainLogZero("certain_stat %s cnt 0", m_strTag.c_str());
        }
        else
        {
            CertainLogZero("certain_stat %s max_us %lu avg_us %lu cnt %lu",
                    m_strTag.c_str(), iMaxUseTimeUS, iTotalUseTimeUS / iCnt, iCnt);
        }
    }
};

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
