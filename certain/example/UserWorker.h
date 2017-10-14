#pragma once
#include "Certain.h"
#include "IOWorker.h"
#include "co_routine.h"

class clsCallDataBase
{
public:
    virtual bool ForUserProceed() = 0;
    virtual void Finish(bool bCancelled) = 0;
    virtual void Proceed() = 0;
    virtual ~clsCallDataBase() { }
};

class clsBatchHelper
{
    public:
};

class clsUserWorker : public Certain::clsThreadBase
{
public:
    typedef Certain::clsCircleQueue<clsCallDataBase *> clsUserQueue;

    static uint32_t m_iWorkerNum;
    static clsUserQueue **m_ppUserQueue;

    static void Init(uint32_t iWorkerNum)
    {
        m_iWorkerNum = iWorkerNum;
        m_ppUserQueue = new clsUserQueue * [m_iWorkerNum];
        for (uint32_t i = 0; i < m_iWorkerNum; ++i)
        {
            m_ppUserQueue[i] = new clsUserQueue(10000);
        }
    }

    static int PushCallData(clsCallDataBase *poCallData)
    {
        static volatile uint64_t s_iHint = 0;
        uint32_t iWorkerID = Certain::Hash(s_iHint++) % m_iWorkerNum;

        int iRet = m_ppUserQueue[iWorkerID]->PushByMultiThread(poCallData);
        if (iRet != 0)
        {
            CertainLogError("PushByMultiThread ret %d", iRet);
            return -2;
        }

        return 0;
    }

private:
    uint32_t m_iWorkerID;
    clsUserQueue *m_poUserQueue;

    struct UserRoutine_t
    {
        void * pCo;
        void * pData;
        bool bHasJob;
        int iRoutineID;
        clsUserWorker * pSelf;
    };

    std::stack<UserRoutine_t*> * m_poCoWorkList;

    // For simple, not to excute all the pre cmd.
    Certain::clsLRUTable<uint64_t, vector<clsCallDataBase*>* > *m_poBatchTable;

public:

    clsUserWorker(uint32_t iWorkerID)
    {
        m_iWorkerID = iWorkerID;
        m_poUserQueue = m_ppUserQueue[iWorkerID];

        m_poCoWorkList = new std::stack<UserRoutine_t*>;

        m_poBatchTable = new Certain::clsLRUTable<uint64_t, vector<clsCallDataBase*>* >;
    }

    void Run();

    static int CoEpollTick(void * arg);
    static void * UserRoutine(void * arg);

    void DoWithUserCmd(clsCallDataBase *poCallData);
};
