#ifndef CERTAIN_USERWORKER_H_
#define CERTAIN_USERWORKER_H_

#include "Certain.h"
#include "IOWorker.h"
#include "SimpleCmd.h"
#include "co_routine.h"

namespace Certain
{

class clsUserWorker : public clsThreadBase
{
public:
    typedef clsCircleQueue<clsClientCmd *> clsUserQueue;

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

	static int PushUserCmd(clsClientCmd *poCmd)
    {
        uint32_t iWorkerID = Hash(poCmd->GetEntityID()) % m_iWorkerNum;

        int iRet = m_ppUserQueue[iWorkerID]->PushByMultiThread(poCmd);
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
    clsLRUTable<uint64_t, vector<clsClientCmd*>* > *m_poBatchTable;

public:

    clsUserWorker(uint32_t iWorkerID)
    {
        m_iWorkerID = iWorkerID;
        m_poUserQueue = m_ppUserQueue[iWorkerID];

        m_poCoWorkList = new std::stack<UserRoutine_t*>;

        m_poBatchTable = new clsLRUTable<uint64_t, vector<clsClientCmd*>* >;
    }

    void AddCmdBatch(clsClientCmd *poCmd)
    {
        uint64_t iEntityID = poCmd->GetEntityID();
        vector<clsClientCmd*>* pvec = NULL;

        if (!m_poBatchTable->Find(iEntityID, pvec))
        {
            pvec = new vector<clsClientCmd*>;
        }

        pvec->push_back(poCmd);
    }

    void TakeCmdBatch(uint64_t &iEntityID, vector<clsClientCmd*>* &pvec)
    {
        iEntityID = 0;
        pvec = NULL;

        if (m_poBatchTable->PeekOldest(iEntityID, pvec))
        {
            assert(m_poBatchTable->Remove(iEntityID));
        }
    }

	void Run();

	static int CoEpollTick(void * arg);
	static void * UserRoutine(void * arg);

    void DoWithUserCmd(clsClientCmd *poCmd);
};

}; // namespace Certain

#endif // CERTAIN_USERWORKER_H_
