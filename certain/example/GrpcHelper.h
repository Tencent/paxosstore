#pragma once
#include <memory>
#include <iostream>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "utils/Thread.h"
#include "utils/Singleton.h"
#include "utils/Hash.h"

#include "UserWorker.h"

class clsServiceImpl;

#define DEFINE_CALL_DATA(PKG, SERV, ITF, REQ, RESP) \
class cls_##PKG##SERV##ITF : public clsCallDataBase \
{ \
public: \
    cls_##PKG##SERV##ITF( \
            PKG::SERV::AsyncService *poSERV, \
            grpc::ServerCompletionQueue *poQueue, \
            clsServiceImpl *poMyService) \
        : m_enumCallStatus(CREATE), \
          m_poSERV(poSERV), \
          m_poQueue(poQueue), \
          m_oResponder(&m_oCtx), \
          m_poMyService(poMyService) \
    { } \
    virtual ~cls_##PKG##SERV##ITF() { } \
    void Proceed() \
    { \
        clsCallDataBase *poBase = dynamic_cast<clsCallDataBase *>(this); \
        assert(poBase != NULL); \
        if (m_enumCallStatus == CREATE) \
        { \
            m_enumCallStatus = PROCESS; \
            m_poSERV->Request##ITF(&m_oCtx, &m_oRequest, &m_oResponder, \
                    m_poQueue, m_poQueue, poBase); \
        } \
        else if (m_enumCallStatus == PROCESS) \
        { \
            int iRet = m_poMyService->ITF(m_oCtx, m_oRequest, m_oResponse); \
            m_enumCallStatus = FINISH; \
            m_oStatus = grpc::Status(grpc::StatusCode(abs(iRet)), "service_retcode"); \
        } \
        else \
        { \
            auto vecCallData = NewCallDataForQueue(1, m_poSERV, m_poQueue, m_poMyService); \
            for (uint32_t i = 0; i < vecCallData.size(); ++i) \
            { \
                vecCallData[i]->Proceed(); \
            } \
            delete this; \
        } \
    } \
    void Finish(bool bCancelled) \
    { \
        if (bCancelled) \
        { \
            m_oStatus = grpc::Status::CANCELLED; \
            m_enumCallStatus = FINISH; \
            return; \
        } \
        assert(m_enumCallStatus == FINISH); \
        clsCallDataBase *poBase = dynamic_cast<clsCallDataBase *>(this); \
        m_oResponder.Finish(m_oResponse, m_oStatus, poBase); \
    } \
    bool ForUserProceed() { return m_enumCallStatus == PROCESS; } \
    static vector<clsCallDataBase *> NewCallDataForQueue(uint32_t iNum, \
            PKG::SERV::AsyncService *poSERV, \
            grpc::ServerCompletionQueue *poQueue, \
            clsServiceImpl *poMyService) \
    { \
        vector<clsCallDataBase *> vecCallData; \
        for (uint32_t i = 0; i < iNum; ++i) \
        { \
            vecCallData.push_back(new cls_##PKG##SERV##ITF( \
                        poSERV, poQueue, poMyService)); \
        } \
        return vecCallData; \
    } \
private: \
    enum enumCallStatus { CREATE, PROCESS, FINISH }; \
    enumCallStatus m_enumCallStatus; \
    PKG::SERV::AsyncService *m_poSERV; \
    grpc::ServerCompletionQueue *m_poQueue; \
    grpc::ServerContext m_oCtx; \
    PKG::REQ m_oRequest; \
    PKG::RESP m_oResponse; \
    grpc::ServerAsyncResponseWriter<PKG::RESP> m_oResponder; \
    grpc::Status m_oStatus; \
    clsServiceImpl *m_poMyService; \
};

class clsServerWorker : public Certain::clsThreadBase
{
public:
    clsServerWorker(string strAddr)
    {
        m_strAddr = strAddr;
        m_oBuilder.AddListeningPort(
                m_strAddr, grpc::InsecureServerCredentials());
        m_poQueue = m_oBuilder.AddCompletionQueue();
        m_poService = NULL;
    }

    virtual ~clsServerWorker()
    {
        m_poServer->Shutdown();
        m_poQueue->Shutdown();
    }

    void Register(grpc::Service *poService)
    {
        if (m_poService == NULL)
        {
            m_oBuilder.RegisterService(poService);
            m_poService = poService;
        }
        else
        {
            delete poService, poService = NULL;
        }
    }

    grpc::Service *GetService()
    {
        assert(m_poService != NULL);
        return m_poService;
    }

    void BuildAndStart()
    {
        m_poServer = m_oBuilder.BuildAndStart();
    }

    void Run()
    {
        cout << "Server listening on " << m_strAddr << endl;
        void* tag = NULL;

        while (true)
        {
            bool ok = false;
            assert(m_poQueue->Next(&tag, &ok));

            clsCallDataBase *poBase = static_cast<clsCallDataBase *>(tag);
            if (!ok)
            {
                poBase->Finish(true);
                poBase->Proceed();
                continue;
            }

            if (!poBase->ForUserProceed())
            {
                poBase->Proceed();
                continue;
            }

            while (clsUserWorker::PushCallData(poBase) != 0)
            {
                usleep(1000);
            }
        }
    }

    grpc::ServerCompletionQueue *GetQueue()
    {
        return m_poQueue.get();
    }

private:
    string m_strAddr;
    grpc::ServerBuilder m_oBuilder;
    unique_ptr<grpc::ServerCompletionQueue> m_poQueue;
    unique_ptr<grpc::Server> m_poServer;
    grpc::Service *m_poService;
};

class clsServerWorkerMng : public Certain::clsSingleton<clsServerWorkerMng>
{
private:
    vector<clsCallDataBase *> m_vecCallData;
    vector<clsServerWorker *> m_vecWorker;
    int m_iCallDataNum;
    clsServiceImpl *m_poMyService;

public:
    int Init(string strAddr, int iWorkerNum, int iCallDataNum, clsServiceImpl *poMyService)
    {
        m_vecCallData.clear();
        m_vecWorker.clear();
        m_iCallDataNum = iCallDataNum;
        m_poMyService = poMyService;

        for (int i = 0; i < iWorkerNum; ++i)
        {
            clsServerWorker *poServer = new clsServerWorker(strAddr);
            m_vecWorker.push_back(poServer);
        }
        return 0;
    }

    vector<clsServerWorker *> GetWorkers()
    {
        return m_vecWorker;
    }

    int GetCallDataNum()
    {
        return m_iCallDataNum;
    }

    clsServiceImpl *GetMyService()
    {
        return m_poMyService;
    }

    void AddCallData(vector<clsCallDataBase *> vecCallData)
    {
        for (uint32_t i = 0; i < vecCallData.size(); ++i)
        {
            m_vecCallData.push_back(vecCallData[i]);
        }
    }

    void RunAll()
    {
        for (uint32_t i = 0; i < m_vecWorker.size(); ++i)
        {
            m_vecWorker[i]->BuildAndStart(); // Must before Proceed.
        }

        for (uint32_t i = 0; i < m_vecCallData.size(); ++i)
        {
            m_vecCallData[i]->Proceed();
        }

        for (uint32_t i = 0; i < m_vecWorker.size(); ++i)
        {
            m_vecWorker[i]->Start();
        }
    }
};

#define REGISTER_INTERFACE(PKG, SERV, ITF) \
    do { \
        clsServerWorkerMng *poMng = clsServerWorkerMng::GetInstance(); \
        vector<clsServerWorker *> vec = poMng->GetWorkers(); \
        clsServiceImpl *poMyService = dynamic_cast<clsServiceImpl *>(poMng->GetMyService()); \
        int iCallDataNum = poMng->GetCallDataNum(); \
        for (uint32_t i = 0; i < vec.size(); ++i) { \
            PKG::SERV::AsyncService *po = new PKG::SERV::AsyncService; \
            vec[i]->Register(po); \
            vector<clsCallDataBase *> vecCallData = \
            cls_##PKG##SERV##ITF::NewCallDataForQueue( \
                    iCallDataNum, dynamic_cast<PKG::SERV::AsyncService *>( \
                        vec[i]->GetService()), vec[i]->GetQueue(), poMyService); \
            poMng->AddCallData(vecCallData); \
        } \
    } while(0);

