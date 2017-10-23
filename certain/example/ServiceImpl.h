#pragma once
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "example.grpc.pb.h"
#include "example/Coding.h"
#include "example/CoHashLock.h"
#include "example/DBImpl.h"
#include "example/GrpcHelper.h"
#include "example/TemporaryTable.h"

struct QueueItem_t
{
    int iOper;
    uint64_t iEntityID;
    void *poRequest;
    void *poResponse;
    volatile int iRet;
};

enum BatchStatus
{
    WAITING = -9001,
};

class clsServiceImpl
{
public:
    int Echo(grpc::ServerContext& oContext,
            const example::EchoRequest& oRequest,
            example::EchoResponse& oResponse);

    // Write command
    int InsertCard(grpc::ServerContext& oContext,
            const example::CardRequest& oRequest,
            example::CardResponse& oResponse);

    int UpdateCard(grpc::ServerContext& oContext,
            const example::CardRequest& oRequest,
            example::CardResponse& oResponse);

    int DeleteCard(grpc::ServerContext& oContext,
            const example::CardRequest& oRequest,
            example::CardResponse& oResponse);

    // Read command
    int SelectCard(grpc::ServerContext& oContext,
            const example::CardRequest& oRequest,
            example::CardResponse& oResponse);

    // GetAllAndSet
    int GetDBEntityMeta(grpc::ServerContext& oContext,
            const ::example::GetRequest& oRequest,
            example::GetResponse& oResponse);

    int GetAllForCertain(grpc::ServerContext& oContext,
            const example::GetRequest& oRequest,
            example::GetResponse& oResponse);

    int RecoverData(grpc::ServerContext& oContext,
            const example::GetRequest& oRequest,
            example::GetResponse& oResponse);

private:
    int BatchFunc(int iOper,
            const example::CardRequest& oRequest,
            example::CardResponse& oResponse);

    void BatchReturn(std::queue<QueueItem_t*>* poQueue, int iRet);

    void HandleSingleCommand(clsTemporaryTable* poTable, QueueItem_t* poItem,
            std::vector<uint64_t>* poVecUUID);

    Certain::clsMutex m_poBatchMapMutex;
    // iEntityID -> queue<QueueItem>
    std::unordered_map<uint64_t, std::queue<QueueItem_t*>> m_oBatchMap;
};

DEFINE_CALL_DATA(example, CardServer, Echo, EchoRequest, EchoResponse);
DEFINE_CALL_DATA(example, CardServer, InsertCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, UpdateCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, DeleteCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, SelectCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, GetDBEntityMeta, GetRequest, GetResponse);
DEFINE_CALL_DATA(example, CardServer, GetAllForCertain, GetRequest, GetResponse);
DEFINE_CALL_DATA(example, CardServer, RecoverData, GetRequest, GetResponse);

