#pragma once
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "example.grpc.pb.h"
#include "example/DBImpl.h"
#include "example/Coding.h"

#include "example/GrpcHelper.h"

#include "src/EntityInfoMng.h"

using namespace Certain;

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
};

DEFINE_CALL_DATA(example, CardServer, Echo, EchoRequest, EchoResponse);
DEFINE_CALL_DATA(example, CardServer, InsertCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, UpdateCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, DeleteCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, SelectCard, CardRequest, CardResponse);
DEFINE_CALL_DATA(example, CardServer, GetDBEntityMeta, GetRequest, GetResponse);
DEFINE_CALL_DATA(example, CardServer, GetAllForCertain, GetRequest, GetResponse);
DEFINE_CALL_DATA(example, CardServer, RecoverData, GetRequest, GetResponse);

