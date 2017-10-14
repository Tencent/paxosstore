#pragma once
#include <string>

#include <grpc++/grpc++.h>

#include "example.grpc.pb.h"

class clsClient 
{
public:
    clsClient(std::vector<std::string>* poIPList = NULL);

    clsClient(const std::string& strAddr);

    grpc::Status Call(uint64_t iEntityID, int iOper, 
            const google::protobuf::Message* oRequest, google::protobuf::Message* oResponse,
            const std::string& strAddr = "");

private:
    example::CardServer::Stub* GetMachineA(uint64_t iEntityID);
    example::CardServer::Stub* GetMachineB(uint64_t iEntityID);

    grpc::ChannelArguments m_oArgs;

    std::vector<std::string>* m_poIPList;

    std::vector<std::unique_ptr<example::CardServer::Stub>> m_poStub;
    std::unique_ptr<example::CardServer::Stub> m_poSingleStub;
};
