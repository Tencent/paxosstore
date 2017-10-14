#include "example/Client.h"

bool IsValidStatusCode(int iCode) 
{
    return (iCode == 0 || iCode == example::StatusCode::eCardBalanceNotEnough ||
            iCode == example::StatusCode::eCardNotExist ||
            iCode == example::StatusCode::eCardExist);
}

bool IsCardOper(int iOper)
{
    return (iOper == example::OperCode::eInsertCard || iOper == example::OperCode::eUpdateCard || 
            iOper == example::OperCode::eDeleteCard || iOper == example::OperCode::eSelectCard);
}

bool IsGetOper(int iOper)
{
    return (iOper == example::OperCode::eGetDBEntityMeta || 
            iOper == example::OperCode::eGetAllForCertain || 
            iOper == example::OperCode::eRecoverData);
}

clsClient::clsClient(std::vector<std::string>* poIPList)
{
    grpc_init();

    m_oArgs.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 20);

    m_poIPList = poIPList;
    if (m_poIPList == NULL) return;

    for (size_t i = 0; i < m_poIPList->size(); ++i)
    {
        std::shared_ptr<grpc::Channel> poChannel = grpc::CreateCustomChannel(
                m_poIPList->at(i), grpc::InsecureChannelCredentials(), m_oArgs);
        m_poStub.push_back(example::CardServer::NewStub(poChannel));
    }
}

clsClient::clsClient(const std::string& strAddr)
{
    grpc_init();

    m_oArgs.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 20);

    std::shared_ptr<grpc::Channel> poChannel = grpc::CreateCustomChannel(
            strAddr, grpc::InsecureChannelCredentials(), m_oArgs);
    m_poSingleStub = example::CardServer::NewStub(poChannel);
}

grpc::Status clsClient::Call(uint64_t iEntityID, int iOper, 
        const google::protobuf::Message* oRequest, google::protobuf::Message* oResponse, 
        const std::string& strAddr)
{
    const example::CardRequest* oCardRequest = NULL;
    example::CardResponse* oCardResponse = NULL;

    const example::GetRequest* oGetRequest = NULL;
    example::GetResponse* oGetResponse = NULL;

    if (IsCardOper(iOper))
    {
        oCardRequest = dynamic_cast<const example::CardRequest*>(oRequest);
        oCardResponse = dynamic_cast<example::CardResponse*>(oResponse);
    }
    else if (IsGetOper(iOper))
    {
        oGetRequest = dynamic_cast<const example::GetRequest*>(oRequest);
        oGetResponse = dynamic_cast<example::GetResponse*>(oResponse);
    }
    else 
    {
        assert(false);
    }

    example::CardServer::Stub* poStub = strAddr.empty() ? GetMachineA(iEntityID) : m_poSingleStub.get();
    assert(poStub != NULL);

    grpc::Status oStatus;

    static int kRetry = 2;
    for (int i = 0; i < kRetry; ++i)
    {
        grpc::ClientContext oCtx;

        switch (iOper)
        {
            case example::OperCode::eInsertCard:
                oStatus = poStub->InsertCard(&oCtx, *oCardRequest, oCardResponse);
                break;
            case example::OperCode::eUpdateCard:
                oStatus = poStub->UpdateCard(&oCtx, *oCardRequest, oCardResponse);
                break;
            case example::OperCode::eDeleteCard:
                oStatus = poStub->DeleteCard(&oCtx, *oCardRequest, oCardResponse);
                break;
            case example::OperCode::eSelectCard:
                oStatus = poStub->SelectCard(&oCtx, *oCardRequest, oCardResponse);
                break;
            case example::OperCode::eGetDBEntityMeta:
                oStatus = poStub->GetDBEntityMeta(&oCtx, *oGetRequest, oGetResponse);
                break;
            case example::OperCode::eGetAllForCertain:
                oStatus = poStub->GetAllForCertain(&oCtx, *oGetRequest, oGetResponse);
                break;
            case example::OperCode::eRecoverData:
                oStatus = poStub->RecoverData(&oCtx, *oGetRequest, oGetResponse);
                break;
            default:
                break;
        }

        if ((!strAddr.empty()) || (IsValidStatusCode(oStatus.error_code()))) return oStatus;

        poStub = GetMachineB(iEntityID);
    }

    return oStatus;
}

example::CardServer::Stub* clsClient::GetMachineA(uint64_t iEntityID)
{
    assert(m_poIPList != NULL && !m_poIPList->empty());
    return m_poStub[iEntityID % m_poIPList->size()].get();
}

example::CardServer::Stub* clsClient::GetMachineB(uint64_t iEntityID)
{
    assert(m_poIPList != NULL && !m_poIPList->empty());
    return m_poStub[(iEntityID + 1) % m_poIPList->size()].get();
}

