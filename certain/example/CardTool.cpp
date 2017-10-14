#include <string>
#include <unistd.h>

#include <grpc++/grpc++.h>

#include "example.grpc.pb.h"
#include "example/Client.h"
#include "example/UUIDGenerator.h"

using namespace std;

void PrintUsage()
{
    printf("./CardTool -X/-Y/-Z addr -o Insert -i <card_id> -n <user_name> -u <user_id> -b <balance>\n");
    printf("./CardTool -X/-Y/-Z addr -o Update -i <card_id> -d <delta>\n");
    printf("./CardTool -X/-Y/-Z addr -o Delete -i <card_id>\n");
    printf("./CardTool -X/-Y/-Z addr -o Select -i <card_id>\n");
    printf("./CardTool -a       addr -o Recover -i <card_id>\n");
}

grpc::Status Run(const string& strAddr, const string& strOper, 
        example::CardRequest& oRequest, example::CardResponse& oResponse,
        std::vector<std::string>* poIPList) 
{
    clsClient oClient(poIPList);
    
    clsClient oGetClient(strAddr);
    example::GetRequest oGetRequest;
    example::GetResponse oGetResponse;

    grpc::Status oStatus;

    switch (strOper[0])
    {
        case 'I':
            oRequest.mutable_card_info()->set_last_modified_time(time(0));
            oStatus = oClient.Call(oRequest.card_id(), example::eInsertCard, &oRequest, &oResponse);
            break;
        case 'U':
            oRequest.mutable_card_info()->set_last_modified_time(time(0));
            oStatus = oClient.Call(oRequest.card_id(), example::eUpdateCard, &oRequest, &oResponse);
            if (oStatus.error_code() == 0)
            {
                printf("balance=%lu\n", oResponse.card_info().balance());
            }
            break;
        case 'D':
            oStatus = oClient.Call(oRequest.card_id(), example::eDeleteCard, &oRequest, &oResponse);
            break;
        case 'S':
            oStatus = oClient.Call(oRequest.card_id(), example::eSelectCard, &oRequest, &oResponse);
            if (oStatus.error_code() == 0)
            {
                printf("user_name=%s user_id=%lu balance=%lu\n", 
                        oResponse.card_info().user_name().c_str(), oResponse.card_info().user_id(), 
                        oResponse.card_info().balance());
            }
            break;
        case 'R':
            oGetRequest.set_entity_id(oRequest.card_id());
            oStatus = oGetClient.Call(oGetRequest.entity_id(), example::eRecoverData, 
                    &oGetRequest, &oGetResponse, strAddr);
            break;
        default:
            PrintUsage();
            exit(-1);
    }

    return oStatus;
}

int main(int argc, char** argv)
{
    if (argc < 5)
    {
        PrintUsage();
        exit(-1);
    }

    string strOper;
    string strAddr;

    example::CardRequest oRequest;
    example::CardResponse oResponse;

    std::vector<std::string> vecIPList;

    clsUUIDGenerator::GetInstance()->Init();

    int iOpt;
    while ((iOpt = getopt(argc, argv, "a:o:i:n:u:b:d:X:Y:Z:")) != EOF)
    {
        switch (iOpt)
        {
            case 'a':
                strAddr = optarg;
                break;
            case 'o':
                strOper = optarg;
                break;
            case 'i':
                oRequest.set_card_id(strtoul(optarg, NULL, 10));
                break;
            case 'n':
                oRequest.mutable_card_info()->set_user_name(optarg);
                break;
            case 'u':
                oRequest.mutable_card_info()->set_user_id(strtoul(optarg, NULL, 10));
                break;
            case 'b':
                oRequest.mutable_card_info()->set_balance(strtoul(optarg, NULL, 10));
                break;
            case 'd':
                oRequest.set_delta(strtol(optarg, NULL, 10));
                break;
            case 'X':
                vecIPList.push_back(optarg);
                break;
            case 'Y':
                vecIPList.push_back(optarg);
                break;
            case 'Z':
                vecIPList.push_back(optarg);
                break;
            default:
                PrintUsage();
                exit(-1);
        }
    }

    oRequest.set_uuid(clsUUIDGenerator::GetInstance()->GetUUID());

    map<int, string> tErrMsgMap;
    tErrMsgMap[example::StatusCode::eCardBalanceNotEnough] = "card balance not enough";
    tErrMsgMap[example::StatusCode::eCardNotExist] = "card not exist";
    tErrMsgMap[example::StatusCode::eCardExist] = "card exists";

    grpc::Status oRet = Run(strAddr, strOper, oRequest, oResponse, &vecIPList);
    if (oRet.ok()) 
    {
        printf("Done\n");
    }
    else
    {
        int iCode = oRet.error_code();
        string strMsg = oRet.error_message();
        if (tErrMsgMap.find(iCode)!= tErrMsgMap.end())
        {
            strMsg = tErrMsgMap[oRet.error_code()];
        }
        printf("Failure with error: code(%d) msg(%s)\n", iCode, strMsg.c_str());
    }

    return 0;
}
