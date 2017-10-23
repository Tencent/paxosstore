#include <atomic>
#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "example.grpc.pb.h"
#include "example/Client.h"
#include "example/Coding.h"
#include "example/UUIDGenerator.h"
#include "utils/Thread.h"
#include "utils/Time.h"
#include "utils/UseTimeStat.h"

Certain::clsUseTimeStat s_oStat("RW");
std::atomic_int s_iStopCnt(0);
std::atomic<uint64_t> s_iHelloIdx(0);
std::atomic<uint64_t> s_iFailCnt(0);

static const std::vector<int> kOperList = {
    example::eInsertCard, example::eSelectCard, example::eUpdateCard,
    example::eSelectCard, example::eDeleteCard};

class clsRWStressTester : public Certain::clsThreadBase
{
public:
    clsRWStressTester(std::shared_ptr<clsClient> poClient, uint64_t iCount, uint64_t iEntityNum)
    {
        m_iCount = iCount;
        m_poClient = poClient;
        m_iEntityNum = iEntityNum;
    }

    void Run()
    {
        for (uint32_t i = 0; i < m_iCount; ++i)
        {
            example::CardRequest oRequest;
            example::CardResponse oResponse;

            oRequest.set_card_id(s_iHelloIdx.fetch_add(1));
            oRequest.set_entity_id(GetEntityID(oRequest.card_id(), m_iEntityNum));
            oRequest.mutable_card_info()->set_balance(100);
            oRequest.set_delta(20);

            for (size_t k = 0; k < kOperList.size(); ++k)
            {
                uint64_t iStartUS = Certain::GetCurrTimeUS();

                oRequest.mutable_card_info()->set_last_modified_time(time(0)); 
                oRequest.set_uuid(clsUUIDGenerator::GetInstance()->GetUUID());
                grpc::Status oStatus = m_poClient->Call(oRequest.entity_id(), kOperList[k], &oRequest, &oResponse);

                uint64_t iEndUS = Certain::GetCurrTimeUS();

                s_oStat.Update(iEndUS - iStartUS);

                if (oStatus.error_code() != 0)
                {
                    s_iFailCnt.fetch_add(1);
                    printf("card_id %lu idx %lu oper %d ret %d\n", 
                            oRequest.card_id(), k, kOperList[k], oStatus.error_code());
                    break;
                }
            }
        }

        s_iStopCnt++;
    }

private:
    uint64_t m_iCount;
    uint64_t m_iEntityNum;
    std::shared_ptr<clsClient> m_poClient;
};

int main(int argc, char** argv)
{
    if (argc != 8)
    {
        printf("%s sAddr1 sAddr2 sAddr3 iChannelNum iWorkerNumPerChannel iCountPerWorker iEntityNum\n",
                argv[0]);
        exit(-1);
    }

    std::vector<std::string> vecIPList;
    for (int i = 1; i <= 3; ++i)
        vecIPList.push_back(argv[i]);

    int iChannelNum = atoi(argv[4]);
    int iWorkerNumPerChannel = atoi(argv[5]);
    int iCountPerWorker = atoi(argv[6]);
    uint64_t iEntityNum = atoi(argv[7]);

    clsUUIDGenerator::GetInstance()->Init();

    for (int i = 0; i < iChannelNum; ++i)
    {
        std::shared_ptr<clsClient> poClient(new clsClient(&vecIPList));
        for (int j = 0; j < iWorkerNumPerChannel; ++j)
        {
            clsRWStressTester* poTester = new clsRWStressTester(poClient, iCountPerWorker, iEntityNum);
            poTester->Start();
        }
    }

    while (s_iStopCnt.load() == 0)
    {
        sleep(1);
        s_oStat.Print();
    }

    printf("Fail %lu\n", s_iFailCnt.load());

    return 0;
}
