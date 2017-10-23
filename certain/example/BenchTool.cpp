#include <atomic>
#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "example.grpc.pb.h"
#include "utils/Thread.h"
#include "utils/Time.h"
#include "utils/UseTimeStat.h"

Certain::clsUseTimeStat s_oStat("echo");
std::atomic_int s_iStopCnt(0);
std::atomic<uint64_t> s_iHelloIdx(0);

class clsStressTester : public Certain::clsThreadBase
{
public:
    clsStressTester(std::shared_ptr<grpc::Channel> poChannel, uint64_t iCount)
    {
        m_iCount = iCount;
        m_poStub = example::CardServer::NewStub(poChannel);
    }

    void Run()
    {
        for (uint32_t i = 0; i < m_iCount; ++i)
        {
            uint64_t iStartUS = Certain::GetCurrTimeUS();

            grpc::ClientContext oCtx;
            example::EchoRequest oRequest;
            example::EchoResponse oResponse;

            char sBuffer[64];
            sprintf(sBuffer, "example_%lu", s_iHelloIdx.fetch_add(1));
            oRequest.set_value(sBuffer);

            grpc::Status oStatus = m_poStub->Echo(&oCtx, oRequest, &oResponse);

            uint64_t iEndUS = Certain::GetCurrTimeUS();

            s_oStat.Update(iEndUS - iStartUS);

            if (oStatus.error_code() != 0)
            {
                cout << " status " << oStatus.error_code() << endl;
            }
        }

        s_iStopCnt++;
    }

private:
    uint64_t m_iCount;
    std::unique_ptr<example::CardServer::Stub> m_poStub;
};

int main(int argc, char** argv)
{
    if (argc != 5)
    {
        printf("%s sAddr iChannelNum iWorkerNumPerChannel CountPerWorker\n",
                argv[0]);
        exit(-1);
    }

    grpc_init();

    std::string strAddr = argv[1];
    int iChannelNum = atoi(argv[2]);
    int iWorkerNumPerChannel = atoi(argv[3]);
    int iCountPerWorker = atoi(argv[4]);

    grpc::ChannelArguments oArgs;
    oArgs.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 20);

    for (int i = 0; i < iChannelNum; ++i)
    {
        std::shared_ptr<grpc::Channel> poChannel = grpc::CreateCustomChannel(
                strAddr, grpc::InsecureChannelCredentials(), oArgs);

        for (int j = 0; j < iWorkerNumPerChannel; ++j)
        {
            clsStressTester *poTester = new clsStressTester(
                    poChannel, iCountPerWorker);
            poTester->Start();
        }
    }

    while (s_iStopCnt.load() == 0)
    {
        sleep(1);
        s_oStat.Print();
    }

    return 0;
}
