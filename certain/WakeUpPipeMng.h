#ifndef CERTAIN_WAKEUPPIPEMNG_H_
#define CERTAIN_WAKEUPPIPEMNG_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsWakeUpPipeMng : public clsSingleton<clsWakeUpPipeMng>
{
private:
    typedef pair<int, int> Pipe_t;
    vector< Pipe_t > vecPipe;

    clsMutex m_oMutex;

public:
    clsWakeUpPipeMng() { }

    void NewPipe(int &iInFD)
    {
        iInFD = 0;
        int iOutFD = 0;

        AssertEqual(MakeNonBlockPipe(iInFD, iOutFD), 0);

        Pipe_t tPipe;
        tPipe.first = iInFD;
        tPipe.second = iOutFD;

        printf("NewPipe iInFD %d\n", iInFD);

        clsThreadLock oLock(&m_oMutex);
        vecPipe.push_back(tPipe);
    }

    void WakeupAll()
    {
        for (uint32_t i = 0; i < vecPipe.size(); ++i)
        {
            int iOutFD = vecPipe[i].second;

            write(iOutFD, "x", 1);
        }
    }
};

} // namespace Certain

#endif
