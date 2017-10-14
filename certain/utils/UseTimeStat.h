#ifndef CERTAIN_UTILS_USETIMESTAT_H_
#define CERTAIN_UTILS_USETIMESTAT_H_

#include "utils/Logger.h"
#include "utils/Time.h"

#define TIMERUS_START(x) uint64_t x = Certain::GetCurrTimeUS()
#define TIMERUS_STOP(x) x = Certain::GetCurrTimeUS() - x

#define TIMERMS_START(x) uint64_t x = Certain::GetCurrTimeMS()
#define TIMERMS_STOP(x) x = Certain::GetCurrTimeMS() - x

namespace Certain
{

class clsUseTimeStat
{
private:
    string m_strTag;
    volatile uint64_t m_iMaxUseTimeUS;
    volatile uint64_t m_iTotalUseTimeUS;
    volatile uint64_t m_iCnt;

public:

    clsUseTimeStat(string strTag)
    {
        m_strTag = strTag;
        Reset();
    }

    ~clsUseTimeStat() { }

    void Reset()
    {
        m_iMaxUseTimeUS = 0;
        m_iTotalUseTimeUS = 0;
        m_iCnt = 0;
    }

    void Update(uint64_t iUseTimeUS)
    {
        if (m_iMaxUseTimeUS < iUseTimeUS)
        {
            m_iMaxUseTimeUS = iUseTimeUS;
        }

        __sync_fetch_and_add(&m_iTotalUseTimeUS, iUseTimeUS);
        __sync_fetch_and_add(&m_iCnt, 1);
    }

    void Print();
};

} // namespace Certain

#endif // CERTAIN_UTILS_USETIMESTAT_H_
