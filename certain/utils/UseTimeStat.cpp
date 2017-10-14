#include "utils/UseTimeStat.h"

namespace Certain
{

void clsUseTimeStat::Print()
{
    uint64_t iTotalUseTimeUS = __sync_fetch_and_and(&m_iTotalUseTimeUS, 0);
    uint64_t iCnt = __sync_fetch_and_and(&m_iCnt, 0);
    uint64_t iMaxUseTimeUS = __sync_fetch_and_and(&m_iMaxUseTimeUS, 0);

    if (iCnt == 0)
    {
        if (g_iCertainUseLog)
        {
            CertainLogImpt("certain_stat %s cnt 0", m_strTag.c_str());
        }
        else
        {
            printf("certain_stat %s cnt 0\n", m_strTag.c_str());
        }
    }
    else
    {
        if (g_iCertainUseLog)
        {
            CertainLogImpt("certain_stat %s max_us %lu avg_us %lu cnt %lu",
                    m_strTag.c_str(), iMaxUseTimeUS, iTotalUseTimeUS / iCnt, iCnt);
        }
        else
        {
            printf("certain_stat %s max_us %lu avg_us %lu cnt %lu\n",
                    m_strTag.c_str(), iMaxUseTimeUS, iTotalUseTimeUS / iCnt, iCnt);
        }
    }
}

} // namespace Certain
