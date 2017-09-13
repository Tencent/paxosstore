#include "utils/Thread.h"
#include "utils/Time.h"

namespace Certain
{

bool clsThreadBase::IsExited()
{
    if (m_bExited)
    {
        return true;
    }

    if (!m_bExiting)
    {
        return false;
    }

    int iRet = pthread_kill(m_tID, 0);
    if (iRet == ESRCH)
    {
        m_bExited = true;
        assert(pthread_join(m_tID, NULL) == 0);
    }

    return m_bExited;
}

bool clsThreadBase::CheckIfExiting(uint64_t iStopWaitTimeMS)
{
    if (!m_bStopFlag)
    {
        return false;
    }

    if (m_bExiting)
    {
        return true;
    }

    uint64_t iCurrTimeMS = GetCurrTimeMS();

    if (m_iStopStartTimeMS == 0)
    {
        m_iStopStartTimeMS = iCurrTimeMS;
    }
    else if (m_iStopStartTimeMS + iStopWaitTimeMS <= iCurrTimeMS)
    {
        m_bExiting = true;
    }

    return m_bExiting;
}

} // namespace Certain
