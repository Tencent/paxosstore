#ifndef CERTAIN_LEASEPOLICY_H_
#define CERTAIN_LEASEPOLICY_H_

#include "utils/Time.h"

namespace Certain
{

class clsLeasePolicy
{
private:
    uint32_t m_iLocalAcceptorID;
    uint32_t m_iLeaseDurationMS;
    uint64_t m_iExpiredTimeMS;

public:
    static const uint64_t kUnlimitedMS = uint64_t(-1);

    clsLeasePolicy(uint32_t iLocalAcceptorID, uint32_t iLeaseDurationMS)
        : m_iLocalAcceptorID(iLocalAcceptorID),
          m_iLeaseDurationMS(iLeaseDurationMS),
          m_iExpiredTimeMS(0) { }

    ~clsLeasePolicy() { }

    void OnRecvMsgSuccessfully()
    {
        uint64_t iCurrTimeMS = Certain::GetCurrTimeMS();
        m_iExpiredTimeMS = iCurrTimeMS + m_iLeaseDurationMS;
    }

    // Used to check if lease has expired.
    // 0 means lease expired, return x>0 means that x MS remains.
    uint64_t GetLeaseTimeoutMS()
    {
        uint64_t iCurrTimeMS = Certain::GetCurrTimeMS();

        if (m_iExpiredTimeMS <= iCurrTimeMS)
        {
            return 0;
        }

        if (m_iLocalAcceptorID == 1)
        {
            return m_iExpiredTimeMS - iCurrTimeMS;
        }
        else
        {
            return kUnlimitedMS;
        }
    }

    void Reset(uint32_t iLeaseDurationMS)
    {
        m_iLeaseDurationMS = iLeaseDurationMS;
        m_iExpiredTimeMS = 0;
    }
};

} // namespace Certain

#endif // CERTAIN_LEASEPOLICY_H_
