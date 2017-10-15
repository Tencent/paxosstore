#include "CertainUserImpl.h"

int clsCertainUserImpl::GetLocalAcceptorID(uint64_t iEntityID,
        uint32_t &iLocalAcceptorID)
{
    assert(m_poConf != NULL);
    iLocalAcceptorID = m_poConf->GetLocalServerID();
    return 0;
}

int clsCertainUserImpl::GetServerID(uint64_t iEntityID,
        uint32_t iAcceptorID, uint32_t &iServerID)
{
    assert(m_poConf != NULL);
    iServerID = iAcceptorID;
    return 0;
}

int clsCertainUserImpl::InitServerAddr(Certain::clsConfigure *poConf)
{
    poConf->SetServerNum(poConf->GetServerAddrs().size());
    poConf->SetAcceptorNum(poConf->GetServerAddrs().size());
    m_poConf = poConf;
    m_hPort = 50050 + m_poConf->GetLocalServerID();
    return 0;
}

int clsCertainUserImpl::GetSvrAddr(uint64_t iEntityID, uint32_t iAcceptorID, Certain::InetAddr_t & tAddr)
{
    uint32_t iServerID = 0;
    GetServerID(iEntityID, iAcceptorID, iServerID);
    if (iServerID >= m_poConf->GetServerNum()) assert(false);

    tAddr = m_poConf->GetServerAddrs()[iServerID];

    return 0;
}
