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

int clsCertainUserImpl::GetServiceAddr(uint64_t iEntityID,
        uint32_t iAcceptorID, std::string &strAddr)
{
    uint32_t iServerID = 0;
    GetServerID(iEntityID, iAcceptorID, iServerID);
    if (iServerID >= m_poConf->GetServerNum()) assert(false);

    Certain::InetAddr_t tAddr = m_poConf->GetServerAddrs()[iServerID];

    char sIP[32] = {0};
    inet_ntop(AF_INET, (void*)&tAddr.tAddr.sin_addr, sIP, sizeof(sIP));

    strAddr = (std::string)sIP + ":" + std::to_string(50050 + iAcceptorID);

    return 0;
}

void clsCertainUserImpl::LockEntity(uint64_t iEntityID, void **ppLockInfo)
{
    *(uintptr_t *)ppLockInfo = iEntityID;
    m_poCoHashLock->Lock(iEntityID);
}

void clsCertainUserImpl::UnLockEntity(void *ppLockInfo)
{
    uint64_t iEntityID = uintptr_t(ppLockInfo);
    m_poCoHashLock->Unlock(iEntityID);
}

void clsCertainUserImpl::TickHandleCallBack()
{
    m_poCoHashLock->CheckAllLock();
}
