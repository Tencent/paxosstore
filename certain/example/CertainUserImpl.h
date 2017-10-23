#pragma once
#include "certain/Certain.h"

#include "CoHashLock.h"
#include "network/InetAddr.h"

class clsCertainUserImpl : public Certain::clsCertainUserBase
{
private:
    uint16_t m_hPort;
    Certain::clsConfigure *m_poConf;
    clsCoHashLock *m_poCoHashLock;

public:
    clsCertainUserImpl() : m_hPort(0), 
                           m_poConf(NULL),
                           m_poCoHashLock(new clsCoHashLock(100000)) { }

    ~clsCertainUserImpl() { }

    virtual int GetLocalAcceptorID(uint64_t iEntityID,
            uint32_t &iLocalAcceptorID);

    virtual int GetServerID(uint64_t iEntityID,
            uint32_t iAcceptorID, uint32_t &iServerID);

    virtual int InitServerAddr(Certain::clsConfigure *poConf);

    virtual void LockEntity(uint64_t iEntityID, void **ppLockInfo);

    virtual void UnLockEntity(void *ppLockInfo);

    virtual void TickHandleCallBack();

    int GetServiceAddr(uint64_t iEntityID, uint32_t iAcceptorID, std::string &tAddr);

    uint16_t GetServicePort() { return m_hPort; }
};
