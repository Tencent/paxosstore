#pragma once
#include "Certain.h"
#include "network/InetAddr.h"

class clsCertainUserImpl : public Certain::clsCertainUserBase
{
private:
    uint16_t m_hPort;
    Certain::clsConfigure *m_poConf;

public:
    clsCertainUserImpl() : m_hPort(0), m_poConf(NULL) { }
    ~clsCertainUserImpl() { }

    virtual int GetLocalAcceptorID(uint64_t iEntityID,
            uint32_t &iLocalAcceptorID);

    virtual int GetServerID(uint64_t iEntityID,
            uint32_t iAcceptorID, uint32_t &iServerID);

    virtual int InitServerAddr(Certain::clsConfigure *poConf);

    virtual int GetSvrAddr(uint64_t iEntityID, uint32_t iAcceptorID, Certain::InetAddr_t & tAddr);

    uint16_t GetServicePort() { return m_hPort; }
};
