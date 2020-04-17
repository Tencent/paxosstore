#ifndef CERTAIN_NETWORK_INETADDR_H_
#define CERTAIN_NETWORK_INETADDR_H_

#include "utils/Assert.h"
#include "utils/CRC32.h"
#include "utils/Hash.h"
#include "utils/Time.h"
#include "utils/AutoHelper.h"

namespace Certain
{

struct InetAddr_t
{
    struct sockaddr_in tAddr;

    bool operator == (const InetAddr_t &tOther) const
    {
        const struct sockaddr_in &tOtherAddr = tOther.tAddr;

        if (tAddr.sin_addr.s_addr != tOtherAddr.sin_addr.s_addr)
        {
            return false;
        }

        if (tAddr.sin_port != tOtherAddr.sin_port)
        {
            return false;
        }

        return true;
    }

    bool operator < (const InetAddr_t &tOther) const
    {
        const struct sockaddr_in &tOtherAddr = tOther.tAddr;

        if (tAddr.sin_addr.s_addr != tOtherAddr.sin_addr.s_addr)
        {
            return tAddr.sin_addr.s_addr < tOtherAddr.sin_addr.s_addr;
        }

        if (tAddr.sin_port != tOtherAddr.sin_port)
        {
            return tAddr.sin_port < tOtherAddr.sin_port;
        }

        return false;
    }

    InetAddr_t()
    {
        memset(&tAddr, 0, sizeof(tAddr));
    }

    InetAddr_t(struct sockaddr_in tSockAddr)
    {
        tAddr = tSockAddr;
    }

    InetAddr_t(const char *sIP, uint16_t iPort)
    {
        memset(&tAddr, 0, sizeof(tAddr));
        tAddr.sin_family = AF_INET;
        tAddr.sin_addr.s_addr = inet_addr(sIP);
        tAddr.sin_port = htons(iPort);
    }

    InetAddr_t(uint32_t iIP, uint16_t iPort)
    {
        memset(&tAddr, 0, sizeof(tAddr));
        tAddr.sin_family = AF_INET;
        tAddr.sin_addr.s_addr = iIP;
        tAddr.sin_port = htons(iPort);
    }

    string ToString() const
    {
        const char *sIP = inet_ntoa(tAddr.sin_addr);
        uint16_t iPort = ntohs(tAddr.sin_port);

        char acBuffer[32];
        snprintf(acBuffer, 32, "%s:%hu", sIP, iPort);

        return acBuffer;
    }

    uint32_t GetNetOrderIP() const
    {
        return tAddr.sin_addr.s_addr;
    }
};

} // namespace Certain

#endif // CERTAIN_NETWORK_SOCKETHELPER_H_
