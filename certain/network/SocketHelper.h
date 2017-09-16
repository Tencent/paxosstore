#ifndef CERTAIN_NETWORK_SOCKETHELPER_H_
#define CERTAIN_NETWORK_SOCKETHELPER_H_

#include "InetAddr.h"

namespace Certain
{

struct ConnInfo_t
{
    int iFD;

    InetAddr_t tLocalAddr;
    InetAddr_t tPeerAddr;

    string ToString() const
    {
        char acBuffer[128];
        snprintf(acBuffer, 128, "fd %d local %s peer %s",
                iFD, tLocalAddr.ToString().c_str(),
                tPeerAddr.ToString().c_str());
        return acBuffer;
    }
};

// The fd is set non block.
// If ptInetAddr is not NULL, the fd will be bound to it.
int CreateSocket(const InetAddr_t *ptInetAddr);

// Check if the fd is valid by getsockopt.
bool CheckIfValid(int iFD);

// Use the fd to connect the peer tInetAddr.
int Connect(int iFD, const InetAddr_t &tInetAddr);

// 1. Create a socket by CreateSocket, set to tConnInfo.iFD.
// 2. Connect the peer tInetAddr.
// 3. Set local address to tConnInfo.tLocalAddr finally.
int Connect(const InetAddr_t &tInetAddr, ConnInfo_t &tConnInfo);

int SetNonBlock(int iFD, bool bNonBlock = true);

int MakeNonBlockPipe(int &iInFD, int &iOutFD);

} // namespace Certain

#endif // CERTAIN_NETWORK_SOCKETHELPER_H_
