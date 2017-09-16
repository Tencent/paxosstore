#ifndef CERTAIN_NETWORK_EPOLLIO_H_
#define CERTAIN_NETWORK_EPOLLIO_H_

#include "network/SocketHelper.h"

namespace Certain
{

#define CERTAIN_MAX_FD_NUM				(1 << 20)
#define CERTAIN_EPOLLIO_UNREACHABLE do { assert(false); return 0; } while(0)

const int kDefaultFDEvents = EPOLLIN | EPOLLOUT | EPOLLET;
const int kDefaultEventSize = 8096;

class clsFDBase;
class clsIOHandlerBase
{
public:
    virtual ~clsIOHandlerBase() { }
    virtual int HandleRead(clsFDBase *poFD) = 0;
    virtual int HandleWrite(clsFDBase *poFD) = 0;
};

class clsFDBase
{
private:
    int m_iFD;
    uint32_t m_iFDID;

    clsIOHandlerBase *m_poIOHandler;
    int m_iEvents;

    bool m_bReadable;
    bool m_bWritable;

public:
    clsFDBase(int iFD, clsIOHandlerBase *poIOHandler = NULL,
            uint32_t iFDID = -1, int iEvents = kDefaultFDEvents) :
            m_iFD(iFD),
            m_iFDID(iFDID),
            m_poIOHandler(poIOHandler),
            m_iEvents(iEvents),
            m_bReadable(false),
            m_bWritable(false) { }

    virtual ~clsFDBase() { };

    int GetFD() { return m_iFD; }
    uint32_t GetFDID() { return m_iFDID; }

    int GetEvents() { return m_iEvents; }

    BOOLEN_IS_SET(Readable);
    BOOLEN_IS_SET(Writable);

    clsIOHandlerBase *GetIOHandler()
    {
        return m_poIOHandler;
    }
    void SetIOHandler(clsIOHandlerBase *poIOHandler)
    {
        m_poIOHandler = poIOHandler;
    }
};

class clsEpollIO
{
private:
    int m_iEpollFD;

    uint32_t m_iEventSize;
    epoll_event *m_ptEpollEv;

    struct UniqueFDPtr_t
    {
        uint32_t iFDID;
        clsFDBase *poFD;
    };
    UniqueFDPtr_t *m_atUniqueFDPtrMap;

public:
    clsEpollIO(uint32_t iEventSize = kDefaultEventSize);
    ~clsEpollIO();

    int Add(clsFDBase *poFD);
    int Remove(clsFDBase *poFD);
    int RemoveAndCloseFD(clsFDBase *poFD);
    int Modify(clsFDBase *poFD);

    clsFDBase *GetFDBase(int iFD, uint32_t iFDID);

    void RunOnce(int iTimeoutMS);
};

} // namespace Certain

#endif // CERTAIN_NETWORK_EPOLLIO_H_
