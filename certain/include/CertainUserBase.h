#ifndef CERTAIN_INCLUDE_CERTAIN_USER_BASE_H_
#define CERTAIN_INCLUDE_CERTAIN_USER_BASE_H_

#include "Command.h"
#include "PerfLog.h"
#include "AsyncPipeMng.h"
#include "AsyncQueueMng.h"
#include "UUIDGroupMng.h"
#include "EntityInfoMng.h"

namespace Certain
{

class clsCertainUserBase
{
public:
    virtual ~clsCertainUserBase() { }

    virtual void LockEntity(uint64_t iEntityID, void **ppLockInfo)
    {
    }
    virtual void BatchLockEntity(map<uint64_t, uint32_t> &, void **ppLockInfo)
    {
    }
    virtual void UnLockEntity(void *ppLockInfo)
    {
    }

    virtual void LockPLogEntity(uint64_t iEntityID, void **ppLockInfo)
    {
        assert(false);
    }

    virtual void UnLockPLogEntity(void *ppLockInfo)
    {
        assert(false);
    }

    virtual int GetLocalAcceptorID(uint64_t iEntityID,
            uint32_t &iLocalAcceptorID)
    {
        return 0;
    }

    virtual int GetServerID(uint64_t iEntityID,
            uint32_t iAcceptorID, uint32_t &iServerID)
    {
        return 0;
    }

    virtual int InitServerAddr(clsConfigure *poConf)
    {
        // If ServerAddrs and ExtAddr have defined in certain.conf,
        // it's no need implement this function.
        return 0;
    }

    virtual void OnReady()
    {
        // If you have nothing to do when Certain is ready,
        // it's no need implement this function.
    }

    virtual uint32_t GetStartRoutineID()
    {
        return 0;
    }

    typedef int (*CallBackType)();
    virtual CallBackType HandleLockCallBack()
    {
        return NULL;
    }

    virtual bool IsRejectAll()
    {
        return false;
    }

    virtual string GetConfSuffix()
    {
        return "";
    }

    virtual int GetControlGroupID(uint64_t iEntityID)
    {
        return -1;
    }

    virtual uint32_t GetControlGroupLimit()
    {
        // conservative 50% limit
        return MAX_ASYNC_PIPE_NUM / 2;
    }

    virtual int UpdateServerAddr(Certain::clsConfigure *poConf)
    {
        return 0;
    }
};

} // namespace Certian

#endif
