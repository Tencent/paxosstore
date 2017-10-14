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

    virtual int GetLocalAcceptorID(uint64_t iEntityID,
            uint32_t &iLocalAcceptorID)
    {
        // Required.
        return 0;
    }

    virtual int GetServerID(uint64_t iEntityID,
            uint32_t iAcceptorID, uint32_t &iServerID)
    {
        // Required.
        return 0;
    }

    virtual void LockEntity(uint64_t iEntityID, void **ppLockInfo)
    {
        // Not required.
    }

    virtual void UnLockEntity(void *ppLockInfo)
    {
        // Not required.
    }

    virtual void LockPLogEntity(uint64_t iEntityID, void **ppLockInfo)
    {
        // Not required.
        // Implement if enable plog meta key.
    }

    virtual void UnLockPLogEntity(void *ppLockInfo)
    {
        // Not required.
        // Implement if enable plog meta key.
    }

    virtual int InitServerAddr(clsConfigure *poConf)
    {
        // Not required.
        // Implement if you have your own distribution mode,
        // Or it will follow 'ServerAddrs' in the configure.
        return 0;
    }

    virtual void OnReady()
    {
        // Not required.
        // Implement if you have something to do when Certain is ready.
    }

    virtual uint32_t GetStartRoutineID()
    {
        // Not required.
        // Implement if use colib based thread worker.
        return 0;
    }

    virtual void SetRoutineID(uint32_t iRoutineID)
    {
        // Not required.
        // Implement if use colib based thread worker.
        return;
    }

    virtual void TickHandleCallBack()
    {
        // Not required.
        // Implement if use colib based thread worker.
    }

    virtual string GetConfSuffix()
    {
        // Not required.
        return "";
    }

    virtual int GetControlGroupID(uint64_t iEntityID)
    {
        // Not required.
        return -1;
    }

    virtual uint32_t GetControlGroupLimit()
    {
        // Not required.
        return MAX_ASYNC_PIPE_NUM / 2;
    }

    virtual int UpdateServerAddr(Certain::clsConfigure *poConf)
    {
        // Not required.
        return 0;
    }

    virtual int GetSvrAddr(uint64_t iEntityID, uint32_t iAcceptorID, Certain::InetAddr_t & tAddr)
    {
        return 0;
    }
};

} // namespace Certian

#endif
