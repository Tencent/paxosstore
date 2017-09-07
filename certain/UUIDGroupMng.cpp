#include "UUIDGroupMng.h"

namespace Certain
{

bool clsUUIDGroup::CheckTimeout(uint64_t iCheckUUID)
{
    uint64_t iCurrTime = GetCurrTime();

    uint64_t iUUID;
    uint32_t iTimeout;

    bool bCheckUUIDRemoved = false;

    while (m_poLRUTable->PeekOldest(iUUID, iTimeout))
    {
        if (iTimeout > iCurrTime)
        {
            break;
        }

        Assert(m_poLRUTable->Remove(iUUID));

        if (iUUID == iCheckUUID)
        {
            bCheckUUIDRemoved = true;
        }
    }

    return bCheckUUIDRemoved;
}

size_t clsUUIDGroup::Size()
{
    clsThreadReadLock oReadLock(&m_oRWLock);

    return m_poLRUTable->Size();
}

bool clsUUIDGroup::IsUUIDExist(uint64_t iUUID)
{
    {
        clsThreadReadLock oReadLock(&m_oRWLock);

        if (!m_poLRUTable->Find(iUUID))
        {
            return false;
        }
    }

    {
        clsThreadWriteLock oWriteLock(&m_oRWLock);

        return !CheckTimeout(iUUID);
    }
}

bool clsUUIDGroup::AddUUID(uint64_t iUUID)
{
    clsThreadWriteLock oWriteLock(&m_oRWLock);

    CheckTimeout();

    uint32_t iTimeout = GetCurrTime() + 3600; // 1hour

    return m_poLRUTable->Add(iUUID, iTimeout);
}

bool clsUUIDGroupMng::IsUUIDExist(uint64_t iEntityID, uint64_t iUUID)
{
    uint32_t iGroupID = Hash(iEntityID) % UUID_GROUP_NUM;
    return aoGroup[iGroupID].IsUUIDExist(iUUID);
}

bool clsUUIDGroupMng::AddUUID(uint64_t iEntityID, uint64_t iUUID)
{
    uint32_t iGroupID = Hash(iEntityID) % UUID_GROUP_NUM;
    return aoGroup[iGroupID].AddUUID(iUUID);
}

} // namespace Certain
