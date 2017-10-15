#ifndef CERTAIN_UUIDGROUPMNG_H_
#define CERTAIN_UUIDGROUPMNG_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsUUIDGroup
{
private:
    // iUUID --> iTimeout
    clsLRUTable<uint64_t, uint32_t> *m_poLRUTable;

    clsRWLock m_oRWLock;

    bool CheckTimeout(uint64_t iCheckUUID = 0);

public:
    clsUUIDGroup()
    {
        m_poLRUTable = new clsLRUTable<uint64_t, uint32_t>(MAX_UUID_NUM);
    }

    ~clsUUIDGroup()
    {
        delete m_poLRUTable;
    }

    size_t Size();

    bool IsUUIDExist(uint64_t iUUID);
    bool AddUUID(uint64_t iUUID);
};

class clsUUIDGroupMng : public clsSingleton<clsUUIDGroupMng>
{
private:
    clsUUIDGroup aoGroup[UUID_GROUP_NUM];

    friend class clsSingleton<clsUUIDGroupMng>;
    clsUUIDGroupMng() { }

public:
    int Init(clsConfigure *poConf) { return 0; }
    void Destroy() { }

    bool IsUUIDExist(uint64_t iEntityID, uint64_t iUUID);

    bool AddUUID(uint64_t iEntityID, uint64_t iUUID);
};

} // namespace Certain

#endif
