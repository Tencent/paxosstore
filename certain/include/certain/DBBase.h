#ifndef CERTAIN_INCLUDE_CERTAIN_DB_BASE_H_
#define CERTAIN_INCLUDE_CERTAIN_DB_BASE_H_

#include "certain/CertainUserBase.h"

namespace Certain
{

struct EntryValue_t
{
    uint64_t iEntityID;
    uint64_t iEntry;
    string strValue;
};

class clsDBBase
{
public:
    static int MultiCommit(uint64_t iEntityID, uint64_t iMaxCommitedEntry,
            uint64_t iMaxTaskEntry);

public:
    virtual ~clsDBBase() { }

    virtual int Commit(uint64_t iEntityID, uint64_t iEntry,
            const string &strWriteBatch) = 0;

    virtual int MultiCommit(vector<EntryValue_t> vecEntryValue) = 0;

    virtual int GetEntityMeta(uint64_t iEntityID,
            uint64_t &iCommitedEntry, uint32_t &iFlag) = 0;

    virtual int GetAllAndSet(uint64_t iEntityID, uint32_t iAcceptorID,
            uint64_t &iMaxCommitedEntry) = 0;
};

} // namespace Certian

#endif
