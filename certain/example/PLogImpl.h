#pragma once

#include "certain/Certain.h"

#include "Coding.h"
#include "CoHashLock.h"
#include "DBType.h"

class clsPLogFilter : public dbtype::CompactionFilter
{
public:
    bool Filter(int iLevel, const dbtype::Slice& oKey,
            const dbtype::Slice& oExistingValue, std::string* strNewValue,
            bool* bValueChanged) const override
    {
        uint64_t iEntityID = 0, iEntry = 0, iValueID = 0, iTimestampMS = 0;
        if (!DecodePLogKey(oKey, iEntityID, iEntry, &iTimestampMS) &&
                !DecodePLogValueKey(oKey, iEntityID, iEntry, iValueID, &iTimestampMS))
        {
            return false;
        }
        return Certain::clsCertainWrapper::GetInstance()->CheckIfEntryDeletable(
                iEntityID, iEntry, iTimestampMS);
    }

    const char* Name() const override { return "clsPLogFilter"; }
};

class clsPLogComparator : public dbtype::Comparator
{
public:
    int Compare(const dbtype::Slice& a, const dbtype::Slice& b) const override
    {
        assert(a.size() >= 8 && b.size() >= 8);
        dbtype::Slice ta = a, tb = b;
        ta.remove_prefix(8);
        tb.remove_prefix(8);
        return ta.compare(tb);
    }

    const char* Name() const override { return "clsPLogComparator"; }

    void FindShortestSeparator(std::string* start,
            const dbtype::Slice& limit) const override
    {
        // return with *start unchanged
    }

    virtual void FindShortSuccessor(std::string* key) const
    {
        // return with *key unchanged
    }
};

class clsPLogImpl : public Certain::clsPLogBase
{
private:
    dbtype::DB *m_poLevelDB;

public:
    clsPLogImpl(dbtype::DB *poLevelDB) : m_poLevelDB(poLevelDB) { }

    virtual ~clsPLogImpl() { }

    virtual int PutValue(uint64_t iEntityID, uint64_t iEntry,
            uint64_t iValueID, const std::string &strValue);

    virtual int GetValue(uint64_t iEntityID, uint64_t iEntry,
            uint64_t iValueID, std::string &strValue);

    virtual int Put(uint64_t iEntityID, uint64_t iEntry,
            const std::string &strRecord);

    virtual int Get(uint64_t iEntityID, uint64_t iEntry,
            std::string &strRecord);

    virtual int PutWithPLogEntityMeta(uint64_t iEntityID, uint64_t iEntry,
            const Certain::PLogEntityMeta_t &tMeta, const std::string &strRecord);

    virtual int GetPLogEntityMeta(uint64_t iEntityID,
            Certain::PLogEntityMeta_t &tMeta);

    virtual int LoadUncommitedEntrys(uint64_t iEntityID,
            uint64_t iMaxCommitedEntry, uint64_t iMaxLoadingEntry,
            std::vector< std::pair<uint64_t, std::string> > &vecRecord, bool &bHasMore);
};
