#ifndef CERTAIN_EXAMPLE_SIMPLE_SIMPLEPLOG_H_
#define CERTAIN_EXAMPLE_SIMPLE_SIMPLEPLOG_H_

#include "Certain.h"
#include "simple/KVEngine.h"

namespace Certain
{

class clsPLogImpl : public clsPLogBase
{
private:
	struct EntryKey_t
	{
		uint64_t iEntityID;
		uint64_t iEntry;
		uint64_t iValueID;

		EntryKey_t() { }

		EntryKey_t(uint64_t iEntityID, uint64_t iEntry,
				uint64_t iValueID = 0)
		{
			this->iEntityID = iEntityID;
			this->iEntry = iEntry;
			this->iValueID = iValueID;
		}
	};

    uint64_t ntohll(uint64_t i)
    {
        if (__BYTE_ORDER == __LITTLE_ENDIAN) {
            return (((uint64_t)ntohl((uint32_t)i)) << 32) | (uint64_t)(ntohl((uint32_t)(i >> 32)));
        }
        return i;
    }

    uint64_t htonll(uint64_t i)
    {
        if (__BYTE_ORDER == __LITTLE_ENDIAN) {
            return (((uint64_t)htonl((uint32_t)i)) << 32) | ((uint64_t)htonl((uint32_t)(i >> 32)));
        }
        return i;
    }

	string Serialize(const EntryKey_t &tKey)
	{
        EntryKey_t t = tKey;
        t.iEntityID = htonll(tKey.iEntityID);
        t.iEntry = htonll(tKey.iEntry);
        t.iValueID = htonll(tKey.iValueID);
		return string((const char *)&t, sizeof(t));
	}

	EntryKey_t Parse(const string &strKey)
	{
		EntryKey_t t = *(EntryKey_t *)(strKey.data());
        t.iEntityID = ntohll(t.iEntityID);
        t.iEntry = ntohll(t.iEntry);
        t.iValueID = ntohll(t.iValueID);
        return t;
	}

	clsKVEngine *m_poKVEngine;

public:
	clsPLogImpl(clsKVEngine *poKVEngine)
			: m_poKVEngine(poKVEngine) { assert(__BYTE_ORDER == __LITTLE_ENDIAN); }

	virtual ~clsPLogImpl() { }

	virtual int PutValue(uint64_t iEntityID, uint64_t iEntry,
			uint64_t iValueID, const string &strValue);

	virtual int GetValue(uint64_t iEntityID, uint64_t iEntry,
			uint64_t iValueID, string &strValue);

	virtual int Put(uint64_t iEntityID, uint64_t iEntry,
			const string &strRecord);

	virtual int Get(uint64_t iEntityID, uint64_t iEntry,
			string &strRecord);

	virtual int LoadUncommitedEntrys(uint64_t iEntityID,
			uint64_t iMaxCommitedEntry, uint64_t iMaxLoadingEntry,
			vector< pair<uint64_t, string> > &vecRecord, bool &bHasMore);
};

} // namespace Certain

#endif
