#include "PLogImpl.h"

namespace Certain
{

int clsPLogImpl::PutValue(uint64_t iEntityID, uint64_t iEntry,
		uint64_t iValueID, const string &strValue)
{
	AssertNotEqual(0, iValueID);
	string strKey = Serialize(EntryKey_t(iEntityID, iEntry, iValueID));

	return m_poKVEngine->Put(strKey, strValue);
}

int clsPLogImpl::GetValue(uint64_t iEntityID, uint64_t iEntry,
		uint64_t iValueID, string &strValue)
{
	AssertLess(0, iValueID);
	string strKey = Serialize(EntryKey_t(iEntityID, iEntry, iValueID));

	return m_poKVEngine->Get(strKey, strValue);
}

int clsPLogImpl::Put(uint64_t iEntityID, uint64_t iEntry,
		const string &strRecord)
{
	string strKey = Serialize(EntryKey_t(iEntityID, iEntry));
	return m_poKVEngine->Put(strKey, strRecord);
}

int clsPLogImpl::Get(uint64_t iEntityID, uint64_t iEntry,
		string &strRecord)
{
	string strKey = Serialize(EntryKey_t(iEntityID, iEntry));
	return m_poKVEngine->Get(strKey, strRecord);
}

int clsPLogImpl::LoadUncommitedEntrys(uint64_t iEntityID,
		uint64_t iMaxCommitedEntry, uint64_t iMaxLoadingEntry,
		vector< pair<uint64_t, string> > &vecRecord, bool &bHasMore)
{
    bHasMore = false;
	vecRecord.clear();
	string strStart = Serialize(EntryKey_t(iEntityID, iMaxCommitedEntry));
	string strEnd = Serialize(EntryKey_t(iEntityID, uint64_t(-1)));

	vector< pair<string, string> > vecKeyValue;
	m_poKVEngine->RangeLoad(strStart, strEnd, vecKeyValue);

	for (vector< pair<string, string> >::iterator iter = vecKeyValue.begin();
			iter != vecKeyValue.end(); ++iter)
	{
		pair<uint64_t, string> tPair;
		EntryKey_t tKey = Parse(iter->first);
		AssertEqual(tKey.iEntityID, iEntityID);
		AssertLess(iMaxCommitedEntry, tKey.iEntry);

		tPair.first = tKey.iEntry;
		tPair.second = iter->second;
		vecRecord.push_back(tPair);
	}

	return 0;
}

} // namespace Certain
