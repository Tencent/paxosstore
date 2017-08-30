#include "DBImpl.h"

namespace Certain
{

static string StoreKey(const string &strKey)
{
	string strStoreKey = "a";
	strStoreKey.append(strKey);
	return strStoreKey;
}

static string StoreKey(const uint64_t iEntityID)
{
	string strStoreKey = "b";
	strStoreKey.append((char *)&iEntityID, sizeof(iEntityID));
	return strStoreKey;
}

static string SerializeEntry(const uint64_t iEntry)
{
	return string((char *)&iEntry, sizeof(iEntry));
}

static uint64_t ParseEntry(const string strEntry)
{
	AssertEqual(strEntry.size(), sizeof(uint64_t));
	return *(uint64_t *)strEntry.data();
}

int clsDBImpl::ExcuteGet(clsSimpleCmd *poCmd)
{
	string strValue;
	string strStoreKey = StoreKey(poCmd->GetKey());
	int iRet = m_poKVEngine->Get(strStoreKey, strValue);
	poCmd->SetValue(strValue);

	return iRet;
}

int clsDBImpl::ExcuteSet(clsSimpleCmd *poCmd, string &strWriteBatch)
{
	static const int kBufferSize = 1024;
	static __thread char acBuffer[kBufferSize];

	int iRet = poCmd->SerializeToArray(acBuffer, kBufferSize);
	AssertLess(0, iRet);

    assert(iRet <= kBufferSize);
	strWriteBatch = string(acBuffer, iRet);

	return eRetCodeOK;
}

int clsDBImpl::Commit(uint64_t iEntityID, uint64_t iEntry,
		const string &strWriteBatch)
{
	CheckMaxCommitedEntry(iEntityID, iEntry);
	vector< pair<string, string> > vecKeyValue;

	{
		string strStoreKey = StoreKey(iEntityID);
		string strValue = SerializeEntry(iEntry);
		vecKeyValue.push_back(make_pair(strStoreKey, strValue));
	}

	if (strWriteBatch.size() == 0)
	{
		CertainLogFatal("E(%lu, %lu) Check if Noop Come",
				iEntityID, iEntry);
		return eRetCodeDBExcuteErr;
	}

	clsSimpleCmd *poCmd = new clsSimpleCmd;
	clsAutoDelete<clsSimpleCmd> oAuto(poCmd);

	AssertEqual(poCmd->ParseFromArray(strWriteBatch.c_str(),
				strWriteBatch.size()), 0);

	AssertEqual(poCmd->GetEntityID(), iEntityID);
	AssertEqual(poCmd->GetEntry(), iEntry);

	AssertEqual(poCmd->GetSubCmdID(), clsSimpleCmd::kSet);

	{
		string strStoreKey = StoreKey(poCmd->GetKey());
		vecKeyValue.push_back(make_pair(strStoreKey, poCmd->GetValue()));
	}

	return m_poKVEngine->MultiPut(vecKeyValue);
}

int clsDBImpl::ExcuteCmd(clsClientCmd *poClientCmd, string &strWriteBatch)
{
	int iRet;

	strWriteBatch.clear();
	CheckMaxCommitedEntry(poClientCmd->GetEntityID(), poClientCmd->GetEntry());

	AssertEqual(poClientCmd->GetCmdID(), kSimpleCmd);
	clsSimpleCmd *poCmd = dynamic_cast<clsSimpleCmd *>(poClientCmd);

	switch (poCmd->GetSubCmdID())
	{
		case clsSimpleCmd::kGet:
			iRet = ExcuteGet(poCmd);
			break;

		case clsSimpleCmd::kSet:
			iRet = ExcuteSet(poCmd, strWriteBatch);
			break;

		default:
			assert(false);
	}

	if (iRet != eRetCodeOK && iRet != eRetCodeNotFound)
	{
		return eRetCodeDBExcuteErr;
	}
	return iRet;
}

void clsDBImpl::CheckMaxCommitedEntry(uint64_t iEntityID, uint64_t iEntry)
{
	uint64_t iMaxCommitedEntry;
    uint32_t iFlag;
	LoadMaxCommitedEntry(iEntityID, iMaxCommitedEntry, iFlag);
	AssertEqual(iMaxCommitedEntry + 1, iEntry);
}

int clsDBImpl::LoadMaxCommitedEntry(uint64_t iEntityID,
		uint64_t &iCommitedEntry, uint32_t &iFlag)
{
	string strValue;
	string strStoreKey = StoreKey(iEntityID);
	iCommitedEntry = 0;

	if (m_poKVEngine->Get(strStoreKey, strValue) == eRetCodeOK)
	{
		iCommitedEntry = ParseEntry(strValue);
	}

	return eRetCodeOK;
}

} // namespace Certain
