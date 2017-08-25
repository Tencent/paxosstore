
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

int clsDBImpl::SubmitGet(clsSimpleCmd *poCmd)
{
	string strValue;
	string strStoreKey = StoreKey(poCmd->GetKey());
	poCmd->SetResult(m_poKVEngine->Get(strStoreKey, strValue));
	poCmd->SetValue(strValue);

	return eRetCodeOK;
}

int clsDBImpl::SubmitSet(clsSimpleCmd *poCmd, string &strWriteBatch)
{
	poCmd->SetResult(eRetCodeOK);

	int iRet = poCmd->SerializeToArray(m_pcBuffer, kBufferSize);
	AssertLess(0, iRet);
	strWriteBatch = string(m_pcBuffer, iRet);

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
		return eRetCodeDBSubmitErr;
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

int clsDBImpl::Submit(clsClientCmd *poClientCmd, string &strWriteBatch)
{
	int iRet;

	strWriteBatch.clear();
	CheckMaxCommitedEntry(poClientCmd->GetEntityID(), poClientCmd->GetEntry());

	AssertEqual(poClientCmd->GetCmdID(), kSimpleCmd);
	clsSimpleCmd *poCmd = dynamic_cast<clsSimpleCmd *>(poClientCmd);

	switch (poCmd->GetSubCmdID())
	{
		case clsSimpleCmd::kGet:
			iRet = SubmitGet(poCmd);
			break;

		case clsSimpleCmd::kSet:
			iRet = SubmitSet(poCmd, strWriteBatch);
			break;

		default:
			assert(false);
	}

	if (iRet != eRetCodeOK && iRet != eRetCodeNotFound)
	{
		return eRetCodeDBSubmitErr;
	}
	return eRetCodeOK;
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
