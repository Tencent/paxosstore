
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "EntryInfoMng.h"
#include "EntityInfoMng.h"
#include "EntryState.h"

namespace Certain
{

EntryInfo_t *clsEntryInfoMng::TakeTimeout()
{
	return m_poEntryTimer->TakeTimeoutElt();
}

void clsEntryInfoMng::AddTimeout(EntryInfo_t *ptInfo, uint32_t iTimeoutMS)
{
	CertainLogInfo("E(%lu, %lu) iTimeoutMS %u",
			ptInfo->ptEntityInfo->iEntityID, ptInfo->iEntry, iTimeoutMS);

	Assert(ptInfo != NULL && ptInfo->ptEntityInfo != NULL);

#if CERTAIN_DEBUG
	PLogPos_t tPos;
	tPos.iEntityID = ptInfo->ptEntityInfo->iEntityID;
	tPos.iEntry = ptInfo->iEntry;
	Assert(ptInfo->iEntry == 0 || m_poEntryTable->Find(tPos));
#endif

	if (m_poEntryTimer->Exist(ptInfo))
	{
		AssertEqual(m_poEntryTimer->Remove(ptInfo), true);
		AssertEqual(m_poEntryTimer->Add(ptInfo, iTimeoutMS), true);
	}
	else
	{
		AssertEqual(m_poEntryTimer->Add(ptInfo, iTimeoutMS), true);
	}
}

bool clsEntryInfoMng::WaitForTimeout(EntryInfo_t *ptInfo)
{
	return m_poEntryTimer->Exist(ptInfo);
}

void clsEntryInfoMng::RemoveTimeout(EntryInfo_t *ptInfo)
{
	CertainLogInfo("E(%lu, %lu)", ptInfo->ptEntityInfo->iEntityID,
			ptInfo->iEntry);
	m_poEntryTimer->Remove(ptInfo);
}

EntryInfo_t *clsEntryInfoMng::CreateEntryInfo(EntityInfo_t *ptEntityInfo,
		uint64_t iEntry)
{
	PLogPos_t tPos;
	tPos.iEntityID = ptEntityInfo->iEntityID;
	tPos.iEntry = iEntry;

	CertainLogDebug("E(%lu, %lu)", tPos.iEntityID, tPos.iEntry);

	if (m_poEntryTable->Find(tPos))
	{
		CertainLogFatal("entry_info exist E(%lu, %lu)",
				tPos.iEntityID, tPos.iEntry);
		return NULL;
	}

	//EntryInfo_t *ptInfo = (EntryInfo_t *)calloc(1, sizeof(EntryInfo_t));
	EntryInfo_t *ptInfo = (EntryInfo_t *)m_poFixSizePool->Alloc(sizeof(EntryInfo_t), false);
	Assert(ptInfo != NULL);
	memset(ptInfo, 0, sizeof(EntryInfo_t));

	ptInfo->iEntry = iEntry;

	ptInfo->apWaitingMsg = (clsPaxosCmd**)calloc(sizeof(clsPaxosCmd*), m_iAcceptorNum);

	ptInfo->poMachine = new clsEntryStateMachine;

	ptInfo->iActiveAcceptorID = INVALID_ACCEPTOR_ID;

	assert(ptEntityInfo != NULL);
	ptInfo->ptEntityInfo = ptEntityInfo;
	m_poEntityMng->RefEntityInfo(ptEntityInfo);

	CIRCLEQ_ENTRY_INIT(ptInfo, tListElt);
	CIRCLEQ_INSERT_HEAD(EntryInfo_t, &ptInfo->ptEntityInfo->tEntryList, ptInfo, tListElt);

	m_poEntryTable->Add(tPos, ptInfo);

	m_iCreateCnt++;

	if (m_iCreateCnt % 10000 == 0)
	{
		CertainLogImpt("worker %u create %lu destroy %lu catchup %u delta %lu mem %lu",
				m_iEntityWorkerID, m_iCreateCnt, m_iDestroyCnt, m_iCatchUpFlagCnt,
				m_iCreateCnt - m_iDestroyCnt, m_poEntityMng->GetMemCacheCtrl()->GetTotalSize());
	}

	return ptInfo;
}

void clsEntryInfoMng::DestroyEntryInfo(EntryInfo_t *ptInfo)
{
	PLogPos_t tPos;
	tPos.iEntityID = ptInfo->ptEntityInfo->iEntityID;
	tPos.iEntry = ptInfo->iEntry;

	CertainLogDebug("E(%lu, %lu)", tPos.iEntityID, tPos.iEntry);

#if CERTAIN_DEBUG
	if (!m_poEntryTable->Find(tPos))
	{
		CertainLogFatal("BUG entry_info not exist E(%lu, %lu)",
				tPos.iEntityID, tPos.iEntry);
		return;
	}
#endif

	m_poEntityMng->GetMemCacheCtrl()->RemoveFromTotalSize(ptInfo);

	RemoveTimeout(ptInfo);
	Assert(m_poEntryTable->Remove(tPos));

	RemoveCatchUpFlag(ptInfo);

	CIRCLEQ_REMOVE(EntryInfo_t, &ptInfo->ptEntityInfo->tEntryList, ptInfo, tListElt);
	m_poEntityMng->UnRefEntityInfo(ptInfo->ptEntityInfo);
	ptInfo->ptEntityInfo = NULL;

	Assert(!ptInfo->bUncertain);
	for (uint32_t i = 0; i < m_iAcceptorNum; ++i)
	{
		if (ptInfo->apWaitingMsg[i] != NULL)
		{
			CertainLogError("Wait still cmd: %s",
					ptInfo->apWaitingMsg[i]->GetTextCmd().c_str());
			delete ptInfo->apWaitingMsg[i];
		}
	}

	free(ptInfo->apWaitingMsg), ptInfo->apWaitingMsg = NULL;

	delete ptInfo->poMachine, ptInfo->poMachine = NULL;

	//free(ptInfo);
	m_poFixSizePool->Free((char *)ptInfo, false);

	m_iDestroyCnt++;

	if (m_iDestroyCnt % 10000 == 0
			|| m_iCreateCnt == m_iDestroyCnt)
	{
		CertainLogImpt("worker %u create %lu destroy %lu catchup %u delta %lu mem %lu",
				m_iEntityWorkerID, m_iCreateCnt, m_iDestroyCnt, m_iCatchUpFlagCnt,
				m_iCreateCnt - m_iDestroyCnt, m_poEntityMng->GetMemCacheCtrl()->GetTotalSize());
	}
}

EntryInfo_t *clsEntryInfoMng::FindEntryInfo(uint64_t iEntityID,
		uint64_t iEntry)
{
	PLogPos_t tPos;
	tPos.iEntityID = iEntityID;
	tPos.iEntry = iEntry;

	EntryInfo_t *ptInfo = NULL;

	if (!m_poEntryTable->Find(tPos, ptInfo))
	{
		CertainLogDebug("entry_info not exist E(%lu, %lu)", iEntityID, iEntry);
		return NULL;
	}

	Assert(ptInfo != NULL);

	return ptInfo;
}

void clsEntryInfoMng::IncreaseEliminatePriority(EntryInfo_t *ptInfo)
{
	Assert(ptInfo != NULL && ptInfo->ptEntityInfo != NULL);
	PLogPos_t tPos;
	tPos.iEntityID = ptInfo->ptEntityInfo->iEntityID;
	tPos.iEntry = ptInfo->iEntry;
	Assert(m_poEntryTable->Refresh(tPos, false));
}

void clsEntryInfoMng::ReduceEliminatePriority(EntryInfo_t *ptInfo)
{
	Assert(ptInfo != NULL && ptInfo->ptEntityInfo != NULL);
	PLogPos_t tPos;
	tPos.iEntityID = ptInfo->ptEntityInfo->iEntityID;
	tPos.iEntry = ptInfo->iEntry;
	Assert(m_poEntryTable->Refresh(tPos));
}

void clsEntryInfoMng::RemoveCatchUpFlag(EntryInfo_t *ptInfo)
{
	if (ptInfo->bCatchUpFlag)
	{
		ptInfo->bCatchUpFlag = false;

		if (m_iCatchUpFlagCnt > 0)
		{
			m_iCatchUpFlagCnt--;
		}
		else
		{
			CertainLogFatal("m_iCatchUpFlagCnt == 0");
		}
	}
}

bool clsEntryInfoMng::CheckIfCatchUpLimited(EntryInfo_t *ptInfo)
{
	if (ptInfo == NULL)
	{
		return (m_iCatchUpFlagCnt * m_poConf->GetEntityWorkerNum() > m_poConf->GetMaxCatchUpConcurr());
	}

	if (ptInfo->bCatchUpFlag)
	{
		return false;
	}

	if (m_iCatchUpFlagCnt * m_poConf->GetEntityWorkerNum() > m_poConf->GetMaxCatchUpConcurr())
	{
		CertainLogError("E(%lu, %lu)", ptInfo->ptEntityInfo->iEntityID, ptInfo->iEntry);
		return true;
	}

	ptInfo->bCatchUpFlag = true;
	m_iCatchUpFlagCnt++;

	return false;
}

} // namespace Certain


