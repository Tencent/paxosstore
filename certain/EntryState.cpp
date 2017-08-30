#include "EntryState.h"
#include "PerfLog.h"

namespace Certain
{

uint32_t clsEntryStateMachine::s_iAcceptorNum = 0;
uint32_t clsEntryStateMachine::s_iMajorityNum = 0;

uint32_t clsEntryStateMachine::GetAcceptorID(uint64_t iValueID)
{
	uint32_t iProposalNum = iValueID & 0xffffffff;
	AssertLess(0, iProposalNum);
	return (iProposalNum - 1) % s_iAcceptorNum;
}

int clsEntryStateMachine::Init(clsConfigure *poConf)
{
	s_iAcceptorNum = poConf->GetAcceptorNum();
	s_iMajorityNum = (s_iAcceptorNum >> 1) + 1;

	return 0;
}

const EntryRecord_t &clsEntryStateMachine::GetRecord(uint32_t iAcceptorID)
{
	return m_atRecord[iAcceptorID];
}

uint32_t clsEntryStateMachine::GetNextPreparedNum(uint32_t iLocalAcceptorID)
{
	if (m_iMaxPreparedNum == 0)
	{
		m_iMaxPreparedNum = iLocalAcceptorID + 1;
	}
	else
	{
		m_iMaxPreparedNum += s_iAcceptorNum;
	}

	return m_iMaxPreparedNum;
}

void clsEntryStateMachine::ResetAllCheckedEmpty()
{
	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		m_atRecord[i].bCheckedEmpty = false;
	}
}

void clsEntryStateMachine::SetCheckedEmpty(uint32_t iAcceptorID)
{
	m_atRecord[iAcceptorID].bCheckedEmpty = true;
}

bool clsEntryStateMachine::IsLocalEmpty()
{
	// (TODO)rock: use tla to check
	return m_iEntryState == kEntryStateNormal;
}

void clsEntryStateMachine::SetStoredValueID(uint32_t iLocalAcceptorID)
{
	EntryRecord_t &tLocalRecord = m_atRecord[iLocalAcceptorID];
	if (tLocalRecord.tValue.iValueID > 0)
	{
		tLocalRecord.iStoredValueID = tLocalRecord.tValue.iValueID;
	}
}

bool clsEntryStateMachine::IsReadOK()
{
	uint32_t iCount = 0;

	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		if (m_atRecord[i].bCheckedEmpty && m_atRecord[i].iPromisedNum == 0)
		{
			iCount++;
		}
	}

	CertainLogDebug("iCount %u", iCount);
	return iCount >= s_iMajorityNum;
}

uint32_t clsEntryStateMachine::CountAcceptedNum(uint32_t iAcceptedNum)
{
	uint32_t iCount = 0;

	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		if (m_atRecord[i].iAcceptedNum == iAcceptedNum)
		{
			iCount++;
		}
	}
	return iCount;
}

uint32_t clsEntryStateMachine::CountPromisedNum(uint32_t iPromisedNum)
{
	uint32_t iCount = 0;

	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		if (m_atRecord[i].iPromisedNum == iPromisedNum)
		{
			iCount++;
		}
	}
	return iCount;
}

bool clsEntryStateMachine::GetValueByAcceptedNum(uint32_t iAcceptedNum,
		PaxosValue_t &tValue)
{
	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		if (m_atRecord[i].iAcceptedNum == iAcceptedNum)
		{
			tValue = m_atRecord[i].tValue;
			return true;
		}
	}

	return false;
}

void clsEntryStateMachine::UpdateMostAcceptedNum(const EntryRecord_t &tRecord)
{
	if (m_iMostAcceptedNum == tRecord.iAcceptedNum)
	{
		m_iMostAcceptedNumCnt++;
		return;
	}

	uint32_t iCount = CountAcceptedNum(tRecord.iAcceptedNum);

	if (m_iMostAcceptedNumCnt < iCount)
	{
		m_iMostAcceptedNum = tRecord.iAcceptedNum;
		m_iMostAcceptedNumCnt = iCount;
	}
}

int clsEntryStateMachine::CalcEntryState(uint32_t iLocalAcceptorID)
{
	EntryRecord_t &tLocalRecord = m_atRecord[iLocalAcceptorID];

	m_iEntryState = kEntryStateNormal;

	if (tLocalRecord.bChosen)
	{
		m_iEntryState = kEntryStateChosen;
		return 0;
	}

	if (m_iMostAcceptedNumCnt >= s_iMajorityNum)
	{
		m_iEntryState = kEntryStateChosen;

		if (tLocalRecord.iAcceptedNum != m_iMostAcceptedNum)
		{
			if (!GetValueByAcceptedNum(m_iMostAcceptedNum, tLocalRecord.tValue))
			{
				return -1;
			}
			tLocalRecord.iAcceptedNum = m_iMostAcceptedNum;
		}

		tLocalRecord.bChosen = true;
		return 0;
	}

	if (tLocalRecord.iPromisedNum > tLocalRecord.iPreparedNum)
	{
		m_iEntryState = kEntryStatePromiseRemote;
		if (tLocalRecord.iAcceptedNum >= tLocalRecord.iPromisedNum)
		{
			m_iEntryState = kEntryStateAcceptRemote;
		}
		return 0;
	}

	if (tLocalRecord.iPromisedNum != tLocalRecord.iPreparedNum)
	{
		return -2;
	}

	// iPromisedNum == 0 means null.
	if (tLocalRecord.iPromisedNum > 0)
	{
		m_iEntryState = kEntryStatePromiseLocal;

		uint32_t iLocalPromisedNum = tLocalRecord.iPromisedNum;
		uint32_t iPromisedNumCnt = CountPromisedNum(iLocalPromisedNum);

		if (iPromisedNumCnt >= s_iMajorityNum)
		{
			m_iEntryState = kEntryStateMajorityPromise;
		}

		if (tLocalRecord.iAcceptedNum == tLocalRecord.iPromisedNum)
		{
			m_iEntryState = kEntryStateAcceptLocal;
		}
	}

	if (tLocalRecord.iAcceptedNum > tLocalRecord.iPromisedNum)
	{
		m_iEntryState = kEntryStateAcceptRemote;
	}

	return 0;
}

int clsEntryStateMachine::MakeRealRecord(EntryRecord_t &tRecord)
{
	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		EntryRecord_t &tRealRecord = m_atRecord[i];

		if (tRecord.tValue.iValueID != tRealRecord.tValue.iValueID)
		{
			continue;
		}

#if CERTAIN_DEBUG
		// For check only.
		if (tRecord.tValue.strValue.size() > 0)
		{
			if (tRecord.tValue.strValue != tRealRecord.tValue.strValue
					|| (tRecord.tValue.vecValueUUID.size() > 0
						&& tRealRecord.tValue.vecValueUUID.size() > 0
						&& tRecord.tValue.vecValueUUID
						!= tRealRecord.tValue.vecValueUUID))
			{
				CertainLogFatal("CRC32(%u, %u) BUG record: %s lrecord[%u]: %s",
						CRC32(tRecord.tValue.strValue),
						CRC32(tRealRecord.tValue.strValue),
						EntryRecordToString(tRecord).c_str(),
						i, EntryRecordToString(tRealRecord).c_str());

				clsPerfLog::GetInstance()->Flush();
				Assert(false);
			}
		}
#endif
		tRecord.tValue = tRealRecord.tValue;
		break;
	}

	if (tRecord.iAcceptedNum > 0)
	{
		if (tRecord.tValue.iValueID > 0)
		{
			return 0;
		}

		return -1;
	}

	return 1;
}

string clsEntryStateMachine::ToString()
{
	string strState;
	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		if (i > 0)
		{
			strState += " ";
		}
		EntryRecord_t &tRecord = m_atRecord[i];
		strState += EntryRecordToString(tRecord);
	}
	return strState;
}

int clsEntryStateMachine::Update(uint64_t iEntityID ,uint64_t iEntry,
		uint32_t iLocalAcceptorID, uint32_t iAcceptorID,
		const EntryRecord_t &tRecordMayWithValueIDOnly)
{
#if CERTAIN_DEBUG
	RETURN_RANDOM_ERROR_WHEN_IN_DEBUG_MODE();
#endif

	int iRet;

	if (m_iEntryState == kEntryStateChosen)
	{
		return -1;
	}

	if (iLocalAcceptorID >= s_iAcceptorNum || iAcceptorID >= s_iAcceptorNum)
	{
		return -2;
	}

	m_iEntryState = kEntryStateNormal;
	EntryRecord_t &tLocalRecord = m_atRecord[iLocalAcceptorID];
	EntryRecord_t &tRemoteRecord = m_atRecord[iAcceptorID];

	// Make record into real when it comes in machine state.
	EntryRecord_t tRecord = tRecordMayWithValueIDOnly;
	iRet = CheckEntryRecordMayWithVIDOnly(tRecord);
	if (iRet != 0)
	{
		CertainLogFatal("CheckEntryRecordMayWithVIDOnly ret %d", iRet);
		return -3;
	}

	iRet = MakeRealRecord(tRecord);
	if (iRet < 0)
	{
		CertainLogFatal("MakeRealRecord ret %d", iRet);
		return -4;
	}

	if (!tRecord.tValue.bHasValue && tRecord.tValue.iValueID > 0)
	{
		if (tLocalRecord.iAcceptedNum > tRecord.iAcceptedNum)
		{
			// The remote acceptor supposed that this acceptor know the V.
			// But the V stored in tLocalRecord has been overriden,

			// Ignore the accepted message.
			tRecord.iAcceptedNum = 0;
			tRecord.tValue.iValueID = 0;
		}
		else
		{
			return -5;
		}
	}

	iRet = CheckEntryRecord(tRecord);
	if (iRet != 0)
	{
		CertainLogFatal("CheckEntryRecord ret %d", iRet);
		return -6;
	}

	if (tRecord.bChosen)
	{
		// For check only.
		if (tRemoteRecord.iAcceptedNum >= tRecord.iAcceptedNum)
		{
			if (tRemoteRecord.tValue.iValueID != tRecord.tValue.iValueID)
			{
				return -7;
			}
		}

		tLocalRecord.iAcceptedNum = tRecord.iAcceptedNum;
		tLocalRecord.tValue = tRecord.tValue;
		tLocalRecord.bChosen = true;

		tRemoteRecord.iAcceptedNum = tRecord.iAcceptedNum;
		tRemoteRecord.tValue = tRecord.tValue;
		tRemoteRecord.bChosen = true;

		m_iEntryState = kEntryStateChosen;

		return m_iEntryState;
	}

	// 1. update m_iMaxPreparedNum
	uint32_t iGlobalMaxPreparedNum = max(
			tRecord.iPreparedNum, tRecord.iPromisedNum);
	if (m_iMaxPreparedNum < iGlobalMaxPreparedNum)
	{
		uint32_t iNextPreparedNum = m_iMaxPreparedNum;

		while (iNextPreparedNum <= iGlobalMaxPreparedNum)
		{
			m_iMaxPreparedNum = iNextPreparedNum;

			if (iNextPreparedNum == 0)
			{
				iNextPreparedNum = iLocalAcceptorID + 1;
			}
			else
			{
				iNextPreparedNum += s_iAcceptorNum;
			}
		}
	}

	// 2. update iPreparedNum
	if (tRemoteRecord.iPreparedNum < tRecord.iPreparedNum)
	{
		tRemoteRecord.iPreparedNum = tRecord.iPreparedNum;
	}

	// 3. update old remote record
	if (iAcceptorID != iLocalAcceptorID
			&& tRecord.iAcceptedNum > tRemoteRecord.iAcceptedNum)
	{
		tRemoteRecord.iAcceptedNum = tRecord.iAcceptedNum;
		if (tRemoteRecord.tValue.iValueID != tRecord.tValue.iValueID)
		{
			tRemoteRecord.tValue = tRecord.tValue;
		}

		UpdateMostAcceptedNum(tRecord);
	}

	// 4. update value for remote accept first when use PreAuth
	if (tRecord.iAcceptedNum == 0)
	{
		if (tRecord.tValue.iValueID > 0)
		{
			if (0 == tRecord.iPromisedNum)
			{
				return -8;
			}

			if (iLocalAcceptorID == iAcceptorID)
			{
				if (tLocalRecord.iAcceptedNum != 0 || tLocalRecord.tValue.iValueID != 0)
				{
					return -9;
				}
				tLocalRecord.tValue = tRecord.tValue;
			}
			else if (tRecord.iPromisedNum <= s_iAcceptorNum)
			{
				if (tRecord.iPromisedNum == 0 || tRecord.iPreparedNum != tRecord.iPromisedNum)
				{
					return -10;
				}
				tRecord.iAcceptedNum = tRecord.iPromisedNum;
			}
		}

		// store the newest status of remote record
		if (iLocalAcceptorID != iAcceptorID && tRemoteRecord.iAcceptedNum == 0)
		{
			if (tRemoteRecord.tValue.iValueID > 0 && tRecord.tValue.iValueID > 0
					&& (tRemoteRecord.tValue.iValueID != tRecord.tValue.iValueID
					|| tRemoteRecord.tValue.strValue != tRecord.tValue.strValue))
			{
				return -11;
			}
			tRemoteRecord.tValue = tRecord.tValue;
		}
	}

	// 5. update old local record
	if (tRecord.iAcceptedNum > tLocalRecord.iAcceptedNum
			&& tRecord.iAcceptedNum >= tLocalRecord.iPromisedNum)
	{
		tLocalRecord.iAcceptedNum = tRecord.iAcceptedNum;
		if (tLocalRecord.tValue.iValueID != tRecord.tValue.iValueID)
		{
			tLocalRecord.tValue = tRecord.tValue;
		}

		UpdateMostAcceptedNum(tRecord);
	}

	// 6. update iPromisedNum
	if (tRemoteRecord.iPromisedNum < tRecord.iPromisedNum)
	{
		tRemoteRecord.iPromisedNum = tRecord.iPromisedNum;
	}
	if (tLocalRecord.iPromisedNum < tRecord.iPromisedNum)
	{
		tLocalRecord.iPromisedNum = tRecord.iPromisedNum;
	}

	iRet = CalcEntryState(iLocalAcceptorID);
	if (iRet < 0)
	{
		CertainLogFatal("CalcEntryState ret %d", iRet);
		return -12;
	}

	// For check only.
	iRet = CheckEntryRecord(tRecord);
	if (iRet != 0)
	{
		CertainLogFatal("CheckEntryRecord ret %d", iRet);
		return -13;
	}

	if (tLocalRecord.iPreparedNum > 0)
	{
		if ((tLocalRecord.iPreparedNum - 1) % s_iAcceptorNum != iLocalAcceptorID)
		{
			return -14;
		}
	}
	if (tLocalRecord.iPromisedNum > tLocalRecord.iPreparedNum)
	{
		if ((tLocalRecord.iPromisedNum - 1) % s_iAcceptorNum == iLocalAcceptorID)
		{
			return -15;
		}
	}

	if (tLocalRecord.iPromisedNum == iLocalAcceptorID + 1
			&& tLocalRecord.iAcceptedNum == 0
			&& tLocalRecord.tValue.iValueID == 0)
	{
		return -16;
	}

	return m_iEntryState;
}

int clsEntryStateMachine::AcceptOnMajorityPromise(uint32_t iLocalAcceptorID,
		const PaxosValue_t &tValue, bool &bAcceptPreparedValue)
{
#if CERTAIN_DEBUG
	RETURN_RANDOM_ERROR_WHEN_IN_DEBUG_MODE();
#endif
	int iRet;

	bAcceptPreparedValue = false;
	if (m_iEntryState != kEntryStateMajorityPromise)
	{
		return -1;
	}
	EntryRecord_t &tLocalRecord = m_atRecord[iLocalAcceptorID];
	if (tLocalRecord.iPreparedNum != tLocalRecord.iPromisedNum
			|| tLocalRecord.iAcceptedNum >= tLocalRecord.iPromisedNum)
	{
		return -2;
	}

	for (uint32_t i = 0; i < s_iAcceptorNum; ++i)
	{
		if (tLocalRecord.iAcceptedNum < m_atRecord[i].iAcceptedNum)
		{
			tLocalRecord.iAcceptedNum = m_atRecord[i].iAcceptedNum;
			tLocalRecord.tValue = m_atRecord[i].tValue;
		}
	}

	if (tLocalRecord.iAcceptedNum == 0)
	{
		tLocalRecord.tValue = tValue;
		bAcceptPreparedValue = true;
	}

	if (tLocalRecord.iAcceptedNum > tLocalRecord.iPromisedNum)
	{
		return -3;
	}
	tLocalRecord.iAcceptedNum = tLocalRecord.iPromisedNum;

	iRet = CheckEntryRecord(tLocalRecord);
	if (iRet != 0)
	{
		CertainLogFatal("CheckEntryRecord ret %d", iRet);
		return -4;
	}

	iRet = CalcEntryState(iLocalAcceptorID);
	if (iRet < 0)
	{
		CertainLogFatal("CalcEntryState ret %d", iRet);
		return -5;
	}

	if (m_iEntryState != kEntryStateAcceptLocal)
	{
		return -6;
	}

	if ((tLocalRecord.iPromisedNum - 1) % s_iAcceptorNum != iLocalAcceptorID)
	{
		return -7;
	}

	return 0;
}

bool clsEntryStateMachine::IsRemoteCompeting()
{
	if (m_iEntryState == kEntryStatePromiseRemote
			|| m_iEntryState == kEntryStateAcceptRemote)
	{
		return true;
	}

	return false;
}

} // namespace Certain
