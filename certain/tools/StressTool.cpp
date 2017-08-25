
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "Command.h"
#include "EpollIO.h"
#include "IOChannel.h"
#include "Configure.h"

using namespace Certain;

volatile uint64_t iUUIDGenerator = 0;

clsCmdBase *CreateRandomGetCmd(uint32_t iKeyRange)
{
	uint64_t iUUID = __sync_add_and_fetch(&iUUIDGenerator, 1);

	// gen key
	char pcBuffer[20];
	snprintf(pcBuffer, sizeof(pcBuffer), "%lu", iUUID % iKeyRange);
	string strKey = pcBuffer;
	while (strKey.size() < 8)
	{
		strKey.append("x");
	}

	clsSimpleCmd *poCmd = new clsSimpleCmd;
	poCmd->SetSubCmdID(clsSimpleCmd::kGet);
	poCmd->SetKey(strKey);
	poCmd->SetUUID(iUUID);
	AssertEqual(poCmd->GetCmdID(), kSimpleCmd);

	return poCmd;
}

clsCmdBase *CreateRandomSetCmd(uint32_t iKeyRange, uint32_t iValueLen)
{
	uint64_t iUUID = __sync_add_and_fetch(&iUUIDGenerator, 1);

	// gen key
	char pcBuffer[20];
	snprintf(pcBuffer, sizeof(pcBuffer), "%lu", iUUID % iKeyRange);
	string strKey = pcBuffer;
	while (strKey.size() < 8)
	{
		strKey.append("x");
	}

	// gen value
	string strValue(iValueLen, 'y');

	clsSimpleCmd *poCmd = new clsSimpleCmd;
	poCmd->SetSubCmdID(clsSimpleCmd::kSet);

	poCmd->SetKey(strKey);
	poCmd->SetValue(strValue);
	poCmd->SetUUID(iUUID);
	AssertEqual(poCmd->GetCmdID(), kSimpleCmd);

	return poCmd;
}

class clsStressTester : public clsIOHandlerBase,
						public clsThreadBase
{
private:
	uint32_t m_iTesterID;

	InetAddr_t m_tSrvAddr;
	clsEpollIO *m_poEpollIO;

	uint32_t m_iChannelNum;
	vector<clsIOChannel *> m_vecChannel;

	uint32_t m_iCmdNum;
	uint32_t m_iSentCmdNum;

	char *m_pcIOBuffer;
	uint32_t m_iIOBufferLen;

	uint32_t m_iKeyRange;
	uint32_t m_iRandomSeed;
	uint32_t m_iValueLen;

	vector<bool> m_vecSentable;
	vector<uint64_t> m_vecLastTimeMS;

public:
	clsStressTester(uint32_t iTesterID, const InetAddr_t &tSrvAddr,
			uint32_t iChannelNum, uint32_t iCmdNum, uint32_t iKeyRange,
			uint32_t iValueLen) : m_iTesterID(iTesterID),
		  						  m_tSrvAddr(tSrvAddr),
		  						  m_iChannelNum(iChannelNum),
		  						  m_iCmdNum(iCmdNum),
		  						  m_iSentCmdNum(0),
								  m_iKeyRange(iKeyRange),
		  						  m_iRandomSeed(0),
								  m_iValueLen(iValueLen)
	{
		m_iIOBufferLen = 32 * (1 << 10);
		m_pcIOBuffer = (char *)malloc(m_iIOBufferLen);
		AssertNotEqual(m_pcIOBuffer, NULL);

		m_vecSentable.assign(iChannelNum, true);
		m_vecLastTimeMS.assign(iChannelNum, (uint64_t)1 << 60);

		m_poEpollIO = new clsEpollIO;
	}

	~clsStressTester()
	{
		delete m_poEpollIO, m_poEpollIO = NULL;
		free(m_pcIOBuffer), m_pcIOBuffer = NULL;
	}

	int SerializeToBuffer(clsCmdBase *poCmd)
	{
		int iBytes = poCmd->SerializeToArray(m_pcIOBuffer + RP_HEADER_SIZE,
				m_iIOBufferLen - RP_HEADER_SIZE);
		if (iBytes < 0)
		{
			fprintf(stderr, "SerializeToArray ret %d\n", iBytes);
			return -1;
		}

		RawPacket_t *lp = (RawPacket_t *)m_pcIOBuffer;
		ConvertToNetOrder(lp);
		lp->iStartFlag = RP_STATR_FLAG;
		lp->hReserve = 0;
		lp->hCmdID = poCmd->GetCmdID();
		lp->iLen = iBytes;
		lp->iCheckSum = CRC32(lp->pcData, lp->iLen);

		return RP_HEADER_SIZE + iBytes;
	}

	clsIOChannel *MakeIOChannel()
	{
		ConnInfo_t tConnInfo = { 0 };
		int iRet = Connect(m_tSrvAddr, tConnInfo);
		AssertEqual(iRet, 0);

		clsIOChannel *poChannel = new clsIOChannel(this, tConnInfo,
				INVALID_ACCEPTOR_ID);

		return poChannel;
	}

	int ReadSingleResult(clsIOChannel *poChannel, clsSimpleCmd &oResult)
	{
		int iRet;
		RawPacket_t *lp = NULL;

		iRet = poChannel->Read(m_pcIOBuffer, m_iIOBufferLen);
		if (iRet < 0)
		{
			int iFD = poChannel->GetFD();
			CertainLogError("poChannel->Read fd %d, ret %d", iFD, iRet);
			return -1;
		}

		AssertNotMore(RP_HEADER_SIZE, iRet);
		lp = (RawPacket_t *)m_pcIOBuffer;
		ConvertToHostOrder(lp);

		AssertNotMore(iRet, lp->iLen + RP_HEADER_SIZE);
		if (uint32_t(iRet) < lp->iLen + RP_HEADER_SIZE)
		{
			poChannel->SetReadBytes(m_pcIOBuffer, iRet);
			return 1;
		}

		iRet = oResult.ParseFromArray(lp->pcData, lp->iLen);
		AssertEqual(iRet, 0);

		return 0;
	}

	void HandleIOEvent(clsFDBase *poFD)
	{
		clsIOChannel *poChannel = dynamic_cast<clsIOChannel *>(poFD);

		int iRet;
		int iFD = poFD->GetFD();

		bool bReadable = poChannel->IsReadable();
		if (bReadable)
		{
			clsSimpleCmd oResult;
			iRet = ReadSingleResult(poChannel, oResult);
			if (iRet < 0)
			{
				CertainLogError("ReadSingleResult ret %d", iRet);
			}
			else if (iRet > 0)
			{
				AssertEqual(iRet, 1);
			}
			else
			{
				for (uint32_t i = 0; i < m_vecChannel.size(); ++i)
				{
					if (m_vecChannel[i] == poChannel)
					{
						m_vecSentable[i] = true;
					}
				}
				uint64_t iUseTime = GetCurrTimeMS()
										- poChannel->GetTimestamp();

				printf("use_time %lu fd %d ret_cmd: %s res %d\n",
						iUseTime, poChannel->GetFD(),
						oResult.GetTextCmd().c_str(),
						oResult.GetResult());
			}
		}

		bool bWritable = poChannel->IsWritable();
		if (!poChannel->IsBroken() && bWritable)
		{
			iRet = poChannel->Write(m_pcIOBuffer, 0);
			if (iRet < 0)
			{
				CertainLogError("poChannel->Write fd %d, ret %d", iFD, iRet);
			}
		}

		bool bBroken = poChannel->IsBroken();

		CertainLogDebug("readable %u writable %u fd %d broken %u conn %s",
				bReadable, bWritable, iFD, bBroken,
				poChannel->GetConnInfo().ToString().c_str());

		if (bBroken)
		{
			CertainLogError("readable %u writable %u fd %d broken %u",
					bReadable, bWritable, iFD, bBroken);
			m_poEpollIO->RemoveAndCloseFD(poChannel);
		}
	}

	clsCmdBase *CreateRandomCmd()
	{
		m_iRandomSeed++;

		if (m_iRandomSeed % 2 == 0)
		{
			return CreateRandomGetCmd(m_iKeyRange);
		}
		else
		{
			return CreateRandomSetCmd(m_iKeyRange, m_iValueLen);
		}
	}

	void PutCmdToChannel(clsIOChannel *poChannel)
	{
		poChannel->SetTimestamp(GetCurrTimeMS());

		clsCmdBase *poCmd = CreateRandomCmd();
		AssertNotEqual(poCmd, NULL);
		clsAutoDelete<clsCmdBase> oAuto(poCmd);

		int iRet = SerializeToBuffer(poCmd);
		AssertLess(0, iRet);

		CertainLogDebug("send cmd %u", poCmd->GetCmdID());
		poChannel->AppendWriteBytes(m_pcIOBuffer, iRet);

		HandleIOEvent(poChannel);
	}

	void Run()
	{
		int iRet;

		for (uint32_t i = 0; i < m_iChannelNum; ++i)
		{
			clsIOChannel *poChannel = MakeIOChannel();
			m_vecChannel.push_back(poChannel);

			iRet = m_poEpollIO->Add(poChannel);
			AssertEqual(iRet, 0);
		}

		while (1)
		{
			for (uint32_t i = 0; i < m_vecChannel.size(); ++i)
			{
				if (!m_vecSentable[i])
				{
					continue;
				}

				if (m_iSentCmdNum < m_iCmdNum)
				{
					m_iSentCmdNum++;
					PutCmdToChannel(m_vecChannel[i]);
					m_vecSentable[i] = false;
					m_vecLastTimeMS[i] = m_vecChannel[i]->GetTimestamp();
				}
			}

			m_poEpollIO->RunOnce(1);

			if (m_iSentCmdNum == m_iCmdNum)
			{
				bool bRecvAll = true;
				InetAddr_t tPeerAddr, tLocalAddr;

				for (uint32_t i = 0; i < m_vecChannel.size(); ++i)
				{
					if (m_vecSentable[i])
					{
						continue;
					}

					bRecvAll = false;
					const ConnInfo_t &tConn = m_vecChannel[i]->GetConnInfo();

					uint64_t iCurrTimeMS = GetCurrTimeMS();
					if (iCurrTimeMS > m_vecLastTimeMS[i] + 1000)
					{
						CertainLogError("timeout conn %s",
								tConn.ToString().c_str());

						m_vecLastTimeMS[i] = iCurrTimeMS;
					}
				}

				if (bRecvAll)
				{
					clsThreadBase::SetExiting();
					break;
				}
			}
		}

		fprintf(stderr, "tester_id %u\n", m_iTesterID);
	}
};

int main(int argc, char *argv[])
{
	if (argc != 8)
	{
		printf("%s srv_IP srv_Port thread_num channel_num cmd_num key_range value_len\n",
				argv[0]);
		exit(-1);
	}

	InetAddr_t tSrvAddr(argv[1], strtoull(argv[2], 0, 10));

	uint32_t iThreadNum = strtoull(argv[3], 0, 10);
	uint32_t iChannelNum = strtoull(argv[4], 0, 10);
	uint32_t iCmdNum = strtoull(argv[5], 0, 10);
	uint32_t iKeyRange = strtoull(argv[6], 0, 10);
	uint32_t iValueLen = strtoull(argv[7], 0, 10);

	vector<clsStressTester *> vecTester;

	for (uint32_t i = 0; i < iThreadNum; ++i)
	{
		clsStressTester *poTester = new clsStressTester(i, tSrvAddr,
				iChannelNum, iCmdNum, iKeyRange, iValueLen);
		poTester->Start();
		vecTester.push_back(poTester);
	}

	uint32_t iExitedCnt = 0;
	while (iExitedCnt < iThreadNum)
	{
		for (uint32_t i = 0; i < iThreadNum; ++i)
		{
			clsStressTester *poTester = vecTester[i];
			if (poTester != NULL && poTester->IsExited())
			{
				delete poTester;
				vecTester[i] = NULL;
				iExitedCnt++;
			}
		}

		usleep(100);
	}

	return 0;
}
