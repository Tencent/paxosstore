#include "Command.h"
#include "Configure.h"

#include "network/EpollIO.h"
#include "network/IOChannel.h"

#include "SimpleCmd.h"

using namespace Certain;

volatile uint64_t iUUIDGenerator = 0;

class clsStatInfoHelper
{
private:
	string m_strTag;

    struct StatInfo_t
    {
        volatile uint64_t iMaxUseTimeUS;
        volatile uint64_t iTotalUseTimeUS;
        volatile uint64_t iCount;
    };

    map<string, StatInfo_t> m_tStatInfoMap;

    Certain::clsMutex m_oMutex;
    uint64_t m_iNextPrintTimeMS;

public:

	clsStatInfoHelper(string strTag, vector<string> vecName)
	{
		m_strTag = strTag;
        for (uint32_t i = 0; i < vecName.size(); ++i)
        {
            StatInfo_t tInfo = { 0 };
            m_tStatInfoMap[vecName[i]] = tInfo;
        }
        m_iNextPrintTimeMS = Certain::GetCurrTimeMS() + 1000;
	}

	virtual ~clsStatInfoHelper() { }

	void Update(string strName, uint64_t iCount, uint64_t iUseTimeUS)
	{
        StatInfo_t &tInfo = m_tStatInfoMap[strName];

		if (tInfo.iMaxUseTimeUS < iUseTimeUS)
		{
			tInfo.iMaxUseTimeUS = iUseTimeUS;
		}

		__sync_fetch_and_add(&tInfo.iTotalUseTimeUS, iUseTimeUS);
		__sync_fetch_and_add(&tInfo.iCount, 1);

        CheckPrint();
	}

	void CheckPrint()
	{
        map<string, StatInfo_t> tTemp;
        {
            Certain::clsThreadLock oLock(&m_oMutex);
            uint64_t iCurrTimeMS = Certain::GetCurrTimeMS();
            if (iCurrTimeMS <= m_iNextPrintTimeMS)
            {
                return;
            }
            m_iNextPrintTimeMS = iCurrTimeMS + 1000;

            tTemp = m_tStatInfoMap;
            for (auto iter = m_tStatInfoMap.begin(); iter != m_tStatInfoMap.end(); ++iter)
            {
                StatInfo_t &tInfo = tTemp[iter->first];
                tInfo.iTotalUseTimeUS = __sync_fetch_and_and(&(iter->second.iTotalUseTimeUS), 0);
                tInfo.iCount = __sync_fetch_and_and(&(iter->second.iCount), 0);
                tInfo.iMaxUseTimeUS = __sync_fetch_and_and(&(iter->second.iMaxUseTimeUS), 0);
            }
        }

        OnPrint(tTemp);
	}

    virtual void OnPrint(map<string, StatInfo_t> tTemp)
    {
        uint32_t iLen = 256;
        char acBuffer[iLen];

        sprintf(acBuffer, "certain_stat %s", m_strTag.c_str());
        string strResult = acBuffer;

        for (auto iter = tTemp.begin(); iter != tTemp.end(); ++iter)
        {
            string name = iter->first;
            StatInfo_t &tInfo = iter->second;

            if (tInfo.iCount == 0)
            {
                sprintf(acBuffer, " [%s cnt 0]", name.c_str());
            }
            else
            {
                sprintf(acBuffer, " [%s max_us %lu avg_us %lu cnt %lu]",
                        name.c_str(), tInfo.iMaxUseTimeUS,
                        tInfo.iTotalUseTimeUS / tInfo.iCount, tInfo.iCount);
            }
            strResult += acBuffer;
        }

        if (tTemp.find("all") != tTemp.end() && tTemp.find("failed") != tTemp.end())
        {
            StatInfo_t &tAll = tTemp["all"];
            StatInfo_t &tFailed = tTemp["failed"];
            if (tAll.iCount > 0)
            {
                sprintf(acBuffer, " {fail_rt %.5lf%% qps}", double(tFailed.iCount) / tAll.iCount * 100);
                strResult += acBuffer;
            }
        }

        printf("%s\n", strResult.c_str());
    }
};

class clsStopHelper
{
    private:
        string m_strTag;
        uint64_t m_iCount;
        uint64_t m_iLimitCount;

    public:
        clsStopHelper(string strTag, uint64_t iLimitCount)
        {
            m_strTag = strTag;
            m_iLimitCount = iLimitCount;
        }
        ~clsStopHelper() { }

        void AddCount(uint64_t iDelta)
        {
            uint64_t iCnt = __sync_add_and_fetch(&m_iCount, iDelta);
            if (iCnt >= m_iLimitCount)
            {
                printf("%s iCnt %lu\n", m_strTag.c_str(), iCnt);
            }
        }

        bool CheckIfStop()
        {
            return m_iCount >= m_iLimitCount;
        }
};

clsCmdBase *CreateRandomGetCmd(uint32_t iKeyRange)
{
	uint64_t iUUID = __sync_add_and_fetch(&iUUIDGenerator, 1);

	// gen key
	char pcBuffer[20];
	sprintf(pcBuffer, "%lu", iUUID % iKeyRange);
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
	sprintf(pcBuffer, "%lu", iUUID % iKeyRange);
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
	vector<uint64_t> m_vecLastTimeUS;

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
		m_vecLastTimeUS.assign(iChannelNum, (uint64_t)1 << 60);

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
		lp->hMagicNum = htons(RP_MAGIC_NUM);
		lp->hReserve = 0;
		lp->hCmdID = htons(poCmd->GetCmdID());
		lp->iLen = htonl(iBytes);
		lp->iCheckSum = CRC32(lp->pcData, iBytes);

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
			poChannel->AppendReadBytes(m_pcIOBuffer, iRet);
			return 1;
		}

		iRet = oResult.ParseFromArray(lp->pcData, lp->iLen);
		AssertEqual(iRet, 0);

		return 0;
	}

    int HandleRead(clsFDBase* poFD)
    {
        int iRet;
		clsIOChannel *poChannel = dynamic_cast<clsIOChannel *>(poFD);

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
                - poChannel->GetTimestampUS();

            vector<string> vecName; vecName.push_back("all"); vecName.push_back("failed");
            static clsStatInfoHelper oStat("stress", vecName);

            oStat.Update("all", 1, iUseTime * 1000);
            if (oResult.GetResult() != 0)
            {
                printf("use_time %lu fd %d ret_cmd: %s res %d\n",
                        iUseTime, poChannel->GetFD(),
                        oResult.GetTextCmd().c_str(),
                        oResult.GetResult());
                oStat.Update("failed", 1, 0);
            }
        }

        if (poChannel->IsBroken())
        {
            return 1;
        }

        return 0;
    }

	int HandleWrite(clsFDBase *poFD)
	{
		int iRet;

		clsIOChannel *poChannel = dynamic_cast<clsIOChannel *>(poFD);
        iRet = poChannel->Write(m_pcIOBuffer, 0);
        if (iRet < 0)
        {
            CertainLogError("poChannel->Write fd %d, ret %d", poFD->GetFD(), iRet);
        }

        if (poChannel->IsBroken())
        {
            return 1;
        }

        return 0;
	}

	clsCmdBase *CreateRandomCmd()
	{
		m_iRandomSeed++;

		//if (m_iRandomSeed % 2 == 0)
		//{
		//	return CreateRandomGetCmd(m_iKeyRange);
		//}
		//else
		{
			return CreateRandomSetCmd(m_iKeyRange, m_iValueLen);
		}
	}

	void PutCmdToChannel(clsIOChannel *poChannel)
	{
		poChannel->SetTimestampUS(GetCurrTimeUS());

		clsCmdBase *poCmd = CreateRandomCmd();
		AssertNotEqual(poCmd, NULL);
		clsAutoDelete<clsCmdBase> oAuto(poCmd);

		int iRet = SerializeToBuffer(poCmd);
		AssertLess(0, iRet);

		CertainLogDebug("send cmd %u", poCmd->GetCmdID());
		poChannel->AppendWriteBytes(m_pcIOBuffer, iRet);

		HandleWrite(poChannel);
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
					m_vecLastTimeUS[i] = m_vecChannel[i]->GetTimestampUS();
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

					uint64_t iCurrTimeUS = GetCurrTimeUS();
					if (iCurrTimeUS > m_vecLastTimeUS[i] + 1000 * 1000)
					{
						CertainLogError("timeout conn %s",
								tConn.ToString().c_str());

						m_vecLastTimeUS[i] = iCurrTimeUS;
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
