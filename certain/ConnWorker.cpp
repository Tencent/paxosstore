
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "ConnWorker.h"
#include "IOWorker.h"

namespace Certain
{

int clsListenHandler::HandleRead(clsFDBase *poFD)
{
	return m_poConnWorker->HandleListen(
			dynamic_cast<clsListenContext *>(poFD));
}

int clsListenHandler::HandleWrite(clsFDBase *poFD)
{
	CERTAIN_EPOLLIO_UNREACHABLE;
}

int clsNegoHandler::HandleRead(clsFDBase *poFD)
{
	return m_poConnWorker->HandleNego(
			dynamic_cast<clsNegoContext *>(poFD));
}

int clsNegoHandler::HandleWrite(clsFDBase *poFD)
{
	CERTAIN_EPOLLIO_UNREACHABLE;
}

int clsConnInfoMng::Init(clsConfigure *poConf)
{
	m_poConf = poConf;

	m_iServerNum = m_poConf->GetServerNum();
	m_iLocalServerID = m_poConf->GetLocalServerID();

	Assert(m_tExtConnQueue.empty());

	Assert(m_vecIntConnQueue.size() == 0);
	m_vecIntConnQueue.resize(m_iServerNum);

	m_iIOScheduler = 0;

	return 0;
}

void clsConnInfoMng::Destroy()
{
	m_vecIntConnQueue.clear();

	while (!m_tExtConnQueue.empty())
	{
		m_tExtConnQueue.pop();
	}
}

int clsConnInfoMng::PutByOneThread(uint32_t iServerID,
		const ConnInfo_t &tConnInfo)
{
	if (iServerID == m_iLocalServerID || (iServerID >= m_iServerNum
			&& iServerID != INVALID_SERVER_ID))
	{
		CertainLogError("Unexpected server id %u", iServerID);
		return -1;
	}

	CertainLogDebug("iServerID %u conn %s",
			iServerID, tConnInfo.ToString().c_str());

	clsThreadLock oLock(&m_oMutex);

	if (iServerID == INVALID_SERVER_ID)
	{
		m_tExtConnQueue.push(tConnInfo);
	}
	else
	{
		AssertLess(iServerID, m_iServerNum);
		m_vecIntConnQueue[iServerID].push(tConnInfo);
	}

	return 0;
}

void clsConnInfoMng::RemoveInvalidConn()
{
	uint32_t iInvalidCnt = 0;
	uint32_t iConnNotToken = 0;

	for (uint32_t i = 0; i < m_vecIntConnQueue.size(); ++i)
	{
		while (!m_vecIntConnQueue[i].empty())
		{
			iConnNotToken++;
			ConnInfo_t tConnInfo = m_vecIntConnQueue[i].front();

			if (CheckFD(tConnInfo.iFD))
			{
				break;
			}

			iInvalidCnt++;
			CertainLogError("Invalid conn: %s", tConnInfo.ToString().c_str());

			int iRet = close(tConnInfo.iFD);
			if (iRet != 0)
			{
				CertainLogError("close fail fd %d errno %d",
						tConnInfo.iFD, errno);
			}
			m_vecIntConnQueue[i].pop();
		}
	}

	if (iInvalidCnt > 0)
	{
		CertainLogError("iInvalidCnt %u", iInvalidCnt);
	}
	else if (iConnNotToken > 0)
	{
		CertainLogImpt("iConnNotToken %u", iConnNotToken);
	}
}

int clsConnInfoMng::TakeByMultiThread(uint32_t iIOWorkerID,
		const vector< vector<clsIOChannel *> > vecIntChannel,
		ConnInfo_t &tConnInfo, uint32_t &iServerID)
{
	clsThreadLock oLock(&m_oMutex);

	RemoveInvalidConn();

	for (uint32_t i = 0; i < m_vecIntConnQueue.size(); ++i)
	{
		if (i == m_iLocalServerID)
		{
			Assert(m_vecIntConnQueue[i].empty());
			continue;
		}

		if (m_vecIntConnQueue[i].empty())
		{
			continue;
		}

		// Use min first to make every IOWorker has equal IO channels.

		int32_t iMinChannelCnt = INT32_MAX;
		for (uint32_t iWorkerID = 0; iWorkerID < m_poConf->GetIOWorkerNum(); ++iWorkerID)
		{
			int32_t iTemp = clsIOWorkerRouter::GetInstance()->GetAndAddIntChannelCnt(iWorkerID, i, 0);
			if (iMinChannelCnt > iTemp)
			{
				iMinChannelCnt = iTemp;
			}
		}

		AssertNotMore(iMinChannelCnt, vecIntChannel[i].size());
		if (vecIntChannel[i].size() == uint32_t(iMinChannelCnt))
		{
			tConnInfo = m_vecIntConnQueue[i].front();
			m_vecIntConnQueue[i].pop();

			iServerID = i;

			return 0;
		}
	}

	if (iIOWorkerID != m_iIOScheduler)
	{
		return -1;
	}
	m_iIOScheduler = (m_iIOScheduler + 1) % m_poConf->GetIOWorkerNum();

	if (!m_tExtConnQueue.empty())
	{
		tConnInfo = m_tExtConnQueue.front();
		m_tExtConnQueue.pop();

		iServerID = INVALID_SERVER_ID;

		return 0;
	}

	return -2;
}

int clsConnWorker::AddListen(bool bInternal, const InetAddr_t &tLocalAddr)
{
	int iRet;
	int iBacklog = 8096;

	int iFD = CreateSocketFD(&tLocalAddr, true);
	if (iFD < 0)
	{
		CertainLogError("CreateSocketFD ret %d", iFD);
		return -1;
	}

	iRet = listen(iFD, iBacklog);
	if (iRet == -1)
	{
		CertainLogError("listen ret - 1 errno %d", errno);
		return -2;
	}

	CertainLogInfo("Start listen addr %s fd %d",
			tLocalAddr.ToString().c_str(), iFD);

	clsListenContext *poContext = new clsListenContext(
			iFD, m_poListenHandler, tLocalAddr, bInternal);

	iRet = m_poEpollIO->Add(poContext);
	if (iRet != 0)
	{
		CertainLogError("m_poEpollIO->Add ret %d", iRet);
		AssertEqual(close(iFD), 0);
		delete poContext, poContext = NULL;

		return -3;
	}

	return 0;
}

int clsConnWorker::RecvNegoMsg(clsNegoContext *poNegoCtx)
{
	int iRet;

	const ConnInfo_t &tConnInfo = poNegoCtx->GetConnInfo();
	int iFD = tConnInfo.iFD;

	uint8_t acNego[3];

	while (1)
	{
		iRet = read(iFD, acNego, 3);
		if (iRet == -1)
		{
			if (errno == EAGAIN || errno == EINTR)
			{
				continue;
			}

			CertainLogError("conn %s errno %d",
					tConnInfo.ToString().c_str(), errno);
			return -1;
		}
		else if (iRet == 0)
		{
			CertainLogError("conn %s closed by peer",
					tConnInfo.ToString().c_str());
			return -2;
		}
		else if (iRet < 3)
		{
			CertainLogError("simple close conn %s",
					tConnInfo.ToString().c_str());
			return -3;
		}

		AssertEqual(iRet, 3);
		break;
	}

	uint16_t hMagicNum = ntohs(*(uint16_t *)acNego);
	if (hMagicNum != RP_MAGIC_NUM)
	{
		CertainLogFatal("BUG conn %s no RP_MAGIC_NUM found",
				tConnInfo.ToString().c_str());
		return -4;
	}

	uint32_t iServerID = uint32_t(acNego[2]);
	poNegoCtx->SetServerID(iServerID);

	CertainLogDebug("iServerID %u conn %s",
			iServerID, tConnInfo.ToString().c_str());

	return 0;
}

int clsConnWorker::AcceptOneFD(clsListenContext *poContext)
{
	int iRet;
	int iFD, iListenFD = poContext->GetFD();
	ConnInfo_t tConnInfo;

	struct sockaddr_in tSockAddr;
	socklen_t tLen = sizeof(tSockAddr);

	while (1)
	{
		iFD = accept(iListenFD, (struct sockaddr *)(&tSockAddr), &tLen);
		if (iFD == -1)
		{
			if (errno == EINTR)
			{
				continue;
			}
			else if (errno == EAGAIN)
			{
				return 1;
			}

			CertainLogError("accept ret -1 errno %d", errno);
			return -1;
		}
		break;
	}

	AssertEqual(SetNonBlock(iFD, true), 0);

	tConnInfo.tLocalAddr = poContext->GetLocalAddr();
	tConnInfo.tPeerAddr = InetAddr_t(tSockAddr);
	tConnInfo.iFD = iFD;

	CertainLogInfo("accept conn %s", tConnInfo.ToString().c_str());

	if (poContext->IsInternal())
	{
		clsNegoContext *poNegoCtx = new clsNegoContext(m_poNegoHandler,
				tConnInfo);
		iRet = m_poEpollIO->Add(poNegoCtx);
		if (iRet != 0)
		{
			CertainLogError("m_poEpollIO->Add ret %d", iRet);
			AssertEqual(close(tConnInfo.iFD), 0);
			delete poNegoCtx, poNegoCtx = NULL;
		}
	}
	else
	{
		iRet = clsConnInfoMng::GetInstance()->PutByOneThread(
				INVALID_SERVER_ID, tConnInfo);
		if (iRet != 0)
		{
			CertainLogError("clsConnInfoMng PutByOneThread ret %d", iRet);

			AssertEqual(close(tConnInfo.iFD), 0);
		}
	}

	return 0;
}

int clsConnWorker::HandleListen(clsListenContext *poContext)
{
	while (1)
	{
		int iRet = AcceptOneFD(poContext);
		if (iRet == 0)
		{
			continue;
		}
		else if (iRet < 0)
		{
			// Relisten when the fd is invalid, Check if Bug.

			clsAutoDelete<clsListenContext> oAuto(poContext);
			CertainLogFatal("AcceptOnce ret %d", iRet);

			int iFD = poContext->GetFD();
			iRet = close(iFD);
			AssertEqual(iRet, 0);

			iRet = AddListen(poContext->IsInternal(),
					poContext->GetLocalAddr());
			AssertEqual(iRet, 0);

			return -1;
		}

		AssertEqual(iRet, 1);
		break;
	}

	return 0;
}

int clsConnWorker::HandleNego(clsNegoContext *poNegoCtx)
{
	int iRet;
	clsAutoDelete<clsNegoContext> oAuto(poNegoCtx);

	iRet = RecvNegoMsg(poNegoCtx);
	if (iRet != 0)
	{
		CertainLogError("RecvNegoMsg ret %d", iRet);
		m_poEpollIO->RemoveAndCloseFD(dynamic_cast<clsFDBase *>(poNegoCtx));
		return -1;
	}

	m_poEpollIO->Remove(dynamic_cast<clsFDBase *>(poNegoCtx));

	// For check only.
	ConnInfo_t tConnInfo = poNegoCtx->GetConnInfo();
	bool bInnerServer = false;
	vector<InetAddr_t> vecAddr = m_poConf->GetServerAddrs();
	for (uint32_t i = 0; i < vecAddr.size(); ++i)
	{
		if (vecAddr[i].GetNetOrderIP()
				== tConnInfo.tPeerAddr.GetNetOrderIP())
		{
            if (i != poNegoCtx->GetServerID())
            {
                CertainLogFatal("%u -> %u Check if replace machine conn: %s",
                        poNegoCtx->GetServerID(), i, tConnInfo.ToString().c_str());
                poNegoCtx->SetServerID(i);
            }
			bInnerServer = true;
		}
	}
	if (!bInnerServer)
	{
		CertainLogFatal("conn %s not from Inner Servers, check it",
				tConnInfo.ToString().c_str());
		close(poNegoCtx->GetFD());
		return -2;
	}

	CertainLogInfo("srvid %u conn %s from Inner Servers",
			poNegoCtx->GetServerID(), tConnInfo.ToString().c_str());

	iRet = clsConnInfoMng::GetInstance()->PutByOneThread(
			poNegoCtx->GetServerID(), poNegoCtx->GetConnInfo());
	if (iRet != 0)
	{
		CertainLogError("clsConnInfoMng PutByOneThread ret %d", iRet);
		close(poNegoCtx->GetFD());
	}

	return 0;
}

int clsConnWorker::AddAllListen()
{
	int iRet;

	uint32_t iLocalServerID = m_poConf->GetLocalServerID();
	vector<InetAddr_t> vecServerAddr = m_poConf->GetServerAddrs();
	AssertLess(iLocalServerID, vecServerAddr.size());

	InetAddr_t tInternal = vecServerAddr[iLocalServerID];

	iRet = AddListen(true, tInternal);
	if (iRet != 0)
	{
		CertainLogError("AddListen ret %d", iRet);
		return -1;
	}

#if CERTAIN_MAKE_FOR_KVSVR == 0
	InetAddr_t tExteranl = m_poConf->GetExtAddr();

	iRet = AddListen(false, tExteranl);
	if (iRet != 0)
	{
		CertainLogError("AddListen ret %d", iRet);
		return -2;
	}
#endif

	return 0;
}

clsConnWorker::clsConnWorker(clsConfigure *poConf)
{
	m_poConf = poConf;
	m_poEpollIO = new clsEpollIO;
	m_poListenHandler = new clsListenHandler(this);
	m_poNegoHandler = new clsNegoHandler(this);
}

clsConnWorker::~clsConnWorker()
{
	delete m_poNegoHandler, m_poNegoHandler = NULL;
	delete m_poListenHandler, m_poListenHandler = NULL;
	delete m_poEpollIO, m_poEpollIO = NULL;
}

void clsConnWorker::Run()
{
	uint32_t iLocalServerID = m_poConf->GetLocalServerID();
	SetThreadTitle("conn_%u", iLocalServerID);
	CertainLogInfo("conn_%u run", iLocalServerID);

	int iRet = AddAllListen();
	if (iRet != 0)
	{
		CertainLogFatal("AddAllListen ret %d", iRet);
		Assert(false);
	}

	while (1)
	{
		m_poEpollIO->RunOnce(1000);

		if (CheckIfExiting(0))
		{
			printf("conn_%u exit\n", iLocalServerID);
			CertainLogInfo("conn_%u exit", iLocalServerID);
			break;
		}
	}
}

} // namespace Certain


