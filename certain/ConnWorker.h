
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_CONNWORKER_H_
#define CERTAIN_CONNWORKER_H_

#include "Common.h"
#include "Configure.h"

namespace Certain
{

class clsIOChannel;
class clsConnWorker;

class clsListenContext : public clsFDBase
{
private:
	InetAddr_t m_tLocalAddr;
	bool m_bInternal;

public:
	clsListenContext(int iFD,
					 clsIOHandlerBase *poIOHandler,
					 const InetAddr_t &tLocalAddr,
					 bool bInternal) :
			clsFDBase(iFD, poIOHandler, -1, (EPOLLIN | EPOLLET)),
			m_tLocalAddr(tLocalAddr),
			m_bInternal(bInternal) { }

	virtual ~clsListenContext() { }

	InetAddr_t GetLocalAddr() { return m_tLocalAddr; }
	BOOLEN_IS_SET(Internal);
};

class clsListenHandler : public clsIOHandlerBase
{
private:
	clsConnWorker *m_poConnWorker;

public:
	clsListenHandler(clsConnWorker *poConnWorker)
			: m_poConnWorker(poConnWorker) { }
	virtual ~clsListenHandler() { }

	virtual int HandleRead(clsFDBase *poFD);
	virtual int HandleWrite(clsFDBase *poFD);
};

class clsNegoContext : public clsFDBase
{
private:
	ConnInfo_t m_tConnInfo;
	uint32_t m_iServerID;

public:
	clsNegoContext(clsIOHandlerBase *poHandler, const ConnInfo_t &tConnInfo) :
			clsFDBase(tConnInfo.iFD, poHandler, -1, (EPOLLIN | EPOLLET)),
			m_tConnInfo(tConnInfo),
			m_iServerID(INVALID_SERVER_ID) { }

	virtual ~clsNegoContext() { }

	const ConnInfo_t &GetConnInfo() { return m_tConnInfo; }
	TYPE_GET_SET(uint32_t, ServerID, iServerID);
};

class clsNegoHandler : public clsIOHandlerBase
{
private:
	clsConnWorker *m_poConnWorker;

public:
	clsNegoHandler(clsConnWorker *poConnWorker)
			: m_poConnWorker(poConnWorker) { }
	virtual ~clsNegoHandler() { }

	virtual int HandleRead(clsFDBase *poFD);
	virtual int HandleWrite(clsFDBase *poFD);
};

class clsConnInfoMng : public clsSingleton<clsConnInfoMng>
{
private:
	clsConfigure *m_poConf;

	uint32_t m_iServerNum;
    uint32_t m_iLocalServerID;

	clsMutex m_oMutex;

	queue<ConnInfo_t> m_tExtConnQueue;
	vector< queue<ConnInfo_t> > m_vecIntConnQueue;

	// (TODO): remove and add clsIOScheduler
	uint32_t m_iIOScheduler;

	friend class clsSingleton<clsConnInfoMng>;
	clsConnInfoMng() { }

	void RemoveInvalidConn();

public:
	int Init(clsConfigure *poConf);
	void Destroy();

	int TakeByMultiThread(uint32_t iIOWorkerID,
			const vector< vector<clsIOChannel *> > vecIntChannel,
			ConnInfo_t &tConnInfo, uint32_t &iServerID);

	int PutByOneThread(uint32_t iServerID, const ConnInfo_t &tConnInfo);
};

class clsConnWorker : public clsThreadBase
{
private:
	clsConfigure *m_poConf;
	clsEpollIO *m_poEpollIO;

	clsListenHandler *m_poListenHandler;
	clsNegoHandler *m_poNegoHandler;

	int AddListen(bool bInternal, const InetAddr_t &tInetAddr);
	int AddAllListen();

	int RecvNegoMsg(clsNegoContext *poNegoCtx);
	int AcceptOneFD(clsListenContext *poContext);

public:
	virtual ~clsConnWorker();
	clsConnWorker(clsConfigure *poConf);

	int HandleListen(clsListenContext *poContext);
	int HandleNego(clsNegoContext *poNegoCtx);

	void Run();
};

} // namespace Certain

#endif
