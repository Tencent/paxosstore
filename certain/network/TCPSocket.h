
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_TCPSOCKET_H_
#define CERTAIN_TCPSOCKET_H_

#include "utils/Assert.h"
#include "utils/CRC32.h"
#include "utils/Hash.h"
#include "utils/Time.h"
#include "utils/AutoHelper.h"

namespace Certain
{

// (TODO): support ipv6
struct InetAddr_t
{
	struct sockaddr_in tAddr;

	bool operator == (const InetAddr_t &tOther) const
	{
		const struct sockaddr_in &tOtherAddr = tOther.tAddr;

		if (tAddr.sin_addr.s_addr != tOtherAddr.sin_addr.s_addr)
		{
			return false;
		}

		if (tAddr.sin_port != tOtherAddr.sin_port)
		{
			return false;
		}

		return true;
	}

	bool operator < (const InetAddr_t &tOther) const
	{
		const struct sockaddr_in &tOtherAddr = tOther.tAddr;

		if (tAddr.sin_addr.s_addr != tOtherAddr.sin_addr.s_addr)
		{
			return tAddr.sin_addr.s_addr < tOtherAddr.sin_addr.s_addr;
		}

		if (tAddr.sin_port != tOtherAddr.sin_port)
		{
			return tAddr.sin_port < tOtherAddr.sin_port;
		}

		return true;
	}

	InetAddr_t()
	{
		memset(&tAddr, 0, sizeof(tAddr));
	}

	InetAddr_t(struct sockaddr_in tSockAddr)
	{
		tAddr = tSockAddr;
	}

	InetAddr_t(const char *sIP, uint16_t iPort)
	{
		memset(&tAddr, 0, sizeof(tAddr));
		tAddr.sin_family = AF_INET;
		tAddr.sin_addr.s_addr = inet_addr(sIP);
		tAddr.sin_port = htons(iPort);
	}

	InetAddr_t(uint32_t iIP, uint16_t iPort)
	{
		memset(&tAddr, 0, sizeof(tAddr));
		tAddr.sin_family = AF_INET;
		tAddr.sin_addr.s_addr = iIP;
		tAddr.sin_port = htons(iPort);
	}

	string ToString() const
	{
		const char *sIP = inet_ntoa(tAddr.sin_addr);
		uint16_t iPort = ntohs(tAddr.sin_port);

		char acBuffer[32];
		snprintf(acBuffer, 32, "%s:%hu:%u", sIP, iPort, tAddr.sin_addr.s_addr);

		return acBuffer;
	}

	uint32_t GetNetOrderIP()
	{
		return tAddr.sin_addr.s_addr;
	}
};

// (TODO): impl clsTCPSocket
struct ConnInfo_t
{
	int iFD;

	InetAddr_t tLocalAddr;
	InetAddr_t tPeerAddr;

	string ToString() const
	{
		char acBuffer[128];
		snprintf(acBuffer, 128, "fd %d local %s peer %s",
				iFD, tLocalAddr.ToString().c_str(),
				tPeerAddr.ToString().c_str());
		return acBuffer;
	}
};

int CreateSocketFD(const InetAddr_t *ptInetAddr, bool bNonBlock = false);
int Connect(int iFD, const InetAddr_t &tInetAddr);
int Connect(const InetAddr_t &tInetAddr, ConnInfo_t &tConnInfo);
bool CheckFD(int iFD);
int SetNonBlock(int iFD, bool bNonBlock = true);
int MakeNonBlockPipe(int &iInFD, int &iOutFD);

} // namespace Certain

#endif // CERTAIN_TCPSOCKET_H_
