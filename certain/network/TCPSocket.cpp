#include "TCPSocket.h"

namespace Certain
{

int CreateSocketFD(const InetAddr_t *ptInetAddr, bool bNonBlock)
{
	int iRet;

	int iFD = socket(AF_INET, SOCK_STREAM, 0);
	if (iFD == -1)
	{
		CertainLogError("socket ret -1 errno %d", errno);
		return -1;
	}

	int iOptVal = 1;
	iRet = setsockopt(iFD, SOL_SOCKET, SO_REUSEADDR,
			(char *)&iOptVal, sizeof(int));
	if (iRet == -1)
	{
		CertainLogError("setsockopt fail fd %d errno %d", iFD, errno);
		return -2;
	}

	// Close TCP negle algorithm.
	iOptVal = 1;
	iRet = setsockopt(iFD, IPPROTO_TCP, TCP_NODELAY,
			(char *)&iOptVal, sizeof(iOptVal));
	if (iRet == -1)
	{
		CertainLogError("setsockopt fail fd %d errno %d", iFD, errno);
		return -3;
	}

	if (bNonBlock)
	{
		int iFlags = fcntl(iFD, F_GETFL, 0);
		iRet = fcntl(iFD, F_SETFL, iFlags | O_NONBLOCK);
		AssertEqual(iRet, 0);
	}

	if (ptInetAddr == NULL)
	{
		return iFD;
	}

	struct sockaddr *ptAddr = (struct sockaddr *)&ptInetAddr->tAddr;
	socklen_t tLen = sizeof(*ptAddr);

	iRet = bind(iFD, ptAddr, tLen);
	if (iRet == -1)
	{
		CertainLogError("bind fail fd %d addr %s errno %d",
				iFD, ptInetAddr->ToString().c_str(), errno);
		return -4;
	}

	return iFD;
}

int Connect(int iFD, const InetAddr_t &tInetAddr)
{
	const struct sockaddr_in *ptAddr = &tInetAddr.tAddr;
	socklen_t tLen = sizeof(*ptAddr);

	int iRet = connect(iFD, (struct sockaddr*)ptAddr, tLen);
	if (iRet == -1)
	{
		if (errno == EINPROGRESS)
		{
			return 0;
		}

		CertainLogError("connect fail fd %d addr %s errno %d",
				iFD, tInetAddr.ToString().c_str(), errno);
		return -1;
	}

	return 0;
}

int Connect(const InetAddr_t &tInetAddr, ConnInfo_t &tConnInfo)
{
	int iRet;

	int iFD = CreateSocketFD(NULL, true);
	if (iFD == -1)
	{
		CertainLogError("CreateSocketFD ret %d", iFD);
		return -1;
	}

	iRet = Connect(iFD, tInetAddr);
	if (iRet != 0)
	{
		close(iFD);
		CertainLogError("Connect ret %d", iRet);
		return -2;
	}

	tConnInfo.iFD = iFD;
	tConnInfo.tPeerAddr = tInetAddr;

	struct sockaddr tAddr = { 0 };
	socklen_t tLen = sizeof(tAddr);

	iRet = getsockname(iFD, &tAddr, &tLen);
	if (iRet != 0)
	{
		close(iFD);
		CertainLogError("getsockname fail ret %d errno %d", iRet, errno);
		return -3;
	}

	if (tAddr.sa_family != AF_INET)
	{
		close(iFD);
		CertainLogError("not spported sa_family %d", tAddr.sa_family);
		return -4;
	}
	
	tConnInfo.tLocalAddr.tAddr = *((struct sockaddr_in *)&tAddr);

	return 0;
}

bool CheckFD(int iFD)
{
	int iError = 0;
	socklen_t iLen = sizeof(iError);

	int iRet = getsockopt(iFD, SOL_SOCKET, SO_ERROR, &iError, &iLen);
	AssertEqual(iRet, 0);

	return iError == 0;
}

int SetNonBlock(int iFD, bool bNonBlock)
{
	int iFlags = fcntl(iFD, F_GETFL, 0);
	if (iFlags == -1)
	{
		CertainLogError("fcntl fail errno %d", errno);
		return -1;
	}

	iFlags |= O_NONBLOCK;
	if (!bNonBlock)
	{
		iFlags ^= O_NONBLOCK;
	}

	int iRet = fcntl(iFD, F_SETFL, iFlags);
	if (iRet == -1)
	{
		CertainLogError("fcntl fail errno %d", errno);
		return -2;
	}

	return 0;
}

int MakeNonBlockPipe(int &iInFD, int &iOutFD)
{
	int iRet;

	int aiFD[2];
	iRet = pipe(aiFD);
	if (iRet == -1)
	{
		CertainLogError("pipe fail errno %d", errno);
		return -1;
	}

	iRet = SetNonBlock(aiFD[0]);
	if (iRet != 0)
	{
		close(aiFD[0]);
		close(aiFD[1]);

		CertainLogError("SetNonBlock ret %d fd %d", iRet, aiFD[0]);
		return -2;
	}

	iRet = SetNonBlock(aiFD[1]);
	if (iRet != 0)
	{
		close(aiFD[0]);
		close(aiFD[1]);

		CertainLogError("SetNonBlock ret %d fd %d", iRet, aiFD[1]);
		return -3;
	}

	iInFD = aiFD[0];
	iOutFD = aiFD[1];

	return 0;
}

} // namespace Certain
