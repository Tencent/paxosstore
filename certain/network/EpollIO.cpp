#include "EpollIO.h"

namespace Certain
{

clsEpollIO::clsEpollIO(uint32_t iEventSize)
{
	CertainLogDebug("iEventSize %u", iEventSize);

	m_iEventSize = iEventSize;
	m_ptEpollEv = (epoll_event *)calloc(iEventSize, sizeof(epoll_event));
	AssertNotEqual(m_ptEpollEv, NULL);

	m_iEpollFD = epoll_create(1024);
	if (m_iEpollFD == -1)
	{
		CertainLogError("epoll_create fail epoll_fd %d errno %d",
				m_iEpollFD, errno);
		Assert(false);
	}

	m_atUniqueFDPtrMap = (UniqueFDPtr_t *)calloc(MAX_FD_NUM,
			sizeof(UniqueFDPtr_t));
	AssertNotEqual(m_atUniqueFDPtrMap, NULL);
}

clsEpollIO::~clsEpollIO()
{
	for (int i = 0; i < MAX_FD_NUM; ++i)
	{
		if (m_atUniqueFDPtrMap[i].poFD != NULL)
		{
			clsFDBase *poFD = m_atUniqueFDPtrMap[i].poFD;
			AssertEqual(poFD->GetFDID(), m_atUniqueFDPtrMap[i].iFDID);

			RemoveAndCloseFD(poFD);

			m_atUniqueFDPtrMap[i].poFD = NULL;
			delete poFD, poFD = NULL;
		}
	}

	free(m_atUniqueFDPtrMap), m_atUniqueFDPtrMap = NULL;
	free(m_ptEpollEv), m_ptEpollEv = NULL;
}

int clsEpollIO::Add(clsFDBase *poFD)
{
	int iFD = poFD->GetFD();
	AssertLess(iFD, MAX_FD_NUM);
	CertainLogDebug("fd %d", iFD);

	epoll_event ev = { 0 };
	ev.events = poFD->GetEvents();
	ev.data.ptr = static_cast<void *>(poFD);

	int iRet = epoll_ctl(m_iEpollFD, EPOLL_CTL_ADD, iFD, &ev);
	if (iRet == -1)
	{
		CertainLogError("epoll_ctl fail fd %d errno %d", iFD, errno);
		return -1;
	}

	AssertEqual(iRet, 0);

	AssertEqual(m_atUniqueFDPtrMap[iFD].poFD, NULL);
	m_atUniqueFDPtrMap[iFD].poFD = poFD;
	m_atUniqueFDPtrMap[iFD].iFDID = poFD->GetFDID();

	return 0;
}

int clsEpollIO::Remove(clsFDBase *poFD)
{
	int iFD = poFD->GetFD();
	AssertLess(iFD, MAX_FD_NUM);
	CertainLogDebug("fd %d", iFD);

	int iRet = epoll_ctl(m_iEpollFD, EPOLL_CTL_DEL, iFD, NULL);
	if (iRet == -1)
	{
		CertainLogError("epoll_ctl fail fd %d errno %d", iFD, errno);
		return -1;
	}

	AssertEqual(iRet, 0);

	AssertNotEqual(m_atUniqueFDPtrMap[iFD].poFD, NULL);
	m_atUniqueFDPtrMap[iFD].poFD = NULL;

	return 0;
}

int clsEpollIO::RemoveAndCloseFD(clsFDBase *poFD)
{
	int iRet;
	int iFD = poFD->GetFD();

	iRet = Remove(poFD);
	if (iRet != 0)
	{
		CertainLogError("Remove ret %d", iRet);
		return -1;
	}

	iRet = close(iFD);
	if (iRet == -1)
	{
		CertainLogError("epoll_ctl fail fd %d errno %d", iFD, errno);
		return -2;
	}

	return 0;
}

int clsEpollIO::Modify(clsFDBase *poFD)
{
	int iFD = poFD->GetFD();
	AssertLess(iFD, MAX_FD_NUM);
	CertainLogDebug("fd %d", iFD);

	epoll_event ev = { 0 };
	ev.events = poFD->GetEvents();
	ev.data.ptr = static_cast<void *>(poFD);

	int iRet = epoll_ctl(m_iEpollFD, EPOLL_CTL_MOD, iFD, &ev);
	if (iRet == -1)
	{
		CertainLogError("epoll_ctl fail fd %d errno %d", iFD, errno);
		return -1;
	}

	AssertEqual(iRet, 0);
	AssertEqual(m_atUniqueFDPtrMap[iFD].poFD, poFD);

	return 0;
}

void clsEpollIO::RunOnce(int iTimeoutMS)
{
	int iNum, iRet;

	while (1)
	{
		iNum = epoll_wait(m_iEpollFD, m_ptEpollEv, m_iEventSize, iTimeoutMS);
		if (iNum == -1)
		{
			CertainLogError("epoll_wait fail epoll_fd %d errno %d",
					m_iEpollFD, errno);

			if (errno != EINTR)
			{
                // You should probably raise "open files" limit.
				AssertEqual(errno, 0);
                assert(false);
			}

			continue;
		}
		break;
	}

	for (int i = 0; i < iNum; ++i)
	{
		int iEvents = m_ptEpollEv[i].events;
		clsFDBase *poFD = static_cast<clsFDBase *>(m_ptEpollEv[i].data.ptr);
		clsIOHandlerBase *poHandler = poFD->GetIOHandler();
		Assert(poHandler != NULL);

		if ((iEvents & EPOLLIN)
				|| (iEvents & EPOLLERR)
				|| (iEvents & EPOLLHUP))
		{
			poFD->SetReadable(true);
			iRet = poHandler->HandleRead(poFD);
			if (iRet != 0)
			{
				continue;
			}
		}

		if (iEvents & EPOLLOUT)
		{
			poFD->SetWritable(true);
			iRet = poHandler->HandleWrite(poFD);
			if (iRet != 0)
			{
				continue;
			}
		}
	}
}

clsFDBase *clsEpollIO::GetFDBase(int iFD, uint32_t iFDID)
{
	if (m_atUniqueFDPtrMap[iFD].iFDID == iFDID)
	{
		return m_atUniqueFDPtrMap[iFD].poFD;
	}

	return NULL;
}

} // namespace Certain
