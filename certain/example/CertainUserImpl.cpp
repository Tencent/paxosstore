#include "CertainUserImpl.h"

int clsCertainUserImpl::GetLocalAcceptorID(uint64_t iEntityID,
		uint32_t &iLocalAcceptorID)
{
	assert(m_poConf != NULL);
	iLocalAcceptorID = m_poConf->GetLocalServerID();
	return 0;
}

int clsCertainUserImpl::GetServerID(uint64_t iEntityID,
		uint32_t iAcceptorID, uint32_t &iServerID)
{
	assert(m_poConf != NULL);
	iServerID = iAcceptorID;
	return 0;
}

int clsCertainUserImpl::InitServerAddr(Certain::clsConfigure *poConf)
{
	m_poConf = poConf;
	poConf->SetServerNum(poConf->GetServerAddrs().size());
	return 0;
}
