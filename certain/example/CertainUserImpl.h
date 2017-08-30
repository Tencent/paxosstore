#ifndef CERTAIN_EXAMPLE_SIMPLE_CertainUSERIMPL_H_
#define CERTAIN_EXAMPLE_SIMPLE_CertainUSERIMPL_H_

#include "Certain.h"
#include "SimpleCmd.h"

class clsCertainUserImpl : public Certain::clsCertainUserBase
{
private:
	Certain::clsConfigure *m_poConf;

public:
	clsCertainUserImpl() : m_poConf(NULL) { }
	~clsCertainUserImpl() { }

	virtual int GetLocalAcceptorID(uint64_t iEntityID,
			uint32_t &iLocalAcceptorID);

	virtual int GetServerID(uint64_t iEntityID,
			uint32_t iAcceptorID, uint32_t &iServerID);

	virtual int InitServerAddr(Certain::clsConfigure *poConf);
};

#endif
