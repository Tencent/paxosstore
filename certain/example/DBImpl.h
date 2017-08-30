#ifndef CERTAIN_EXAMPLE_SIMPLE_SIMPLEDB_H_
#define CERTAIN_EXAMPLE_SIMPLE_SIMPLEDB_H_

#include "Command.h"
#include "Certain.h"
#include "SimpleCmd.h"
#include "simple/KVEngine.h"

namespace Certain
{

class clsDBImpl : public clsDBBase
{
private:
	clsKVEngine *m_poKVEngine;

	int ExcuteGet(clsSimpleCmd *poCmd);

	int ExcuteSet(clsSimpleCmd *poCmd, string &strWriteBatch);

	void CheckMaxCommitedEntry(uint64_t iEntityID, uint64_t iEntry);

public:
	clsDBImpl(clsKVEngine *poKVEngine) : m_poKVEngine(poKVEngine) { }

	virtual ~clsDBImpl() { }

	virtual int ExcuteCmd(clsClientCmd *poClientCmd, string &strWriteBatch);

	virtual int Commit(uint64_t iEntityID, uint64_t iEntry,
			const string &strWriteBatch);

	virtual int LoadMaxCommitedEntry(uint64_t iEntityID,
			uint64_t &iCommitedEntry, uint32_t &iFlag);
};

} // namespace Certain

#endif
