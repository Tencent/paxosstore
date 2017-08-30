#pragma once

#include "Common.h"
#include "Configure.h"
#include "Certain.pb.h"
#include "Command.h"

namespace Certain
{

class clsSimpleCmd : public clsClientCmd
{
private:
	string m_strKey;
	string m_strValue;

public:
	enum enumSCmdID
	{
		kGet = 1,
		kSet = 2,
	};

	clsSimpleCmd() : clsClientCmd(kSimpleCmd) { }

	virtual ~clsSimpleCmd() { }

	string GetKey() { return m_strKey; }
	void SetKey(string strKey) { m_strKey = strKey; }

	string GetValue() { return m_strValue; }
	void SetValue(string strValue) { m_strValue = strValue; }

	virtual string GetTextCmd();
	virtual int ParseFromArray(const char *pcBuffer, uint32_t iLen);
	virtual int SerializeToArray(char *pcBuffer, uint32_t iLen);

	virtual void CalcEntityID()
	{
		m_iEntityID = Hash(m_strKey);
	}
};

} // namespace Certain
