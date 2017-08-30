#include "SimpleCmd.h"

namespace Certain
{

static int SerializeMsgToArray(const ::google::protobuf::Message &pbMsg,
		char *pcBuffer, uint32_t iLen)
{
	int32_t iRealLen = pbMsg.ByteSize();
	if (uint32_t(iRealLen) > iLen)
	{
		CertainLogError("SerializeToArray iRealLen %d iLen %u",
				iRealLen, iLen);
		return -1;
	}

	if (!pbMsg.SerializeToArray(pcBuffer, iRealLen))
	{
		CertainLogError("SerializeToArray fail");
		return -2;
	}

	return iRealLen;
}

string clsSimpleCmd::GetTextCmd()
{
	char acBuf[1024];
	snprintf(acBuf, 1024, "cmd %hu uuid %lu E(%lu, %lu) scmd %hu key %s val.size %lu val %s",
			m_hCmdID, m_iUUID, m_iEntityID, m_iEntry, m_hSubCmdID,
			m_strKey.c_str(), m_strValue.size(), m_strValue.c_str());
	return acBuf;
}

int clsSimpleCmd::ParseFromArray(const char *pcBuffer, uint32_t iLen)
{
	CertainPB::SimpleCmd oSimpleCmd;
	if (!oSimpleCmd.ParseFromArray(pcBuffer, iLen))
	{
		CertainLogError("ParseFromArray fail");
		return -1;
	}

	SetFromHeader(&oSimpleCmd.header());

	m_hSubCmdID = oSimpleCmd.sub_cmd_id();
	m_strKey = oSimpleCmd.key();
	m_strValue = oSimpleCmd.value();
	SetResult(oSimpleCmd.result());

	return 0;
}

int clsSimpleCmd::SerializeToArray(char *pcBuffer, uint32_t iLen)
{
	CertainPB::SimpleCmd oSimpleCmd;

	SetToHeader(oSimpleCmd.mutable_header());
	oSimpleCmd.set_sub_cmd_id(m_hSubCmdID);
	oSimpleCmd.set_key(m_strKey);
	oSimpleCmd.set_value(m_strValue);
	oSimpleCmd.set_result(m_iResult);

	return SerializeMsgToArray(oSimpleCmd, pcBuffer, iLen);
}

} // namespace Certain
