
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_COMMAND_H_
#define CERTAIN_COMMAND_H_

#include "Common.h"
#include "Configure.h"
#include "Certain.pb.h"

namespace Certain
{

enum enumCmd
{
	kPaxosCmd = 1,
	kRecoverCmd,
	kSimpleCmd,
	kWriteBatchCmd,
};

enum enumPLogType
{
	kPLogTypeCmd = 0,
	kPLogTypeRes,
};

enum enumDBFlag
{
	kDBFlagCheckGetAll = 1,
};

enum enumRetCode
{
	eRetCodePtrReuse	= 1,
	eRetCodeOK 			= 0,

	eRetCodeNotFound 		= -7000,
	eRetCodeFailed			= -7001,
	eRetCodeReadFailed		= -7002,
	eRetCodeTimeout			= -7003,
	eRetCodeBusy			= -7004,
	eRetCodeTurnErr			= -7005,
	eRetCodeRouteErr		= -7006,
	eRetCodeQueueFull		= -7007,
	eRetCodeQueueFailed		= -7008,
	eRetCodeDupUUID			= -7009,
	eRetCodeNoIdlePipe		= -7010,
	eRetCodePipeWaitFailed	= -7011,
	eRetCodePipePtrErr		= -7012,
	eRetCodeLeaseReject		= -7013,
	eRetCodeRecovering		= -7014,
	eRetCodeEntryLimited	= -7015,
	eRetCodeEntityLimited	= -7016,
	eRetCodeCleanUp			= -7017,
	eRetCodeSizeExceed		= -7018,
	eRetCodeCatchUp			= -7019,
	eRetCodeMsgIngnored		= -7020,
	eRetCodeMajPromFailed	= -7021,
	eRetCodeOtherVChosen	= -7022,
	eRetCodeRejectAll		= -7023,
	eRetCodeReject			= -7024,
	eRetCodeStorageCorrupt	= -7025,
	eRetCodeEntityEvicted	= -7026,
	eRetCodeMemCacheLimited	= -7027,
	eRetCodeNoGroupIdlePipe	= -7028,

	eRetCodeDBSubmitErr		= -7100,
	eRetCodeDBCommitErr		= -7101,
	eRetCodeDBTurnErr		= -7102,
	eRetCodeDBLagBehind		= -7103,
	eRetCodeDBCommitLimited	= -7104,
	eRetCodeDBLoadErr		= -7105,
	eRetCodeDBPending		= -7106,
	eRetCodeDBStatusErr		= -7107,

	eRetCodePLogGetErr		= -7200,
	eRetCodePLogPutErr		= -7201,
	eRetCodePLogInCatchUp	= -7202,
	eRetCodePLogPending		= -7203,
	eRetCodePLogReject		= -7204,
	eRetCodeGetAllPending   = -7205,

	eRetCodeRemoteOut       = -7300,
    eRetCodeRandomDrop      = -7301,

	eRetCodeUnkown 			= -7999,
};

struct IOTracker_t
{
	int iFD;
	uint32_t iFDID;
	uint32_t iIOWorkerID;

	IOTracker_t() : iFD(-1),
					iFDID(-1),
					iIOWorkerID(-1) { }

	IOTracker_t(int iArgFD, int iArgFDID, int iArgIOWorkerID)
			: iFD(iArgFD),
			  iFDID(iArgFDID),
			  iIOWorkerID(iArgIOWorkerID) { }
};

class clsCmdBase
{
protected:
	uint64_t m_iUUID;
	uint16_t m_hCmdID;

	uint64_t m_iEntityID;
	uint64_t m_iEntry;

	IOTracker_t m_tIOTracker;

	uint64_t m_iTimestampUS;
	uint32_t m_iRspPkgCRC32;

	int m_iResult;

	bool m_bQuickRsp;

	void SetFromHeader(const CertainPB::CmdHeader *poHeader)
	{
		m_iUUID = poHeader->uuid();
		m_iEntityID = poHeader->entity_id();
		m_iEntry = poHeader->entry();
		m_bQuickRsp = poHeader->quick_rsp();
	}

	void SetToHeader(CertainPB::CmdHeader *poHeader)
	{
		poHeader->set_uuid(m_iUUID);
		poHeader->set_entity_id(m_iEntityID);
		poHeader->set_entry(m_iEntry);
		poHeader->set_quick_rsp(m_bQuickRsp);
	}

public:

	virtual string GetTextCmd() = 0;
	virtual int ParseFromArray(const char *pcBuffer, uint32_t iLen) = 0;
	virtual int SerializeToArray(char *pcBuffer, uint32_t iLen) = 0;

	virtual void CalcEntityID() { }

public:
	clsCmdBase(uint16_t hCmdID) : m_hCmdID(hCmdID)
	{
		m_iUUID = 0;

		m_iEntityID = -1;
		m_iEntry = -1;

		m_iTimestampUS = 0;
		m_iRspPkgCRC32 = 0;

		m_iResult = 0;

		m_bQuickRsp = false;
	}

	virtual ~clsCmdBase() { }

	uint16_t GetCmdID() { return m_hCmdID; }

	uint64_t GetUUID() { return m_iUUID; }
	void SetUUID(uint64_t iUUID) { m_iUUID = iUUID; }

	bool GetQuickRsp() { return m_bQuickRsp; }
	void SetQuickRsp( bool bQuickRsp ) { m_bQuickRsp = bQuickRsp; }


	TYPE_GET_SET(uint64_t, EntityID, iEntityID);
	TYPE_GET_SET(uint64_t, Entry, iEntry);

	IOTracker_t GetIOTracker()
	{
		return m_tIOTracker;
	}
	void SetIOTracker(const IOTracker_t tIOTracker)
	{
		m_tIOTracker = tIOTracker;
	}

	TYPE_GET_SET(uint64_t, TimestampUS, iTimestampUS);
	TYPE_GET_SET(uint32_t, RspPkgCRC32, iRspPkgCRC32);

	TYPE_GET_SET(int, Result, iResult);

	virtual int GenRspPkg(char *pcBuffer, uint32_t iLen, bool bCheckSum);
};

class clsPaxosCmd : public clsCmdBase
{
private:
	bool m_bCheckEmpty;
	bool m_bPLogError;
	bool m_bPLogReturn;
	bool m_bPLogLoad;

	bool m_bCheckHasMore;
	bool m_bHasMore;

	uint32_t m_iSrcAcceptorID;
	uint32_t m_iDestAcceptorID;

	EntryRecord_t m_tSrcRecord;
	EntryRecord_t m_tDestRecord;

	uint64_t m_iMaxChosenEntry;
	uint64_t m_iMaxPLogEntry;

	void Init(uint32_t iAcceptorID, uint64_t iEntityID, uint64_t iEntry,
			const EntryRecord_t *ptSrc, const EntryRecord_t *ptDest);

	void ConvertFromPB(EntryRecord_t &tEntryRecord,
			const CertainPB::EntryRecord *poRecord);

	void ConvertToPB(const EntryRecord_t &tEntryRecord,
			CertainPB::EntryRecord *poRecord, bool bCopyValue);

public:
	clsPaxosCmd() : clsCmdBase(kPaxosCmd)
	{
		Init(INVALID_ACCEPTOR_ID, 0, 0, NULL, NULL);
	}

	clsPaxosCmd(uint32_t iAcceptorID, uint64_t iEntityID, uint64_t iEntry,
			const EntryRecord_t *ptSrc, const EntryRecord_t *ptDest)
			: clsCmdBase(kPaxosCmd)
	{
		Init(iAcceptorID, iEntityID, iEntry, ptSrc, ptDest);
	}

	clsPaxosCmd(uint32_t iAcceptorID, uint64_t iEntityID, uint64_t iEntry)
			: clsCmdBase(kPaxosCmd)
	{
		Init(iAcceptorID, iEntityID, iEntry, NULL, NULL);
		m_bCheckEmpty = true; // For Read Opt.
	}

	virtual ~clsPaxosCmd() { }

	virtual string GetTextCmd();
	virtual int ParseFromArray(const char *pcBuffer, uint32_t iLen);
	virtual int SerializeToArray(char *pcBuffer, uint32_t iLen);

	uint32_t CalcSize()
	{
		return sizeof(clsPaxosCmd) + m_tSrcRecord.tValue.strValue.size() + m_tDestRecord.tValue.strValue.size();
	}

	BOOLEN_IS_SET(PLogError);
	BOOLEN_IS_SET(PLogReturn);
	BOOLEN_IS_SET(PLogLoad);
	BOOLEN_IS_SET(CheckHasMore);
	BOOLEN_IS_SET(HasMore);

	UINT32_GET_SET(SrcAcceptorID);
	UINT32_GET_SET(DestAcceptorID);

	TYPE_GET_SET(uint64_t, MaxChosenEntry, iMaxChosenEntry);
	TYPE_GET_SET(uint64_t, MaxPLogEntry, iMaxPLogEntry);

	const EntryRecord_t &GetSrcRecord()
	{
		return m_tSrcRecord;
	}
	void SetSrcRecord(const EntryRecord_t &tSrcRecord)
	{
		m_tSrcRecord = tSrcRecord;
	}

	const EntryRecord_t &GetDestRecord()
	{
		return m_tDestRecord;
	}
};

class clsClientCmd : public clsCmdBase
{
protected:
	bool m_bNeedRsp;
	bool m_bEvalOnly;
	bool m_bReadOnly;
	bool m_bNeedCommit;

	// for store res only
	uint64_t m_iWriteBatchID;
	string m_strWriteBatch;
	vector<uint64_t> m_vecWBUUID;

	uint32_t m_iPipeIdx;
	uint16_t m_hSubCmdID;

public:
	clsClientCmd(uint16_t hCmdID) : clsCmdBase(hCmdID),
									m_bNeedRsp(false),
									m_bEvalOnly(false),
									m_bReadOnly(false),
									m_bNeedCommit(false),
									m_iWriteBatchID(0),
									m_iPipeIdx(-1),
									m_hSubCmdID(-1) { }

	virtual ~clsClientCmd() { }

	BOOLEN_IS_SET(NeedRsp);
	BOOLEN_IS_SET(EvalOnly);
	BOOLEN_IS_SET(ReadOnly);
	BOOLEN_IS_SET(NeedCommit);

	TYPE_GET_SET(string, WriteBatch, strWriteBatch);
	TYPE_GET_SET(uint64_t, WriteBatchID, iWriteBatchID);
	TYPE_GET_SET(uint32_t, PipeIdx, iPipeIdx);
	TYPE_GET_SET(uint16_t, SubCmdID, hSubCmdID);
	TYPE_GET_SET(int, Result, iResult);
	TYPE_GET_SET(vector<uint64_t>, WBUUID, vecWBUUID);
};

class clsSimpleCmd : public clsClientCmd
{
private:
	string m_strKey;
	string m_strValue;

public:
	enum enumSCmdID
	{
		kGet = 1,
		kSet = 2
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
		uint32_t iHash = Hash(m_strKey);
		m_iEntityID = iHash % ENTITY_NUM;
	}
};

class clsWriteBatchCmd : public clsClientCmd
{
private:
	uint16_t m_hOriginCmdID;

public:
	clsWriteBatchCmd(uint16_t hOriginCmdID, uint64_t iUUID,
			const std::vector<uint64_t>& vecWBUUID, const std::string& strWriteBatch)
			: clsClientCmd(kWriteBatchCmd), m_hOriginCmdID(hOriginCmdID)
	{
		m_iUUID = iUUID;
		m_vecWBUUID = vecWBUUID;
		m_strWriteBatch = strWriteBatch;
	}

	clsWriteBatchCmd(const PaxosValue_t &tValue) :
			clsClientCmd(kWriteBatchCmd),
			m_hOriginCmdID(0)
	{
		m_vecWBUUID = tValue.vecValueUUID;
		m_strWriteBatch = tValue.strValue;
		m_iWriteBatchID = tValue.iValueID;
	}

	virtual ~clsWriteBatchCmd() { }

	TYPE_GET_SET(uint16_t, OriginCmdID, hOriginCmdID);

	virtual string GetTextCmd();
	virtual int ParseFromArray(const char *pcBuffer, uint32_t iLen);
	virtual int SerializeToArray(char *pcBuffer, uint32_t iLen);
};

class clsRecoverCmd : public clsClientCmd
{
private:
	uint64_t m_iMaxPLogEntry;
	uint64_t m_iMaxCommitedEntry;
	uint64_t m_iMaxContChosenEntry;
	uint64_t m_iMaxChosenEntry;
	uint64_t m_iMaxLoadingEntry;
	uint64_t m_iMaxNum;

	typedef vector< pair<uint64_t, EntryRecord_t > > EntryRecordList_t;
	EntryRecordList_t m_tEntryRecordList;

	bool m_bHasMore;
	bool m_bRangeLoaded;
	bool m_bCheckGetAll;
	bool m_bEvictEntity;

public:
	clsRecoverCmd(uint64_t iEntityID, uint64_t iMaxCommitedEntry);

	virtual ~clsRecoverCmd() { }

	TYPE_GET_SET(uint64_t, MaxPLogEntry, iMaxPLogEntry);
	TYPE_GET_SET(uint64_t, MaxCommitedEntry, iMaxCommitedEntry);
	TYPE_GET_SET(uint64_t, MaxContChosenEntry, iMaxContChosenEntry);
	TYPE_GET_SET(uint64_t, MaxChosenEntry, iMaxChosenEntry);
	TYPE_GET_SET(uint64_t, MaxLoadingEntry, iMaxLoadingEntry);
	TYPE_GET_SET(uint64_t, MaxNum, iMaxNum);

	BOOLEN_IS_SET(HasMore);
	BOOLEN_IS_SET(RangeLoaded);
	BOOLEN_IS_SET(CheckGetAll);
	BOOLEN_IS_SET(EvictEntity);

	virtual string GetTextCmd();

	virtual int ParseFromArray(const char *pcBuffer, uint32_t iLen)
	{
		assert(false); return 0;
	}
	virtual int SerializeToArray(char *pcBuffer, uint32_t iLen)
	{
		assert(false); return 0;
	}

	const EntryRecordList_t &GetEntryRecordList()
	{
		return m_tEntryRecordList;
	}
	void SetEntryRecordList(const EntryRecordList_t &tEntryRecordList)
	{
		m_tEntryRecordList = tEntryRecordList;
	}
};

class clsCmdFactory : public clsSingleton<clsCmdFactory>
{
private:
	volatile uint64_t m_iUUIDGenerator;

public:
	int Init(clsConfigure *poConf);
	void Destroy() { }

	clsCmdBase *CreateCmd(uint16_t hCmdID, const char *pcBuffer,
			uint32_t iLen, clsObjReusedPool<clsPaxosCmd> *poPool = NULL);

	clsCmdBase *CreateCmd(const char *pcBuffer, uint32_t iLen,
			clsObjReusedPool<clsPaxosCmd> *poPool = NULL);

	uint64_t GetNextUUID();
};

} // namespace Certain

#endif
