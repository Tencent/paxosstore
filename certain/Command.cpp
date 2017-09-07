#include "Command.h"

#if CERTAIN_SIMPLE_EXAMPLE
#include "example/SimpleCmd.h"
#endif

namespace Certain
{

int clsCmdBase::GenRspPkg(char *pcBuffer, uint32_t iLen, bool bCheckSum)
{
    if (iLen < RP_HEADER_SIZE)
    {
        return -1;
    }

    int iRet = SerializeToArray(pcBuffer + RP_HEADER_SIZE,
            iLen - RP_HEADER_SIZE);
    if (iRet < 0)
    {
        CertainLogError("SerializeToArray ret %d", iRet);
        return -2;
    }

    RawPacket_t *lp = (RawPacket_t *)pcBuffer;
    lp->hMagicNum = RP_MAGIC_NUM;
    lp->hVersion = 0;
    lp->hCmdID = GetCmdID();
    lp->hReserve = 0;
    lp->iLen = iRet;

    if (m_iRspPkgCRC32 == 0)
    {
        if (bCheckSum)
        {
            m_iRspPkgCRC32 = CRC32(lp->pcData, lp->iLen);
        }
    }

    lp->iCheckSum = m_iRspPkgCRC32;

    ConvertToNetOrder(lp);

    return iRet + RP_HEADER_SIZE;
}

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

void clsPaxosCmd::Init(uint32_t iAcceptorID,
        uint64_t iEntityID,
        uint64_t iEntry,
        const EntryRecord_t *ptSrc,
        const EntryRecord_t *ptDest)
{
    m_bCheckEmpty = false;
    m_bPLogError = false;
    m_bPLogReturn = false;
    m_bPLogLoad = false;
    m_bCheckHasMore = false;
    m_bHasMore = false;

    m_iSrcAcceptorID = iAcceptorID;
    m_iDestAcceptorID = INVALID_ACCEPTOR_ID;
    m_iEntityID = iEntityID;
    m_iEntry = iEntry;

    if (ptSrc == NULL)
    {
        InitEntryRecord(&m_tSrcRecord);
    }
    else
    {
        m_tSrcRecord = *ptSrc;
    }

    if (ptDest == NULL)
    {
        InitEntryRecord(&m_tDestRecord);
    }
    else
    {
        m_tDestRecord = *ptDest;
    }

    m_iMaxChosenEntry = 0;

    m_iResult = 0;
}

void clsPaxosCmd::ConvertFromPB(EntryRecord_t &tEntryRecord,
        const CertainPB::EntryRecord *poRecord)
{
    tEntryRecord.iPreparedNum = poRecord->prepared_num();
    tEntryRecord.iPromisedNum = poRecord->promised_num();
    tEntryRecord.iAcceptedNum = poRecord->accepted_num();

    vector<uint64_t> vecValueUUID;
    for (int i = 0; i < poRecord->value_uuid_size(); ++i)
    {
        vecValueUUID.push_back(poRecord->value_uuid(i));
    }
    tEntryRecord.tValue = PaxosValue_t(poRecord->value_id(),
            vecValueUUID, poRecord->has_value(), poRecord->value());
    tEntryRecord.bChosen = poRecord->chosen();

    tEntryRecord.bCheckedEmpty = false;
    tEntryRecord.iStoredValueID = 0;
}

void clsPaxosCmd::clsPaxosCmd::ConvertToPB(const EntryRecord_t &tEntryRecord,
        CertainPB::EntryRecord *poRecord, bool bCopyValue)
{
    poRecord->set_prepared_num(tEntryRecord.iPreparedNum);
    poRecord->set_promised_num(tEntryRecord.iPromisedNum);
    poRecord->set_accepted_num(tEntryRecord.iAcceptedNum);
    poRecord->set_value_id(tEntryRecord.tValue.iValueID);
    if (bCopyValue)
    {
        AssertEqual(poRecord->value_uuid_size(), 0);
        for (uint32_t i = 0; i < tEntryRecord.tValue.vecValueUUID.size(); ++i)
        {
            poRecord->add_value_uuid(tEntryRecord.tValue.vecValueUUID[i]);
        }
        poRecord->set_value(tEntryRecord.tValue.strValue);
    }
    poRecord->set_chosen(tEntryRecord.bChosen);
}

int clsPaxosCmd::ParseFromArray(const char *pcBuffer, uint32_t iLen)
{
    CertainPB::PaxosCmd oPaxosCmd;
    if (!oPaxosCmd.ParseFromArray(pcBuffer, iLen))
    {
        CertainLogError("ParseFromArray fail");
        return -1;
    }

    SetFromHeader(oPaxosCmd.mutable_header());

    m_iSrcAcceptorID = oPaxosCmd.src_acceptor_id();
    m_iDestAcceptorID = oPaxosCmd.dest_acceptor_id();

    ConvertFromPB(m_tSrcRecord, &oPaxosCmd.src_record());
    ConvertFromPB(m_tDestRecord, &oPaxosCmd.dest_record());

    if (oPaxosCmd.check_empty())
    {
        Assert(IsEntryRecordEmpty(m_tSrcRecord));
        Assert(IsEntryRecordEmpty(m_tDestRecord));
        m_tDestRecord.iPromisedNum = INVALID_PROPOSAL_NUM;
    }

    m_iMaxChosenEntry = oPaxosCmd.max_chosen_entry();
    m_iResult = oPaxosCmd.result();

    return 0;
}

int clsPaxosCmd::SerializeToArray(char *pcBuffer, uint32_t iLen)
{
    CertainPB::PaxosCmd oPaxosCmd;

    SetToHeader(oPaxosCmd.mutable_header());

    oPaxosCmd.set_src_acceptor_id(m_iSrcAcceptorID);
    oPaxosCmd.set_dest_acceptor_id(m_iDestAcceptorID);

    bool bCopyValue = false;
    if (m_tSrcRecord.tValue.iValueID != m_tDestRecord.tValue.iValueID
            && m_tSrcRecord.tValue.bHasValue)
    {
        bCopyValue = true;
    }

    if (!IsEntryRecordEmpty(m_tSrcRecord))
    {
        ConvertToPB(m_tSrcRecord,
                oPaxosCmd.mutable_src_record(), bCopyValue);
        Assert(oPaxosCmd.src_record().has_value() == bCopyValue);
    }

    if (!IsEntryRecordEmpty(m_tDestRecord))
    {
        ConvertToPB(m_tDestRecord,
                oPaxosCmd.mutable_dest_record(), false);
    }

    if (m_bCheckEmpty)
    {
        oPaxosCmd.set_check_empty(true);
    }

    if (m_iMaxChosenEntry > 0)
    {
        oPaxosCmd.set_max_chosen_entry(m_iMaxChosenEntry);
    }
    if (m_iResult != 0)
    {
        oPaxosCmd.set_result(m_iResult);
    }

    return SerializeMsgToArray(oPaxosCmd, pcBuffer, iLen);
}

string clsPaxosCmd::GetTextCmd()
{
    char acBuf[1024];
    string strSrc = EntryRecordToString(m_tSrcRecord);
    string strDest = EntryRecordToString(m_tDestRecord);

    snprintf(acBuf, 1024,
            "cmd %u uuid %lu E(%lu, %lu) max %lu "
            "id (%u, %u) ce %u chm %u src %s dest %s res %d",
            m_hCmdID, m_iUUID, m_iEntityID, m_iEntry, m_iMaxChosenEntry,
            m_iSrcAcceptorID, m_iDestAcceptorID, m_bCheckEmpty,
            m_bCheckHasMore, strSrc.c_str(), strDest.c_str(), m_iResult);

    return acBuf;
}

clsRecoverCmd::clsRecoverCmd(uint64_t iEntityID, uint64_t iMaxCommitedEntry)
    : clsClientCmd(kRecoverCmd)
{
    m_iEntityID = iEntityID;
    m_iEntry = 0;

    m_iMaxCommitedEntry = iMaxCommitedEntry;
    m_iMaxContChosenEntry = 0;
    m_iMaxChosenEntry = 0;
    m_iMaxPLogEntry = INVALID_ENTRY;

    m_iMaxLoadingEntry = 0;
    m_bHasMore = false;
    m_bRangeLoaded = false;
    m_bCheckGetAll = false;
    m_bEvictEntity = false;
}

string clsRecoverCmd::GetTextCmd()
{
    char acBuf[128];
    snprintf(acBuf, 128, "cmd %hu uuid %lu E(%lu, %lu) %lu %lu %lu (%lu, %u)",
            m_hCmdID, m_iUUID, m_iEntityID, m_iMaxCommitedEntry,
            m_iMaxContChosenEntry, m_iMaxChosenEntry, m_iMaxLoadingEntry,
            m_tEntryRecordList.size(), m_bHasMore);
    return acBuf;
}

string clsWriteBatchCmd::GetTextCmd()
{
    char acBuf[128];
    snprintf(acBuf, 128, "cmd %hu uuid %lu wb %lu E(%lu, %lu) origin_cmd %hu size %lu",
            m_hCmdID, m_iUUID, m_iWriteBatchID, m_iEntityID, m_iEntry, m_hOriginCmdID,
            m_strWriteBatch.size());
    return acBuf;
}

int clsWriteBatchCmd::ParseFromArray(const char *pcBuffer, uint32_t iLen)
{
    Assert(false); return 0;
}

int clsWriteBatchCmd::SerializeToArray(char *pcBuffer, uint32_t iLen)
{
    Assert(false); return 0;
}

int clsCmdFactory::Init(clsConfigure *poConf)
{
    // uuid = [server_id:8][time:16][auto_incr:16]

    uint64_t iServerID = poConf->GetLocalServerID();
    uint64_t iTime = GetCurrTime() % (1 << 16);

    m_iUUIDGenerator = (iServerID << 32) + (iTime << 16);

    return 0;
}

clsCmdBase *clsCmdFactory::CreateCmd(uint16_t hCmdID, const char *pcBuffer,
        uint32_t iLen, clsObjReusedPool<clsPaxosCmd> *poPool)
{
    clsCmdBase *poCmd = NULL;
    int iRet;

    switch (hCmdID)
    {
        case kPaxosCmd:
            if (poPool == NULL)
            {
                poCmd = new clsPaxosCmd;
            }
            else
            {
                poCmd = poPool->NewObjPtr();
            }
            iRet = poCmd->ParseFromArray(pcBuffer, iLen);
            break;

        case kSimpleCmd:
            poCmd = new clsSimpleCmd;
            iRet = poCmd->ParseFromArray(pcBuffer, iLen);
            break;

        default:
            assert(false);
    }

    if (iRet < 0)
    {
        CertainLogError("ParseFromArray ret %d", iRet);
        delete poCmd, poCmd = NULL;

        AssertEqual(iRet, 0);
    }
    return poCmd;
}

clsCmdBase *clsCmdFactory::CreateCmd(const char *pcBuffer, uint32_t iLen,
        clsObjReusedPool<clsPaxosCmd> *poPool)
{
    AssertNotMore(RP_HEADER_SIZE, iLen);
    RawPacket_t *lp = (RawPacket_t *)pcBuffer;
    AssertEqual(lp->iLen + RP_HEADER_SIZE, iLen);

    return CreateCmd(lp->hCmdID, lp->pcData, lp->iLen, poPool);
}

uint64_t clsCmdFactory::GetNextUUID()
{
    return __sync_fetch_and_add(&m_iUUIDGenerator, 1);
}

} // namespace Certain
