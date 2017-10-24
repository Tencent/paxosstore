#ifndef CERTAIN_COMMON_H_
#define CERTAIN_COMMON_H_

#include "utils/Assert.h"
#include "utils/CRC32.h"
#include "utils/Hash.h"
#include "utils/Time.h"
#include "utils/AutoHelper.h"
#include "utils/Singleton.h"
#include "utils/CircleQueue.h"
#include "utils/Thread.h"
#include "utils/AOF.h"
#include "utils/LRUTable.h"
#include "utils/ArrayTimer.h"
#include "utils/FixSizePool.h"
#include "utils/OSSReport.h"
#include "utils/ObjReusedPool.h"
#include "utils/UseTimeStat.h"

#include "network/SocketHelper.h"
#include "network/IOChannel.h"
#include "network/EpollIO.h"

#define INVALID_ACCEPTOR_ID     (uint32_t(-1))
#define INVALID_SERVER_ID       (uint32_t(-1))
#define INVALID_PROPOSAL_NUM    (uint32_t(-1))

#define INVALID_ENTITY_ID       (uint64_t(-1))
#define INVALID_ENTRY           (uint64_t(-1))

#define MAX_IO_WORKER_NUM       128
#define MAX_ENTITY_WORKER_NUM   128
#define MAX_SMALL_ENTITY_NUM    0 // Disabled.

// Required: PB's limited size(60MB default) > MAX_WRITEBATCH_SIZE * 2
//           And a few bytes should be reserved for other fields.
#define MAX_WRITEBATCH_SIZE     ((20 << 20) - 1000) // 20MB around

#define MAX_UUID_NUM            50000
#define UUID_GROUP_NUM          32

#define MAX_ASYNC_PIPE_NUM      60

#define MAX_CONTROL_GROUP_NUM   24

#define RP_MAGIC_NUM            0x81ac

namespace Certain
{

struct PLogPos_t
{
    uint64_t iEntityID;
    uint64_t iEntry;

    bool operator < (const PLogPos_t &tOther) const
    {
        if (iEntityID != tOther.iEntityID)
        {
            return iEntityID < tOther.iEntityID;
        }
        return iEntry < tOther.iEntry;
    }

    bool operator > (const PLogPos_t &tOther) const
    {
        if (iEntityID != tOther.iEntityID)
        {
            return iEntityID > tOther.iEntityID;
        }
        return iEntry > tOther.iEntry;
    }

    bool operator == (const PLogPos_t &tOther) const
    {
        return (iEntityID == tOther.iEntityID && iEntry == tOther.iEntry);
    }

    bool operator != (const PLogPos_t &tOther) const
    {
        return !(*this == tOther);
    }
};

struct RawPacket_t
{
    uint16_t hMagicNum;
    uint16_t hVersion;

    uint16_t hCmdID;
    uint16_t hReserve;

    uint32_t iCheckSum;
    uint32_t iLen;
    char pcData[];
}__attribute__((__packed__));

#define RP_HEADER_SIZE sizeof(RawPacket_t)

inline void ConvertToHostOrder(RawPacket_t *ptRP)
{
    assert(ptRP->hReserve == 0);

    ptRP->hMagicNum = ntohs(ptRP->hMagicNum);
    ptRP->hVersion = ntohs(ptRP->hVersion);
    ptRP->hCmdID = ntohs(ptRP->hCmdID);
    ptRP->iCheckSum = ntohl(ptRP->iCheckSum);
    ptRP->iLen = ntohl(ptRP->iLen);
}

inline void ConvertToNetOrder(RawPacket_t *ptRP)
{
    assert(ptRP->hReserve == 0);

    ptRP->hMagicNum = htons(ptRP->hMagicNum);
    ptRP->hVersion = htons(ptRP->hVersion);
    ptRP->hCmdID = htons(ptRP->hCmdID);
    ptRP->iCheckSum = htonl(ptRP->iCheckSum);
    ptRP->iLen = htonl(ptRP->iLen);
}

// Proposal Number 0 means null.

// (TODO)rock: use 'class clsPaxosValue' since class inside
struct PaxosValue_t
{
    uint64_t iValueID;
    vector<uint64_t> vecValueUUID;

    bool bHasValue;
    string strValue;

    PaxosValue_t() : iValueID(0), bHasValue(false) { }
    PaxosValue_t(uint64_t iValueID_,
            const vector<uint64_t> vecValueUUID_,
            bool bHasValue_,
            const string &strValue_) :
        iValueID(iValueID_), vecValueUUID(vecValueUUID_),
        bHasValue(bHasValue_), strValue(strValue_) { }

    bool operator == (const PaxosValue_t &tOther) const
    {
        if (iValueID == tOther.iValueID
                && vecValueUUID == tOther.vecValueUUID
                && bHasValue == tOther.bHasValue
                && strValue == tOther.strValue)
        {
            return true;
        }

        return false;
    }
};

// (TODO)rock: use 'class clsEntryRecord' since class inside
struct EntryRecord_t
{
    uint32_t iPreparedNum;
    uint32_t iPromisedNum;
    uint32_t iAcceptedNum;

    PaxosValue_t tValue;
    bool bChosen;

    bool bCheckedEmpty; // For Read Opt.

    uint64_t iStoredValueID; // For PutValue Opt.

    bool operator == (const EntryRecord_t &tOther) const
    {
        if (iPreparedNum == tOther.iPreparedNum
                && iPromisedNum == tOther.iPromisedNum
                && iAcceptedNum == tOther.iAcceptedNum
                && tValue == tOther.tValue
                && bChosen == tOther.bChosen)
        {
            return true;
        }

        return false;
    }
};

struct PackedEntryRecord_t
{
    uint32_t iPreparedNum;
    uint32_t iPromisedNum;
    uint32_t iAcceptedNum;

    uint32_t iValueLen;
    uint64_t iValueID;

    uint8_t bChosen : 1;
    uint8_t bHasValue : 1;
    uint8_t cReserve : 6;

    char pcValue[];
}__attribute__((__packed__));

inline int EntryRecordToString(const EntryRecord_t &tRecord,
        string &strRecord)
{
    uint32_t iTotalLen = sizeof(PackedEntryRecord_t);
    if (tRecord.iStoredValueID != tRecord.tValue.iValueID)
    {
        iTotalLen += tRecord.tValue.strValue.size();
    }
    strRecord.clear();
    strRecord.reserve(iTotalLen);

    PackedEntryRecord_t tPRecord = { 0 };

    tPRecord.iPreparedNum = tRecord.iPreparedNum;
    tPRecord.iPromisedNum = tRecord.iPromisedNum;
    tPRecord.iAcceptedNum = tRecord.iAcceptedNum;

    tPRecord.iValueID = tRecord.tValue.iValueID;

    tPRecord.iValueLen = 0;
    tPRecord.bHasValue = false;

    if (tRecord.iStoredValueID != tRecord.tValue.iValueID)
    {
        tPRecord.iValueLen = tRecord.tValue.strValue.size();
        tPRecord.bHasValue = tRecord.tValue.bHasValue;
    }

    tPRecord.bChosen = tRecord.bChosen;

    tPRecord.cReserve = 0;
    strRecord.append((char *)&tPRecord, sizeof(tPRecord));

    if (tRecord.iStoredValueID != tRecord.tValue.iValueID)
    {
        strRecord.append(tRecord.tValue.strValue);
    }

    return 0;
}

inline int StringToEntryRecord(const string &strRecord,
        EntryRecord_t &tRecord)
{
    if (sizeof(PackedEntryRecord_t) > strRecord.size())
    {
        return -1;
    }

    const PackedEntryRecord_t *ptPRecord = (
            const PackedEntryRecord_t *)strRecord.c_str();
    if (ptPRecord->iValueLen + sizeof(PackedEntryRecord_t)
            != strRecord.size())
    {
        return -2;
    }

    assert(ptPRecord->cReserve == 0);

    tRecord.iPreparedNum = ptPRecord->iPreparedNum;
    tRecord.iPromisedNum = ptPRecord->iPromisedNum;
    tRecord.iAcceptedNum = ptPRecord->iAcceptedNum;

    tRecord.tValue.iValueID = ptPRecord->iValueID;
    tRecord.tValue.strValue = string(ptPRecord->pcValue, ptPRecord->iValueLen);
    tRecord.tValue.bHasValue = ptPRecord->bHasValue;

    tRecord.bChosen = ptPRecord->bChosen;
    tRecord.bCheckedEmpty = false;
    tRecord.iStoredValueID = 0;

    return 0;
}

inline string EntryRecordToString(const EntryRecord_t &tRecord)
{
    char acBuffer[1024];

    snprintf(acBuffer, 1024, "[%u %u %u v([%lu][%lu][%lu] %lu %lu %lu %u) %u]",
            tRecord.iPreparedNum, tRecord.iPromisedNum, tRecord.iAcceptedNum,
            (tRecord.tValue.iValueID >> 48),
            ((tRecord.tValue.iValueID >> 32) & 0xffff),
            (tRecord.tValue.iValueID & 0xffffffff), tRecord.iStoredValueID,
            tRecord.tValue.strValue.size(), tRecord.tValue.vecValueUUID.size(),
            tRecord.tValue.bHasValue, tRecord.bChosen);

    return acBuffer;
}

inline void InitEntryRecord(EntryRecord_t *ptRecord)
{
    ptRecord->iPreparedNum = 0;
    ptRecord->iPromisedNum = 0;
    ptRecord->iAcceptedNum = 0;

    ptRecord->bChosen = false;

    ptRecord->tValue.iValueID = 0;
    ptRecord->tValue.vecValueUUID.clear();
    ptRecord->tValue.strValue = "";
    ptRecord->tValue.bHasValue = false;

    ptRecord->bCheckedEmpty = false;
    ptRecord->iStoredValueID = 0;
}

inline bool IsEntryRecordUpdated(const EntryRecord_t &tOldRecord,
        const EntryRecord_t &tNewRecord)
{
    if (tOldRecord.bChosen)
    {
        return false;
    }

    if (tNewRecord.bChosen)
    {
        return true;
    }

    if (tOldRecord.iPromisedNum != tNewRecord.iPromisedNum)
    {
        return true;
    }

    if (tOldRecord.iAcceptedNum != tNewRecord.iAcceptedNum)
    {
        return true;
    }

    return false;
}

inline bool IsEntryRecordEmpty(const EntryRecord_t &tRecord)
{
    if (tRecord.iPreparedNum == 0 && tRecord.iPromisedNum == 0
            && tRecord.iAcceptedNum == 0 && tRecord.tValue.iValueID == 0)
    {
        return true;
    }
    return false;
}

// For check only.
inline int CheckEntryRecordMayWithVIDOnly(const EntryRecord_t &tRecord)
{
    if (tRecord.iAcceptedNum == 0)
    {
        if (tRecord.bChosen)
        {
            return -1;
        }
    }

    if (tRecord.iAcceptedNum > 0)
    {
        if (tRecord.tValue.iValueID == 0)
        {
            return -2;
        }
    }

    if (tRecord.bChosen)
    {
        if (tRecord.iAcceptedNum == 0)
        {
            return -3;
        }
    }

    if (tRecord.tValue.bHasValue)
    {
        if (tRecord.tValue.iValueID == 0)
        {
            return -4;
        }
    }

    if (tRecord.tValue.iValueID == 0)
    {
        if (tRecord.tValue.vecValueUUID.size() > 0)
        {
            return -5;
        }

        if (tRecord.tValue.strValue.size() > 0)
        {
            return -6;
        }
    }

    return 0;
}

inline int CheckEntryRecord(const EntryRecord_t &tRecord)
{
    if (tRecord.iAcceptedNum == 0)
    {
        if (tRecord.bChosen)
        {
            return -1;
        }
    }

    if (tRecord.iAcceptedNum > 0)
    {
        if (!tRecord.tValue.bHasValue)
        {
            return -2;
        }
        if (tRecord.tValue.iValueID == 0)
        {
            return -3;
        }
    }

    if (tRecord.bChosen)
    {
        if (tRecord.iAcceptedNum == 0)
        {
            return -4;
        }
    }

    if (tRecord.tValue.bHasValue || tRecord.tValue.iValueID > 0)
    {
        if (!tRecord.tValue.bHasValue)
        {
            return -5;
        }

        if (tRecord.tValue.iValueID == 0)
        {
            return -6;
        }
    }

    if (tRecord.tValue.iValueID == 0)
    {
        if (tRecord.tValue.vecValueUUID.size() > 0)
        {
            return -7;
        }
        if (tRecord.tValue.strValue.size() > 0)
        {
            return -8;
        }
    }

    return 0;
}

} // namespace Certain

#endif
