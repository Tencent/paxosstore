#ifndef CERTAIN_PERFLOG_H_
#define CERTAIN_PERFLOG_H_

#include "Common.h"

namespace Certain
{

#define PERFLOG_UNCHECK_SIZE 5
#define PERFLOG_END_FLAG 0xae

#define PERF_LOG_SKIP(x) \
    do { uint64_t tx = x; iCurr += tx; } while(0);
#define PERF_LOG_ERROR_SKIP(x) \
    do { uint64_t tx = x; iErrorSkipped += tx; iCurr += tx; } while(0);

enum enumPerfType
{
    ePerfTypeCommitSeq = 1,
    ePerfTypePLogSeq,
};

struct PerfHead_t
{
    uint8_t cType;
    uint32_t iCheckSum;
    char pcCheckSumData[];
    uint64_t iTimestampMS;

    uint64_t iEntityID;
    uint64_t iEntry;
}__attribute__((__packed__));

struct CommitSeq_t
{
    PerfHead_t tHead;

    uint32_t iWriteBatchSize;
    uint32_t iWriteBatchCRC32;

    char cEndFlag;
}__attribute__((__packed__));

struct PLogSeq_t
{
    PerfHead_t tHead;

    uint32_t iValueSize;
    uint32_t iValueCRC32;
    uint64_t iStoredValueID;

    PackedEntryRecord_t tPackedRecord;

    char cEndFlag;
}__attribute__((__packed__));

class clsPerfLog : public clsSingleton<clsPerfLog>
{
private:
    uint32_t m_iUsePerfLog;
    clsAppendOnlyFile *m_poAOF;

    friend class clsSingleton<clsPerfLog>;
    clsPerfLog() : m_iUsePerfLog(0), m_poAOF(NULL) { }

public:
    int Init(const char *pcPath);
    void Destroy();

    int PutCommitSeq(uint64_t iEntityID, uint64_t iEntry,
            uint32_t iWriteBatchSize, uint32_t iWriteBatchCRC32);

    int PutPLogSeq(uint64_t iEntityID, uint64_t iEntry,
            const EntryRecord_t& tRecord);

    static int ParseData(const char *pcData, size_t iSize,
            vector<PerfHead_t *> &vecPerfHead);

    void Flush(bool bAsync = false);
};

} // namespace Certain

#endif
