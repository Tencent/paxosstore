#ifndef CERTAIN_INCLUDE_CERTAIN_H_
#define CERTAIN_INCLUDE_CERTAIN_H_

#include "CertainUserBase.h"
#include "PLogBase.h"
#include "DBBase.h"

namespace Certain
{

struct EntityMeta_t
{
    uint64_t iMaxCommitedEntry;
    uint64_t iMaxPLogEntry;
    uint64_t iValueID;
    bool bChosen;

    uint32_t iDBFlag;
};

class clsEntityInfoMng;
class clsCertainWrapper : public clsSingleton<clsCertainWrapper>,
public clsThreadBase
{
private:
    clsConfigure *m_poConf;
    clsPLogBase *m_poPLogEngine;
    clsDBBase *m_poDBEngine;
    clsEntityGroupMng *m_poEntityGroupMng;
    clsAsyncQueueMng *m_poQueueMng;
    clsAsyncPipeMng *m_poPipeMng;

    clsCertainUserBase *m_poCertainUser;

    friend class clsSingleton<clsCertainWrapper>;
    clsCertainWrapper() : m_poConf(NULL),
    m_poPLogEngine(NULL),
    m_poDBEngine(NULL),
    m_poEntityGroupMng(NULL),
    m_poQueueMng(NULL),
    m_poPipeMng(NULL),
    m_poCertainUser(NULL) { }

    vector<clsThreadBase *> m_vecWorker;

    int InitWorkers();
    void DestroyWorkers();

    int StartWorkers();
    void StopWorkers();

    int InitManagers();
    void DestroyManagers();

    int SyncWaitCmd(clsClientCmd *poCmd);
    int CheckDBStatus(uint64_t iEntityID, uint64_t iCommitedEntry);
    void TriggeRecover(uint64_t iEntityID, uint64_t iCommitedEntry);

public:
    int Init(clsCertainUserBase *poCertainUser, clsPLogBase *poPLogEngine,
            clsDBBase *poDBEngine, int iArgc = 0, char *pArgv[] = NULL);
    void Destroy();

    void Run();

    virtual ~clsCertainWrapper() { }

    bool CheckIfAllWorkerExited();

    int GetWriteBatch(uint64_t iEntityID, uint64_t iEntry,
            string &strWriteBatch, uint64_t *piValueID = NULL);

    // strWriteBatch.size() == 0 <==> readonly
    int RunPaxos(uint64_t iEntityID, uint64_t iEntry, uint16_t hSubCmdID,
            const vector<uint64_t> &vecWBUUID, const string &strWriteBatch);

    int CatchUpAndRunPaxos(uint64_t iEntityID,  uint16_t hSubCmdID,
            const vector<uint64_t> &vecWBUUID, const string &strWriteBatch);

    int EntityCatchUp(uint64_t iEntityID, uint64_t &iMaxCommitedEntry);

    int GetMaxChosenEntry(uint64_t iEntityID, uint64_t &iMaxChosenEntry);

    int GetEntityInfo(uint64_t iEntityID, EntityInfo_t &tEntityInfo, EntityMeta_t &tMeta);

    clsPLogBase *GetPLogEngine() { return m_poPLogEngine; }
    clsDBBase *GetDBEngine() { return m_poDBEngine; }
    clsCertainUserBase *GetCertainUser() { return m_poCertainUser; }

    int EvictEntity(uint64_t iEntityID);

    int ExplicitGetAll(uint64_t iEntityID);

    clsConfigure *GetConf();
};

} // namespace Certian

#endif
