#include "example/ServiceImpl.h"
#include "CertainUserImpl.h"
#include "example/DBImpl.h"
#include "example/PLogImpl.h"
#include "grpc/src/core/lib/support/env.h"

using namespace Certain;

int InitDB(clsCertainUserBase* poImpl, Certain::clsConfigure* poConf)
{
    string strLID = to_string(poConf->GetLocalServerID());

    dbtype::Options tOpts;
    tOpts.create_if_missing = true;
    tOpts.compaction_filter = new clsPLogFilter;
    tOpts.comparator = new clsPLogComparator;

    static const string kLogDBName = poConf->GetCertainPath() + "/logdb_" + strLID;
    dbtype::DB* poLogDB = NULL;
    assert(dbtype::DB::Open(tOpts, kLogDBName.c_str(), &poLogDB).ok());

    clsPLogBase* oPLogImpl = new clsPLogImpl(poLogDB);

    static const string kDataDBName = poConf->GetCertainPath() + "/datadb_" + strLID;
    dbtype::DB* poDataDB = NULL;
    assert(dbtype::DB::Open(tOpts, kDataDBName.c_str(), &poDataDB).ok());

    pthread_mutex_t* poSnapshotMapMutex = new pthread_mutex_t;
    assert(0 == pthread_mutex_init(poSnapshotMapMutex, NULL));

    std::map<uint32_t, std::pair<uint64_t, std::shared_ptr<clsSnapshotWrapper>>> *poSnapshotMap =
        new std::map<uint32_t, std::pair<uint64_t, std::shared_ptr<clsSnapshotWrapper>>>;
    assert(poSnapshotMap != NULL);

    clsDBBase* oDBImpl = new clsDBImpl(poDataDB, poSnapshotMapMutex, poSnapshotMap);

    return Certain::clsCertainWrapper::GetInstance()->Init(poImpl, oPLogImpl, oDBImpl, poConf);
}

int main(int argc, char** argv)
{
    gpr_setenv("GRPC_POLL_STRATEGY", "epollsig");
    grpc_init(); // for static lib

	if (argc < 3)
	{
		printf("%s -c conf_path [options see certain.conf]\n", argv[0]);
		return -1;
	}

    int iRet;
	clsCertainUserBase* poImpl = new clsCertainUserImpl();

    Certain::clsConfigure oConf(argc, argv);
    string strLID = to_string(oConf.GetLocalServerID());
    SetThreadTitle("card_srv_%u", oConf.GetLocalServerID());

    if (access(oConf.GetCertainPath().c_str(), F_OK) != 0)
    {
        printf("CertainPath %s not exist\n", oConf.GetCertainPath().c_str());
        return -1;
    }

    oConf.SetLogPath(oConf.GetCertainPath() + "/log_" + strLID);

	Certain::clsCertainWrapper *poWrapper = NULL;
    poWrapper = Certain::clsCertainWrapper::GetInstance();
    assert(poWrapper != NULL);

    iRet = InitDB(poImpl, &oConf);
    AssertEqual(iRet, 0);

    int iUserWorkerNum = 50;
    clsUserWorker::Init(iUserWorkerNum);
    for (int i = 0; i < iUserWorkerNum; ++i)
    {
        clsUserWorker *poWorker = new clsUserWorker(i);
        poWorker->Start();
    }

    int iWorkerNum = 30;
    int iCallDataNumPerWorker = 200;

    // New Workers.
    clsServiceImpl oImpl;
    clsServerWorkerMng *poMng = clsServerWorkerMng::GetInstance();
    poMng->Init(string("0.0.0.0:50051"), iWorkerNum, iCallDataNumPerWorker, &oImpl);

    // Register interfaces here.
    REGISTER_INTERFACE(example, CardServer, Echo);

    REGISTER_INTERFACE(example, CardServer, InsertCard);
    REGISTER_INTERFACE(example, CardServer, UpdateCard);
    REGISTER_INTERFACE(example, CardServer, DeleteCard);
    REGISTER_INTERFACE(example, CardServer, SelectCard);
    REGISTER_INTERFACE(example, CardServer, GetDBEntityMeta);
    REGISTER_INTERFACE(example, CardServer, GetAllForCertain);
    REGISTER_INTERFACE(example, CardServer, RecoverData);

    // RunAll
    poMng->RunAll();

    poWrapper->Start();

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(poWrapper->GetDBEngine());
    while (1)
    {
        sleep(1);
        poDBEngine->EraseSnapshot();
    }

    return 0;
}
