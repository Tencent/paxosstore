#include "grpc/src/core/lib/support/env.h"

#include "CertainUserImpl.h"
#include "example/DBImpl.h"
#include "example/PLogImpl.h"
#include "example/ServiceImpl.h"

int InitDB(Certain::clsCertainUserBase* poImpl, Certain::clsConfigure* poConf)
{
    std::string strLID = std::to_string(poConf->GetLocalServerID());

    dbtype::Options tOpts;
    tOpts.create_if_missing = true;
    tOpts.compaction_filter = new clsPLogFilter;
    tOpts.comparator = new clsPLogComparator;

    static const std::string kLogDBName = poConf->GetCertainPath() + "/logdb_" + strLID;
    dbtype::DB* poLogDB = NULL;
    assert(dbtype::DB::Open(tOpts, kLogDBName.c_str(), &poLogDB).ok());

    Certain::clsPLogBase* oPLogImpl = new clsPLogImpl(poLogDB);

    dbtype::Options tDBOpts;
    tDBOpts.create_if_missing = true;
    static const std::string kDataDBName = poConf->GetCertainPath() + "/datadb_" + strLID;
    dbtype::DB* poDataDB = NULL;
    assert(dbtype::DB::Open(tDBOpts, kDataDBName.c_str(), &poDataDB).ok());

    Certain::clsDBBase* oDBImpl = new clsDBImpl(poDataDB);

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
    Certain::clsCertainUserBase* poImpl = new clsCertainUserImpl();

    Certain::clsConfigure oConf(argc, argv);
    std::string strLID = std::to_string(oConf.GetLocalServerID());
    Certain::SetThreadTitle("card_srv_%u", oConf.GetLocalServerID());

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
    std::string strAddr = "0.0.0.0:" + std::to_string(dynamic_cast<clsCertainUserImpl*>(
                poWrapper->GetCertainUser())->GetServicePort());
    poMng->Init(strAddr, iWorkerNum, iCallDataNumPerWorker, &oImpl);

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
