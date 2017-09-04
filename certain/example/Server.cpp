#include "Certain.h"
#include "CertainUserImpl.h"
#include "DBImpl.h"
#include "PLogImpl.h"
#include "UserWorker.h"

#include "utils/Logger.h"

static volatile uint8_t g_iStopFlag;

void SetStopFlag(int iSig)
{
	g_iStopFlag = 1;
}

int main(int argc, char **argv)
{
	signal(SIGINT, SetStopFlag);

	if (argc < 3)
	{
		printf("%s -c conf_path [options see certain.conf]\n", argv[0]);
		return -1;
	}

    Certain::OpenLog("/home/qspace/log/", 3, 1, 1);

	clsCertainUserImpl oImpl;

    leveldb::DB* plog = NULL;
    {
        leveldb::Status s;
        string dbname = "/home/qspace/data/simple/plog";

        leveldb::Options opts;
        opts.create_if_missing = true;
        assert(leveldb::DB::Open(opts, dbname, &plog).ok());
    }

	clsKVEngine oKVEngineForPLog(plog);
    oKVEngineForPLog.SetNoThing();
	Certain::clsPLogImpl oPLogImpl(&oKVEngineForPLog);

    leveldb::DB* db = NULL;
    {
        leveldb::Status s;
        string dbname = "/home/qspace/data/simple/db";

        leveldb::Options opts;
        opts.create_if_missing = true;
        assert(leveldb::DB::Open(opts, dbname, &db).ok());
    }

	clsKVEngine oKVEngineForDB(db);
    oKVEngineForDB.SetUseMap();
	Certain::clsDBImpl oDBImpl(&oKVEngineForDB);

	Certain::clsCertainWrapper *poWrapper = NULL;

	poWrapper = Certain::clsCertainWrapper::GetInstance();
	assert(poWrapper != NULL);

	int iRet = poWrapper->Init(&oImpl, &oPLogImpl, &oDBImpl, argc, argv);
	AssertEqual(iRet, 0);

    Certain::clsUserWorker::Init(100);
    for (uint32_t i = 0; i < 100; ++i)
    {
        Certain::clsUserWorker *poWorker = new Certain::clsUserWorker(i);
        poWorker->Start();
    }

	poWrapper->Start();

	while (1)
	{
		sleep(1);

		if (g_iStopFlag)
		{
            exit(-1);
			if (!poWrapper->IsStopFlag())
			{
				poWrapper->SetStopFlag();
			}
			else if (poWrapper->IsExited())
			{
				printf("%s is exited!\n", argv[0]);

				break;
			}
			else
			{
				printf("%s is exiting...\n", argv[0]);
			}
		}
	}

	poWrapper->Destroy();

	return 0;
}
