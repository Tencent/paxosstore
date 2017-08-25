
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "Certain.h"
#include "CertainUserImpl.h"
#include "DBImpl.h"
#include "PLogImpl.h"
#include "UserWorker.h"

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

	clsCertainUserImpl oImpl;

	clsKVEngine oKVEngineForPLog;
	Certain::clsPLogImpl oPLogImpl(&oKVEngineForPLog);

	clsKVEngine oKVEngineForDB;
	Certain::clsDBImpl oDBImpl(&oKVEngineForDB);

	Certain::clsCertainWrapper *poWrapper = NULL;

	poWrapper = Certain::clsCertainWrapper::GetInstance();
	assert(poWrapper != NULL);

	int iRet = poWrapper->Init(&oImpl, &oPLogImpl, &oDBImpl, argc, argv);
	AssertEqual(iRet, 0);

    Certain::clsUserWorker::GetInstance()->Start();

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
