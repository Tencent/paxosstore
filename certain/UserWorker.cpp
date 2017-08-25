
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "UserWorker.h"


namespace Certain
{

void clsUserWorker::Run()
{
    int iRet;
    clsCertainWrapper *poWrapper = clsCertainWrapper::GetInstance();
    m_poUserQueue = new clsUserQueue(1000);
    while (1)
    {
        clsClientCmd *poCmd = NULL;
        iRet = m_poUserQueue->TakeByOneThread(&poCmd);
        if (iRet != 0)
        {
            usleep(1000);
            continue;
        }

        CertainLogError("cmd %s", poCmd->GetTextCmd().c_str());
        poCmd->SetResult(543331);

        uint64_t iMaxCommitedEntry = 0;
        iRet = poWrapper->EntityCatchUp(poCmd->GetEntityID(), iMaxCommitedEntry);
        if (iRet != 0)
        {
            CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
            poCmd->SetResult(iRet);
        }
        poCmd->SetEntry(iMaxCommitedEntry + 1);
        vector<uint64_t> vecWBUUID;

        if (poCmd->GetSubCmdID() == clsSimpleCmd::kGet)
        {
            string strWriteBatch;
            iRet = poWrapper->RunPaxos(poCmd->GetEntityID(), poCmd->GetEntry(),
                    iMaxCommitedEntry + 1, vecWBUUID, strWriteBatch);
            if (iRet == 0)
            {
                poWrapper->GetDBEngine()->Submit(poCmd, strWriteBatch);
            }
            else
            {
                CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
                poCmd->SetResult(iRet);
            }
        }
        else if (poCmd->GetSubCmdID() == clsSimpleCmd::kSet)
        {
            string strWriteBatch;
            iRet = poWrapper->GetDBEngine()->Submit(poCmd, strWriteBatch);
            if (iRet != 0)
            {
                CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
                poCmd->SetResult(iRet);
            }
            else
            {
                iRet = poWrapper->RunPaxos(poCmd->GetEntityID(), poCmd->GetEntry(),
                        iMaxCommitedEntry + 1, vecWBUUID, strWriteBatch);
                if (iRet != 0)
                {
                CertainLogError("cmd %s ret %d", poCmd->GetTextCmd().c_str(), iRet);
                    poCmd->SetResult(iRet);
                }
            }
        }
        else
        {
            assert(false);
        }

        assert(clsIOWorkerRouter::GetInstance()->Go(poCmd) == 0);
    }
}

}; // namespace Certain
