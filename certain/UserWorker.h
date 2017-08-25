
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_USERWORKER_H_
#define CERTAIN_USERWORKER_H_

#include "Certain.h"
#include "IOWorker.h"

namespace Certain
{

class clsUserWorker : public clsThreadBase,
                      public clsSingleton<clsUserWorker>
{
private:
    typedef clsCircleQueue<clsClientCmd *> clsUserQueue;
    clsUserQueue *m_poUserQueue;

public:

	int PushUserCmd(clsClientCmd *poCmd)
    {
        int iRet = m_poUserQueue->PushByMultiThread(poCmd);
        if (iRet != 0)
        {
            CertainLogError("PushByMultiThread ret %d", iRet);
            return -2;
        }

        return 0;
    }

	void Run();
};

}; // namespace Certain

#endif // CERTAIN_USERWORKER_H_
