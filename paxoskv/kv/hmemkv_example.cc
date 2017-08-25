
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/




#include <cassert>
#include "dbcomm/db_comm.h"
#include "hard_memkv.h"
#include "db_option.h"




    
    int
main ( int argc, char *argv[] )
{
    paxoskv::clsHardMemKv hmemkv;

    paxoskv::Option option;
    option.db_path = "./example_kvsvr";
    option.db_plog_path = "./example_kvsvr/data";
    assert(0 == dbcomm::CheckAndFixDirPath(option.db_path.c_str()));
    assert(0 == dbcomm::CheckAndFixDirPath(option.db_plog_path.c_str()));

    auto ret = hmemkv.Init(option);
    assert(0 == ret);


    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
