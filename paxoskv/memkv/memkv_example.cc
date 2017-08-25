
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <string>
#include <cassert>
#include "memkv/memkv.h"
#include "memkv/memloader.h"
#include "memkv/pmergetor.h"
#include "dbcomm/db_comm.h"


    int
main ( int argc, char *argv[] )
{
    int ret = 0;
    memkv::clsNewMemKv memkv;

    const std::string plog_path = "./data";
    assert(0 == dbcomm::CheckAndFixDirPath(plog_path));

    ret = memkv.Init(
            100 * 10000, plog_path.c_str(), 2);
    assert(0 == ret);

    ret = memkv.StartBuildIdx();
    assert(0 == ret);

    std::set<uint64_t> chosen_keys;
    ret = memkv.ReloadDiskFile(
            false, 
            [=](memkv::clsNewMemKv& memkv) {
                return memkv::LoadAllDiskFile(
                        plog_path, memkv, size_t{8});
            }, 
            [&](memkv::clsNewMemKv& memkv) {
                return memkv::LoadMostRecentWriteFile(
                        plog_path, memkv, size_t{500}, chosen_keys);
            });
    assert(0 == ret);

    ret = memkv.StartMemMergeThread();
    assert(0 == ret);

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
