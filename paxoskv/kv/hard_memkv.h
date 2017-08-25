
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <string>
#include <memory>
#include <vector>
#include "cutils/async_worker.h"


namespace memkv {
    
class clsNewMemKv;
class PMergetor;

}

namespace dbcomm {

class NewStorage;

}

namespace paxos {

class PaxosLogHeader;
class PaxosLog;

} // namespace paxos

namespace paxoskv {


struct Option;

class clsHardMemKv {

public:
    clsHardMemKv();

    ~clsHardMemKv();

    int Init(const Option& option);

    int Read(const std::string& key, 
            paxos::PaxosLogHeader& header, 
            paxos::PaxosLog& plog);

    int ReadHeader(
            const std::string& key, 
            paxos::PaxosLogHeader& header);

    int Write(
            const std::string& key, 
            const paxos::PaxosLogHeader& header, 
            const paxos::PaxosLog& plog);


private:
    std::unique_ptr<memkv::clsNewMemKv> memkv_;
    std::unique_ptr<dbcomm::NewStorage> storage_;
    std::unique_ptr<memkv::PMergetor> mergetor_;

    std::vector<std::unique_ptr<cutils::AsyncWorker>> workers_;
}; // class clsHardMemKv;


void update_option(clsHardMemKv& hmemkv, Option& option);


} // namespace paxoskv


