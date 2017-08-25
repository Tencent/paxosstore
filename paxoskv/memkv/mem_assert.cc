
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <string>
#include <cassert>
#include "core/plog_helper.h"
#include "core/paxos.pb.h"
#include "dbcomm/db_comm.h"
#include "mem_assert.h"


namespace memkv {


void CheckConsistency(
		const NewBasic_t& tBasicInfo, 
		const paxos::PaxosLog& oPLog)
{
    assert(tBasicInfo.llMaxIndex == paxos::get_max_index(oPLog));
    assert(tBasicInfo.llChosenIndex == paxos::get_chosen_index(oPLog));
    assert(tBasicInfo.llReqID == paxos::get_chosen_reqid(oPLog));

	assert(dbcomm::TestFlag(
                tBasicInfo.cState, PENDING) == 
            paxos::has_pending_ins(oPLog));
    if (false == paxos::has_pending_ins(oPLog)) {
        assert(tBasicInfo.llChosenIndex == tBasicInfo.llMaxIndex);
    }
    else {
        assert(tBasicInfo.llChosenIndex < tBasicInfo.llMaxIndex);
    }

    auto chosen_ins = paxos::get_chosen_ins(oPLog);
    if (nullptr == chosen_ins || 
            0 == chosen_ins->index()) {
        assert(0 == tBasicInfo.iVersion);
        return ;
    }

    assert(nullptr != chosen_ins);
    assert(chosen_ins->has_accepted_value());
    paxos::DBData data;
    assert(data.ParseFromString(chosen_ins->accepted_value().data()));
    assert(data.version() == tBasicInfo.iVersion);
    return ;
}


void AssertCheck(
        const NewBasic_t& tBasicInfo, 
        const paxos::PaxosLog& oPLog)
{
    assert(tBasicInfo.llMaxIndex == paxos::get_max_index(oPLog));
    assert(tBasicInfo.llChosenIndex == paxos::get_chosen_index(oPLog));
    assert(tBasicInfo.llReqID == paxos::get_chosen_reqid(oPLog));

    assert(dbcomm::TestFlag(
                tBasicInfo.cState, PENDING) == 
            paxos::has_pending_ins(oPLog));
    return ;
}




} // namespace memkv


