
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cassert>
#include "cutils/log_utils.h"
#include "id_calculator.h"
#include "svrlist_config_base.h"
#include "svr_route.h"


clsIDCalculator::clsIDCalculator(
		clsSvrListConfigBase* poConfig, 
		const SvrAddr_t& tSelfAddr)
	: m_poConfig(poConfig)
	, m_tSelfAddr(tSelfAddr)
{
	assert(NULL != m_poConfig);
}

uint8_t clsIDCalculator::operator()(uint64_t llLogID)
{
	uint8_t selfid = 0;
	int ret = clsSvrRoute::GetRoleByKey4All(
			m_poConfig, m_tSelfAddr, 
			reinterpret_cast<char*>(&llLogID), sizeof(llLogID));
	switch (ret)
	{
	case MACHINE_A:
		selfid = 1;
		break;
	case MACHINE_B:
		selfid = 2;
		break;
	case MACHINE_C:
		selfid = 3;
		break;
	default:
		logerr("clsSvrRoute::GetRoleByKey4All "
                "key %lu ret %d", llLogID, ret);
		selfid = 0; // invalid selfid
		break;
	}

	return selfid;
}

