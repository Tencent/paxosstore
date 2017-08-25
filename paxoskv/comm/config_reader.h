
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstring>
#include "iconfig.h"

namespace KvComm {


inline int SmartConfigReadWithDefault(
		Comm::CConfig& oConfig, 
		const char* section, 
		const char* key, int& iValue, const char* sDefaultValue, 
		std::vector<std::string>& suffixList)
{
	// read with default value
	Comm::ConfigItemInfoEx_t infoArray[] = {
		CONFIG_ITEM_EX_INT(section, key, iValue, sDefaultValue),
		CONFIG_ITEM_EX_END
	};

	return Comm::ConfigRead(&oConfig, infoArray, &suffixList);
}

inline int SmartConfigReadWithDefault(
		Comm::CConfig& oConfig, 
		const char* section, 
		const char* key, char sValue[256], const char* sDefaultValue,
		std::vector<std::string>& suffixList)
{
	Comm::ConfigItemInfoEx_t infoArray[] = {
		{section, key, CONFIG_TYPE_STR, \
			(void*)sValue, sizeof(char) * 256, sDefaultValue}, 
		CONFIG_ITEM_EX_END
	};

	return Comm::ConfigRead(&oConfig, infoArray, &suffixList);
}

inline int ConfigReadWithDefault(
		Comm::CConfig& oConfig, 
		const char* section, 
		const char* key, int& iValue, const int iDefaultValue)
{
	Comm::ConfigItemInfo_t infoArray[] = {
		CONFIG_ITEM_INT(section, key, iValue), 
		CONFIG_ITEM_END
	};

	int ret = Comm::ConfigRead(&oConfig, infoArray);
	if (0 > ret)
	{
		iValue = iDefaultValue;
	}

	return ret;
}

inline int ConfigReadWithDefault(
		Comm::CConfig& oConfig, 
		const char* section, 
		const char* key, char sValue[256], const char* sDefaultValue)
{
	Comm::ConfigItemInfo_t infoArray[] = {
		{section, key, CONFIG_TYPE_STR, (void*)sValue, sizeof(char) * 256}, 
		CONFIG_ITEM_END
	};

	int ret = Comm::ConfigRead(&oConfig, infoArray);
	if (0 > ret)
	{
		strncpy(sValue, sDefaultValue, 255);
	}

	return ret;
}




} // namespace KvComm;


