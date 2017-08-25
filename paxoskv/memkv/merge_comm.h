
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <string>




namespace memkv {

enum {
	MERGE_MODE_NIL = 0, 
	MERGE_MODE_IMMEDIATE = 1, 
	MERGE_MODE_NORMAL = 2,
	MERGE_MODE_URGENT = 3, 
};


namespace CheckMerge {

bool CheckFile(const char* sKvPath);

int GetDataFileCountOn(const std::string& sDirPath);

bool CheckUsageAgainstRecycle(
		const std::string& sKvPath, 
		const std::string& sKvRecyclePath, 
		int iMergeRatio);

bool CheckDiskRatio(
		const char* sKvPath, const int iMaxDiskRatio);

bool CheckTime(const int iMergeCount, const int* arrMergeTime);


} // namespace CheckMerge


void Print(const std::vector<int>& vecFile, const char* sMsg);


} // namespace memkv

