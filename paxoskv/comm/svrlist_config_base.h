
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <vector>
#include <stdint.h>


namespace Comm {
	class CConfig;
}

class clsConHash;
class param;

struct SvrAddr_t;
struct SvrGroup_t;
struct SvrGroupList_t;

// clsSvrListConfigBase DO NOT PROVIDE virtual destructor !!
// DO NOT TRY TO DELETE A DERVITED CLASS THROUGH clsSvrListConfigBase
class clsSvrListConfigBase
{
public:
	void GetSvrGroupList( SvrGroupList_t& grouplist ); //for _selfIP

	SvrGroup_t *GetBySect( uint32_t sect ); //for all

	int GetSvrSetIdx(uint32_t sect); // for all

	int GetAllDataSvr(std::vector<SvrAddr_t>& vecDataSvr);

    std::vector< SvrGroup_t * > *GetGroups();

	uint32_t GetFailCount()
	{
		return _iFailCount;
	}

	uint32_t GetFailPeriod()
	{
		return _iFailPeriod;
	}

	uint32_t GetTimeOut()
	{
		return _iTimeOut;
	}

	int GetConsistentHash()
	{
		return _iConsistentHash;
	}

	void PrintSvrList();

	clsConHash* GetConHash();

	int GetSelfIdx(); // AB 0 - 6 , C -2 ~ ..

	static void OpenTest();

	const char* GetSvrListPath()
	{
		return _svrListPath;
	}

protected:
	clsSvrListConfigBase();
	~clsSvrListConfigBase();

	void Reset();

	int LoadConfigFromCConfig(Comm::CConfig& reader);

	int LoadConfigFromCConfigOld(Comm::CConfig& reader);

	int LoadConfigFromCConfigNew(Comm::CConfig& reader);

	void SetSelfIP(const char* sIP);
	const char* GetSelfIP()
	{
		return _selfIP;
	}

	void SetSvrListPath(const char* sSvrListPath);

	void OpenTestFlag();

private:
	char _selfIP[16];
	char _svrListPath[256];
	SvrGroupList_t * _grouplist; //_selfIP's GroupList

	uint32_t _iFailPeriod;
	uint32_t _iFailCount;
	uint32_t _iTimeOut;
	uint32_t _iOssAttrID; //no use
	uint32_t _iGroupCount;
	uint32_t _iConsistentHash;
	uint32_t _iServerCount;
	clsConHash *_poConHash;


	// for 4all
	bool _bOpenTest;
	param* _old;
	param* _curr;
};


class clsSillySvrListConfig : public clsSvrListConfigBase
{
public:
	clsSillySvrListConfig(const char* sSvrListConf);

	int UpdateConfig();
};


