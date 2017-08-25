
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <map>
#include <deque>
#include <future>
#include <forward_list>
#include <unordered_map>
#include <cassert>



namespace paxos {

class PInsAliveState;

} // namespace paxos


namespace paxoskv {


class DBImpl;

class clsActiveTimeoutQueue {

public:
	clsActiveTimeoutQueue(
			size_t iMaxAliveQueueSize)
		: m_iMaxAliveQueueSize(iMaxAliveQueueSize)
	{
		assert(0 < m_iMaxAliveQueueSize);
	}

	void Add(
            const std::string& key, 
            uint64_t llIndex, uint64_t llActivePropNum);

	void BatchAdd(
			const std::vector<
				std::shared_ptr<
                    paxos::PInsAliveState>>& vecPInsAliveState, 
			const std::vector<int>& vecRet);

	std::map<std::string, std::tuple<uint64_t, uint64_t>>
		WaitUntilMap(size_t iMaxWaitTime);

private:
	const size_t m_iMaxAliveQueueSize;

	std::mutex m_tMutex;
	std::condition_variable m_tCV;

	uint64_t m_llAliveTopTS;
	std::deque<
		std::forward_list<
            std::tuple<std::string, uint64_t, uint64_t>>> m_queAlive;
	std::deque<
		std::forward_list<
            std::tuple<std::string, uint64_t, uint64_t>>> m_queTimeout;
};




class PInsAliveStateTimeoutCache {

public:
	PInsAliveStateTimeoutCache(size_t iTimeoutEntry);

	~PInsAliveStateTimeoutCache();

	bool Has(const std::string& key);

	std::shared_ptr<paxos::PInsAliveState>
		Get(const std::string& key, uint64_t llIndex);

	std::tuple<uint64_t, std::shared_ptr<paxos::PInsAliveState>>
		MoreGet(const std::string& key, uint64_t llIndex);

    int Insert(
            std::shared_ptr<paxos::PInsAliveState>& ptrAliveState, 
            bool bMust);

//	std::shared_ptr<paxos::PInsAliveState>
//		Create(const std::string& key, uint64_t llIndex, bool bMust);

	bool Destroy(const std::string& key, uint64_t llIndex);

	bool Destroy(
            const std::string& key, 
            uint64_t llIndex, uint64_t llActivePropNum);

	size_t BatchDestroy(
			const std::vector<
                std::pair<std::string, uint64_t>>& vecLogIdIndex);

    int StartTimeoutThread(DBImpl& oPLogDB);

	void AddTimeout(const paxos::PInsAliveState& oPInsAliveState);

	void BatchAddTimeout(
			const std::vector<
				std::shared_ptr<
                    paxos::PInsAliveState>>& vecPInsAliveState, 
			const std::vector<int>& vecRet);

	clsActiveTimeoutQueue& GetActiveTimeoutQueue() {
		return m_oActiveTimeoutQueue;
	}

private:
	size_t SelectIdx(const std::string& key);


private:
	// KvComm::clsIDCalculator m_oIDCalculator;
	clsActiveTimeoutQueue m_oActiveTimeoutQueue;

	bool m_bStop;
	std::future<void> m_tFutTimeout;

	std::vector<std::unique_ptr<std::mutex>> m_vecMut;
	std::vector<std::unordered_map<
		std::string, 
        std::shared_ptr<paxos::PInsAliveState>>> m_vecMapCache;
}; 

class clsGetLocalOutHelper {

public:
	clsGetLocalOutHelper(
            size_t iMaxTrackSize, uint64_t llHoldIntervalMS);
	
	~clsGetLocalOutHelper();

	void Add(const std::string& key, uint64_t llIndex);

	std::unordered_map<
        std::string, uint64_t> WaitUntil(size_t iWaitTimeout);

	int StartWorker(DBImpl& oPLogDB);

private:
	const size_t m_iMaxTrackSize;
	const uint64_t m_llHoldIntervalMS;

	std::mutex m_tMutex;
	std::condition_variable m_tCV;
	
	std::atomic<size_t> m_iCurrentTrackSize;
	uint64_t m_llPrevTimeMS;
	std::unordered_map<std::string, uint64_t> m_mapNow;
	std::unordered_map<std::string, uint64_t> m_mapPrev;

	bool m_bStop;
	std::future<void> m_tFutWorker;
};


} // namespace paxoskv


