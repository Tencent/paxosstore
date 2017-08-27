
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "core/pins_wrapper.h"
#include "cutils/log_utils.h"
#include "cutils/mem_utils.h"
#include "cutils/hash_utils.h"
#include "dbcomm/db_comm.h"
#include "comm/kvsvrcomm.h"
#include "db_helper.h"
#include "db_impl.h"


namespace {

template <typename ListType>
// std::forward_list<EntryType>& GetEntries(
ListType& GetEntries(
		const uint64_t llNowTS, 
		std::deque<ListType>& queAlive, 
		// std::deque<std::forward_list<EntryType>>& queAlive, 
		uint64_t& llAliveTopTS)
{
	if (false == queAlive.empty()) {
		if (llNowTS <= llAliveTopTS) {
			return queAlive.back();
		}
	}

	llAliveTopTS = llNowTS;
	queAlive.emplace_back();
	return queAlive.back();
}

template <typename ListType>
void MoveAliveIntoTimeout(
		const size_t iMaxAliveQueueSize, 
		std::deque<ListType>& queAlive, 
		std::deque<ListType>& queTimeout)
{
	while (queAlive.size() > iMaxAliveQueueSize) {
		queTimeout.emplace_back(std::move(queAlive.front()));
		assert(queAlive.front().empty());
		queAlive.pop_front();
	}
}

template <typename ListType>
std::map<uint64_t, uint64_t>
PackTimeoutMap(std::deque<ListType>& queTimeout)
{
	std::map<uint64_t, uint64_t> mapTimeout;
	for (size_t idx = 0; idx < queTimeout.size(); ++idx) {
		for (const auto& entry : queTimeout[idx]) {
			uint64_t llLogID = std::get<0>(entry);
			uint64_t llIndex = std::get<1>(entry);

			auto iter = mapTimeout.find(llLogID);
			if (mapTimeout.end() == iter) {
				mapTimeout[llLogID] = llIndex;
			}
			else {
				assert(iter->first == llLogID);
				iter->second = std::max(iter->second, llIndex);
			}
		}
	}

	return mapTimeout;
}


//void ActiveTimeoutWorker(
//		KvsvrConfig* poConfig, 
//		KVDB::DBImpl& oPLogDB, 
//		KVDB::PInsRedoTracer* poRedoTracer, 
//		bool& bStop);

void GetLocalOutHelperWorker(
		paxoskv::clsGetLocalOutHelper& oHelper, 
		paxoskv::DBImpl& oPLogDB, 
		bool& bStop)
{
	BindToLastCpu();
	logerr("Start");

	while (false == bStop)
	{
		if (0 == access("/home/qspace/data/kvsvr/stop_catch_up", F_OK))
		{
			sleep(5);
			continue;
		}

		auto mapKey = oHelper.WaitUntil(100);
		if (mapKey.empty())
		{
			continue;
		}

		// else
		assert(false == mapKey.empty());

		std::vector<std::string> vecKey;
		vecKey.reserve(mapKey.size());
		for (auto iter = mapKey.begin(); iter != mapKey.end(); ++iter)
		{
            const std::string& key = iter->first;
            // TODO: check before Trigger;
//			uint64_t llIndex = iter->second;
//
//            NewBasic_t tBasicInfo = {0};
//			int ret = oPLogDB.GetBasicInfo(key, tBasicInfo);
//			if (0 != ret) {
//				logerr("DBImpl::GetBasicInfo ret %d", ret);
//				continue;
//			}
//
//			if (tBasicInfo.llMaxIndex != llIndex)
//			{
//				logerr("IGNORE index %lu tBasicInfo.llMaxIndex %lu", 
//						llIndex, tBasicInfo.llMaxIndex);
//				continue;
//			}

			vecKey.push_back(key);
		}

		if (vecKey.empty())
		{
			continue;
		}

		assert(false == vecKey.empty());
		logerr("TESTINFO vecKey.size %zu mapKey.size %zu", 
				vecKey.size(), mapKey.size());
		std::vector<uint8_t> vecPeerID(vecKey.size(), 0);
		oPLogDB.BatchTriggerCatchUpOn(vecKey, vecPeerID);
	}

	logerr("Stop");
}


void ActiveTimeoutWorker(
		paxoskv::DBImpl& oPLogDB, 
        paxoskv::PInsAliveStateTimeoutCache& oASTimeoutCache, 
		bool& bStop)
{
    BindWorkerCpu();

	logerr("START WORKER");

	auto& oActiveTimeoutQueue = oASTimeoutCache.GetActiveTimeoutQueue();

	int iLoopCnt = 0;
	uint64_t llProcessCnt = 0;
	while (false == bStop) {
		++iLoopCnt;
		if (0 == (iLoopCnt % 10000)) {
			logerr("TESTSTAT queuename ACTIVE iLoopCnt %d "
                    "llProcessCnt %lu", 
					iLoopCnt, llProcessCnt);
		}

		auto mapTimeout = oActiveTimeoutQueue.WaitUntilMap(100);
		if (mapTimeout.empty()) {
			continue;
		}

		assert(false == mapTimeout.empty());
		llProcessCnt += mapTimeout.size();
		for (const auto entry : mapTimeout) {
            const std::string& key = entry.first;
			uint64_t llIndex = std::get<0>(entry.second);
			uint64_t llActivePropNum = std::get<1>(entry.second);

			if (false == oASTimeoutCache.Destroy(
						key, llIndex, llActivePropNum)) {
				continue;
			}

			// else
			logdebug("ALIVEINFO: Destroy index %lu active %lu", 
					llIndex, llActivePropNum);
		}
	}
}



} // namespace ;


namespace paxoskv {


void clsActiveTimeoutQueue::Add(
        const std::string& key, 
        uint64_t llIndex, uint64_t llActivePropNum)
{
	uint64_t llNow = dbcomm::GetTickMS();
	{
		std::lock_guard<std::mutex> lock(m_tMutex);

		auto& entries = GetEntries(llNow, m_queAlive, m_llAliveTopTS);
		// auto& entries = GetEntry(llNow, m_queAlive, m_llAliveTopTS);
		entries.emplace_front(key, llIndex, llActivePropNum);

		if (m_iMaxAliveQueueSize >= m_queAlive.size()) {
			return ;
		}

		MoveAliveIntoTimeout(
                m_iMaxAliveQueueSize, m_queAlive, m_queTimeout);
	}

	m_tCV.notify_one();
}

void clsActiveTimeoutQueue::BatchAdd(
		const std::vector<
			std::shared_ptr<paxos::PInsAliveState>>& vecPInsAliveState, 
		const std::vector<int>& vecRet)
{
	assert(vecPInsAliveState.size() == vecRet.size());
	if (vecPInsAliveState.empty()) {
		return ;
	}

	bool bNeedAdd = false;
	for (size_t idx = 0; idx < vecPInsAliveState.size(); ++idx) {
		if (0 != vecRet[idx]) {
			assert(nullptr == vecPInsAliveState[idx]);
			continue;
		}

		assert(0 == vecRet[idx]);
		assert(nullptr != vecPInsAliveState[idx]);
		bNeedAdd = true;
		break;
	}

	if (false == bNeedAdd) {
		return ;
	}

	uint64_t llNow = dbcomm::GetTickMS();
	{
		std::lock_guard<std::mutex> lock(m_tMutex);

		// auto& entries = GetEntry(llNow, m_queAlive, m_llAliveTopTS);
		auto& entries = GetEntries(llNow, m_queAlive, m_llAliveTopTS);

		uint32_t iAddTimeoutCnt = 0;
		for (size_t idx = 0; idx < vecPInsAliveState.size(); ++idx) {
			if (0 != vecRet[idx]) {
				assert(nullptr == vecPInsAliveState[idx]);
				continue;
			}

			assert(0 == vecRet[idx]);
			assert(nullptr != vecPInsAliveState[idx]);

			const auto& oPInsAliveState = *(vecPInsAliveState[idx]);
			entries.emplace_front(
					oPInsAliveState.GetKey(), 
					oPInsAliveState.GetIndex(), 
					oPInsAliveState.GetActiveBeginProposedNum());
			++iAddTimeoutCnt;
		}

		if (m_iMaxAliveQueueSize >= m_queAlive.size()) {
			return ;
		}

		MoveAliveIntoTimeout(
                m_iMaxAliveQueueSize, m_queAlive, m_queTimeout);
	}

	m_tCV.notify_one();
}


std::map<std::string, std::tuple<uint64_t, uint64_t>>
clsActiveTimeoutQueue::WaitUntilMap(size_t iMaxWaitTime)
{
	std::deque<std::forward_list<
		std::tuple<std::string, uint64_t, uint64_t>>> queTimeout;
	{
		std::chrono::system_clock::time_point tp = 
			std::chrono::system_clock::now() + 
			std::chrono::milliseconds{iMaxWaitTime};

		std::unique_lock<std::mutex> lock(m_tMutex);
		m_tCV.wait_until(lock, tp, 
				[&]() {
					return false == m_queTimeout.empty();
				});

		m_queTimeout.swap(queTimeout);
		assert(m_queTimeout.empty());
	}

	std::map<std::string, std::tuple<uint64_t, uint64_t>> mapTimeout;
	for (size_t idx = 0; idx < queTimeout.size(); ++idx) {
		for (const auto& entry : queTimeout[idx]) {
            const std::string& key = std::get<0>(entry);
			uint64_t llIndex = std::get<1>(entry);
			uint64_t llActivePropNum = std::get<2>(entry);

			auto iter = mapTimeout.find(key);
			if (mapTimeout.end() == iter) {
				std::get<0>(mapTimeout[key]) = llIndex;
				std::get<1>(mapTimeout[key]) = llActivePropNum;
			}
			else {
				assert(iter->first == key);
				uint64_t& llMapIndex = std::get<0>(iter->second);
				uint64_t& llMapActivePropNum = std::get<1>(iter->second);
				if (llIndex == llMapIndex) {
					llMapActivePropNum = std::max(
                            llActivePropNum, llMapActivePropNum);
				}
				else {
					if (llIndex > llMapIndex) {
						llMapIndex = llIndex;
						llMapActivePropNum = llActivePropNum;
					}
				}
			}
		}
	}

	return mapTimeout;
}




PInsAliveStateTimeoutCache::PInsAliveStateTimeoutCache(
        size_t iTimeoutEntry)
	: m_oActiveTimeoutQueue(iTimeoutEntry)
	, m_bStop(false)
{
	const size_t iSize = 3989;
	// const size_t iSize = 7919;

	m_vecMut.resize(iSize);
	for (size_t idx = 0; idx < iSize; ++idx)
	{
		m_vecMut[idx] = cutils::make_unique<std::mutex>();
		assert(NULL != m_vecMut[idx]);
	}

	m_vecMapCache.resize(iSize);
}

PInsAliveStateTimeoutCache::~PInsAliveStateTimeoutCache()
{
	m_bStop = true;
	if (m_tFutTimeout.valid())
	{
		m_tFutTimeout.get();
	}
}

size_t PInsAliveStateTimeoutCache::SelectIdx(const std::string& key)
{
	assert(false == m_vecMut.empty());
	assert(m_vecMut.size() == m_vecMapCache.size());
    uint32_t iHash = cutils::bkdr_hash(key.c_str());
	return iHash % m_vecMut.size();
}

void PInsAliveStateTimeoutCache::AddTimeout(
		const paxos::PInsAliveState& oPInsAliveState)
{
	m_oActiveTimeoutQueue.Add(
			oPInsAliveState.GetKey(), 
			oPInsAliveState.GetIndex(), 
			oPInsAliveState.GetActiveBeginProposedNum());
}

void PInsAliveStateTimeoutCache::BatchAddTimeout(
		const std::vector<
			std::shared_ptr<paxos::PInsAliveState>>& vecPInsAliveState, 
		const std::vector<int>& vecRet)
{
	m_oActiveTimeoutQueue.BatchAdd(vecPInsAliveState, vecRet);
}

int PInsAliveStateTimeoutCache::Insert(
        std::shared_ptr<paxos::PInsAliveState>& ptrAliveState, 
        bool bMust)
{
    if (nullptr == ptrAliveState) {
        return 0;
    }

    assert(nullptr != ptrAliveState);
    const auto& key = ptrAliveState->GetKey();
    const size_t idx = SelectIdx(key);
    auto& tMutex = *(m_vecMut[idx]);
    auto& mapCache = m_vecMapCache[idx];

    std::lock_guard<std::mutex> lock(tMutex);
    if (5 < mapCache.size() && false == bMust) {
        logerr("REACH LIMIT mapCache.size %zu", mapCache.size());
        return -1;
    }


    if (mapCache.end() == mapCache.find(key)) {
        mapCache[key] = ptrAliveState;
        return 0;
    }

    auto ptrPrevAliveState = mapCache.at(key);
    assert(nullptr != ptrPrevAliveState);
    assert(ptrPrevAliveState->GetIndex() <= ptrAliveState->GetIndex());
    logerr("INFO: prev.index %lu curr.index %lu", 
            ptrPrevAliveState->GetIndex(), 
            ptrAliveState->GetIndex());
    mapCache[key] = ptrAliveState;
    // notify out of date..
    ptrPrevAliveState->SendNotify();
    return 1;
}

//std::shared_ptr<paxos::PInsAliveState>
//PInsAliveStateTimeoutCache::Create(
//		uint64_t llLogID, uint64_t llIndex, bool bMust)
//{
//	const size_t idx = SelectIdx(llLogID);
//	std::mutex& m_tMutex = *(m_vecMut[idx]);
//	std::unordered_map<uint64_t, std::shared_ptr<paxos::PInsAliveState>>& m_mapCache = m_vecMapCache[idx];
//
//	std::lock_guard<std::mutex> lock(m_tMutex);
//	if (m_mapCache.end() != m_mapCache.find(llLogID))
//	{
//		if (false == bMust) {
//			return nullptr;
//		}
//
//		assert(true == bMust);
//		assert(NULL != m_mapCache.at(llLogID));
//		paxos::PInsAliveState* poPInsAliveState = m_mapCache.at(llLogID).get();
//		assert(NULL != poPInsAliveState);
//
//		hassert(poPInsAliveState->GetIndex() <= llIndex, "%lu %lu", 
//				poPInsAliveState->GetIndex(), llIndex);
//		if (poPInsAliveState->GetIndex() == llIndex)
//		{
//			logerr("TEST IMPORTANT !!! key %lu index %lu re-create IsChosen %d", 
//					llLogID, poPInsAliveState->GetIndex(), 
//					poPInsAliveState->IsChosen());
//		}
//
//		logerr("ALIVEINFO key %lu index %lu erase", 
//				llLogID, m_mapCache.at(llLogID)->GetIndex());
//		m_mapCache.erase(llLogID);
//	}
//	
//	// force for now
//	if (5 < m_mapCache.size() && false == bMust)
//	{
//		logerr("LIMIT m_mapCache.size %zu key %lu index %lu", 
//				m_mapCache.size(), llLogID, llIndex);
//		return nullptr;
//	}
//
//	// assert(20000 >= m_mapCache.size());
//	assert(m_mapCache.end() == m_mapCache.find(llLogID));
//	uint64_t llProposeNum = 
//		cutils::prop_num_compose(m_oIDCalculator(llLogID), 0);
//	m_mapCache.emplace(
//			std::make_pair(
//				llLogID, 
//				std::make_shared<paxos::PInsAliveState>(
//					llLogID, llIndex, llProposeNum)));
//
//	static __thread size_t iCount = 0;
//	static __thread size_t iMaxCacheSizeSoFar = 0;
//	++iCount;
//	iMaxCacheSizeSoFar = std::max(m_mapCache.size(), iMaxCacheSizeSoFar);
//	if (0 == iCount % 1000)
//	{
//		logerr("MEMLEAK iCount %zu iMaxCacheSizeSoFar %zu m_mapCache.size %zu %zu", 
//				iCount, iMaxCacheSizeSoFar, 	
//				m_mapCache.size(), sizeof(paxos::PInsAliveState));
//	}
//	assert(NULL != m_mapCache.at(llLogID));
//	assert(m_mapCache.at(llLogID)->GetLogID() == llLogID);
//	return m_mapCache.at(llLogID);
//}

bool PInsAliveStateTimeoutCache::Has(const std::string& key)
{
	size_t idx = SelectIdx(key);
	auto& tMutex = *(m_vecMut[idx]);
	auto& mapCache = m_vecMapCache[idx];

	std::lock_guard<std::mutex> lock(tMutex);
	if (mapCache.end() == mapCache.find(key)) {
		return false;
	}

	assert(nullptr != mapCache.at(key));
	return true;
}

std::shared_ptr<paxos::PInsAliveState>
PInsAliveStateTimeoutCache::Get(
        const std::string& key, uint64_t llIndex)
{
	const size_t idx = SelectIdx(key);
	std::mutex& tMutex = *(m_vecMut[idx]);
	auto& mapCache = m_vecMapCache[idx];

	std::lock_guard<std::mutex> lock(tMutex);
	if (mapCache.end() == mapCache.find(key))
	{
		return NULL;
	}

	assert(NULL != mapCache.at(key));
	if (llIndex != mapCache.at(key)->GetIndex())
	{
		return NULL;
	}

	assert(llIndex == mapCache.at(key)->GetIndex());
	if (mapCache.at(key)->IsChosen())
	{
		return NULL;
	}

	return mapCache.at(key);
}

std::tuple<uint64_t, std::shared_ptr<paxos::PInsAliveState>>
PInsAliveStateTimeoutCache::MoreGet(
        const std::string& key, uint64_t llIndex)
{
	const size_t idx = SelectIdx(key);
	std::mutex& tMutex = *(m_vecMut[idx]);
    auto& mapCache = m_vecMapCache[idx];

	std::lock_guard<std::mutex> lock(tMutex);
	if (mapCache.end() == mapCache.find(key))
	{
		return std::make_tuple(0, nullptr);
	}

	assert(NULL != mapCache.at(key));
	uint64_t llCacheIndex = mapCache.at(key)->GetIndex();
	if (llIndex != mapCache.at(key)->GetIndex())
	{
		return std::make_tuple(llCacheIndex, nullptr);
	}

	assert(llIndex == mapCache.at(key)->GetIndex());
	if (mapCache.at(key)->IsChosen())
	{
		return std::make_tuple(0, nullptr);
	}

	return std::make_tuple(0, mapCache.at(key));
}

bool PInsAliveStateTimeoutCache::Destroy(
        const std::string& key, uint64_t llIndex)
{
	const size_t idx = SelectIdx(key);
	std::mutex& tMutex = *(m_vecMut[idx]);
    auto& mapCache = m_vecMapCache[idx];

	std::lock_guard<std::mutex> lock(tMutex);
	if (mapCache.end() == mapCache.find(key))
	{
		return false;
	}

	assert(NULL != mapCache.at(key));
	assert(mapCache.at(key)->GetKey() == key);
	if (mapCache.at(key)->GetIndex() == llIndex)
	{
		mapCache.erase(key);
		logdebug("ALIVEINFO index %lu erase", llIndex);
		return true;
	}

	return false;
}

bool PInsAliveStateTimeoutCache::Destroy(
		const std::string& key, 
        uint64_t llIndex, uint64_t llActivePropNum)
{
	assert(0 < llIndex);
	const size_t idx = SelectIdx(key);
	auto& tMutex = *(m_vecMut[idx]);
	auto& mapCache = m_vecMapCache[idx];

	{
		std::lock_guard<std::mutex> lock(tMutex);
		auto iter = mapCache.find(key);
		if (mapCache.end() != iter) {
			assert(key == iter->first);
			assert(nullptr != iter->second);
			if (iter->second->GetIndex() == llIndex && 
					iter->second->GetActiveBeginProposedNum() == 
						llActivePropNum) {
				mapCache.erase(key);
				return true;
			}
		}
	}

	return false;
}

size_t PInsAliveStateTimeoutCache::BatchDestroy(
		const std::vector<
            std::pair<std::string, uint64_t>>& vecLogIdIndex)
{
	size_t iDestroyCnt = 0;
	for (size_t idx = 0; idx < vecLogIdIndex.size(); ++idx)
	{
		const size_t iSelectIdx = SelectIdx(vecLogIdIndex[idx].first);
		std::mutex& tMutex = *(m_vecMut[iSelectIdx]);
        auto& mapCache = m_vecMapCache[iSelectIdx];

		std::lock_guard<std::mutex> lock(tMutex);
		if (mapCache.end() == mapCache.find(vecLogIdIndex[idx].first))
		{
			continue;
		}

        const std::string& key = vecLogIdIndex[idx].first;
		uint64_t llIndex = vecLogIdIndex[idx].second;
		assert(NULL != mapCache.at(key));
		assert(mapCache.at(key)->GetKey() == key);
		if (mapCache.at(key)->GetIndex() == llIndex)
		{
			mapCache.erase(key);
			++iDestroyCnt;
			logerr("ALIVEINFO index %lu erase", llIndex);
		}
	}

	return iDestroyCnt;
}

int PInsAliveStateTimeoutCache::StartTimeoutThread(DBImpl& oPLogDB)
{
 	assert(false == m_bStop);
 	if (m_tFutTimeout.valid())
 	{
 		return 0;
 	}
 
 	assert(false == m_tFutTimeout.valid());
 	m_tFutTimeout = std::async(
 			std::launch::async, 
 			ActiveTimeoutWorker, 
 			std::ref(oPLogDB), std::ref(*this), std::ref(m_bStop));
 	assert(m_tFutTimeout.valid());
    return 0;
}


clsGetLocalOutHelper::clsGetLocalOutHelper(
		size_t iMaxTrackSize, uint64_t llHoldIntervalMS)
	: m_iMaxTrackSize(iMaxTrackSize)
	, m_llHoldIntervalMS(llHoldIntervalMS)
	, m_iCurrentTrackSize(0)
	, m_llPrevTimeMS(dbcomm::GetTickMS())
	, m_bStop(false)
{
	assert(0 < m_iMaxTrackSize);
	assert(0 < m_llHoldIntervalMS);
	m_mapNow.rehash(m_iMaxTrackSize * 2);
	m_mapPrev.rehash(m_iMaxTrackSize * 2);
}

clsGetLocalOutHelper::~clsGetLocalOutHelper()
{
	m_bStop = true;
	m_tCV.notify_all();
	if (m_tFutWorker.valid())
	{
		m_tFutWorker.get();
	}
}

void clsGetLocalOutHelper::Add(
        const std::string& key, uint64_t llIndex)
{
	static __thread uint64_t llPrevTimeMS = 0;

	uint64_t llNowTimeMS = dbcomm::GetTickMS();
	if (llPrevTimeMS + m_llHoldIntervalMS >= llNowTimeMS)
	{
		if (m_iCurrentTrackSize.load(
					std::memory_order_relaxed) >= m_iMaxTrackSize)
		{
			return ;
		}
		// else
	}
	else
	{
		llPrevTimeMS = llNowTimeMS;
	}

	bool bDoNotify = false;
	bool bIncTrackSize = false;
	{
		std::lock_guard<std::mutex> lock(m_tMutex);
		if (m_llPrevTimeMS + m_llHoldIntervalMS < llNowTimeMS)
		{
			m_mapPrev.swap(m_mapNow);
			m_mapNow.clear();

			m_llPrevTimeMS = llNowTimeMS;
			bDoNotify = true;
		}
		else
		{
			if (m_mapNow.size() >= m_iMaxTrackSize)
			{
				return ;
			}
		}

		bIncTrackSize = m_mapNow.end() == m_mapNow.find(key);
		m_mapNow[key] = std::max(m_mapNow[key], llIndex);
	}

	if (bDoNotify)
	{
		m_iCurrentTrackSize.store(1, std::memory_order_relaxed);
		m_tCV.notify_one();
	}
	else
	{
		if (bIncTrackSize)
		{
			m_iCurrentTrackSize.fetch_add(1, std::memory_order_relaxed);
		}
	}
}


std::unordered_map<std::string, uint64_t> 
clsGetLocalOutHelper::WaitUntil(size_t iWaitTimeout)
{
	std::chrono::system_clock::time_point tp = 
		std::chrono::system_clock::now() + 
		std::chrono::milliseconds{iWaitTimeout};

	std::unordered_map<std::string, uint64_t> mapKey;
	mapKey.rehash(m_iMaxTrackSize * 2);
	{
		std::unique_lock<std::mutex> lock(m_tMutex);
		m_tCV.wait_until(lock, tp, 
				[&]() {
					return false == m_mapPrev.empty();
				});

		if (m_mapPrev.empty()) 
		{
			return mapKey;
		}

		m_mapPrev.swap(mapKey);
	}

	return mapKey;
}

int clsGetLocalOutHelper::StartWorker(DBImpl& oPLogDB)
{
	assert(false == m_tFutWorker.valid());
	assert(false == m_bStop);
	m_tFutWorker = std::async(
			std::launch::async, 
			GetLocalOutHelperWorker, 
			std::ref(*this), std::ref(oPLogDB), std::ref(m_bStop));
	assert(m_tFutWorker.valid());
	return 0;
}




} // namespace paxoskv;


