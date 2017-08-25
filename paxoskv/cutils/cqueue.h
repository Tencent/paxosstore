
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include "log_utils.h"

namespace cutils {

template <typename EntryType>
class CQueue {
   public:
    CQueue(const std::string& queue_name, const size_t max_queue_size)
        : queue_name_(queue_name), max_queue_size_(max_queue_size) {}

    void Push(std::unique_ptr<EntryType> item) {
        size_t iSize = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            msg_.emplace(std::move(item));
            assert(nullptr != msg_.back());
            if (max_queue_size_ < msg_.size()) {
                msg_.pop();
            }
            assert(max_queue_size_ >= msg_.size());

            iSize = msg_.size();
        }

        cv_.notify_one();
        static __thread size_t iCount = 0;
        static __thread size_t iMaxSizeSoFar = 0;

        iMaxSizeSoFar = std::max(iSize, iMaxSizeSoFar);
        ++iCount;
        if (0 == iCount % 1000) {
            logerr("MEMLEAK: %s iCount %zu iMaxSizeSoFar %zu iSize %zu",
                   queue_name_.c_str(), iCount, iMaxSizeSoFar, iSize);
        }
    }

    void BatchPush(std::vector<std::unique_ptr<EntryType>> vec_item) {
        if (true == vec_item.empty()) {
            return;
        }

        size_t iSize = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (auto& item : vec_item) {
                msg_.emplace(std::move(item));
                assert(nullptr != msg_.back());
            }

            for (size_t idx = 0; idx < vec_item.size(); ++idx) {
                if (max_queue_size_ >= msg_.size()) {
                    break;
                }

                msg_.pop();
            }
            assert(max_queue_size_ >= msg_.size());

            iSize = msg_.size();
        }

        static __thread size_t iCount = 0;
        static __thread size_t iMaxSizeSoFar = 0;
        iMaxSizeSoFar = std::max(iSize, iMaxSizeSoFar);
        ++iCount;
        if (0 == iCount % 1000) {
            logerr("MEMLEAK: %s iCount %zu iMaxSizeSoFar %zu iSize %zu",
                   queue_name_.c_str(), iCount, iMaxSizeSoFar, iSize);
        }

        if (size_t{1} == vec_item.size()) {
            cv_.notify_one();
            return;
        }

        cv_.notify_all();
    }

    std::unique_ptr<EntryType> Pop() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (msg_.empty()) {
                cv_.wait(lock, [&]() { return !msg_.empty(); });
            }

            assert(false == msg_.empty());
            auto item = move(msg_.front());
            msg_.pop();
            return item;
        }
    }

    std::unique_ptr<EntryType> Pop(std::chrono::microseconds timeout) {
        auto time_point = std::chrono::system_clock::now() + timeout;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (false == cv_.wait_until(lock, time_point, [&]() {
                    return !msg_.empty();
                })) {
                // timeout
                return nullptr;
            }

            assert(false == msg_.empty());
            auto item = move(msg_.front());
            msg_.pop();
            return item;
        }
    }

    std::vector<std::unique_ptr<EntryType>> BatchPop(size_t iMaxBatchSize) {
        std::vector<std::unique_ptr<EntryType>> vec;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (msg_.empty()) {
                cv_.wait(lock, [&]() { return !msg_.empty(); });
            }

            assert(false == msg_.empty());
            while (false == msg_.empty() && vec.size() < iMaxBatchSize) {
                assert(nullptr != msg_.front());
                auto item = std::move(msg_.front());
                msg_.pop();
                assert(nullptr != item);
                vec.push_back(std::move(item));
                assert(nullptr != vec.back());
            }
        }
        assert(false == vec.empty());
        return std::move(vec);
    }

    int BatchPopNoWait(size_t iMaxBatchSize,
                       std::vector<std::unique_ptr<EntryType>>& vec) {
        vec.clear();
        assert(vec.empty());

        std::lock_guard<std::mutex> lock(mutex_);
        if (msg_.empty()) {
            assert(vec.empty());
            return 1;
        }

        assert(false == msg_.empty());
        while (false == msg_.empty() && vec.size() < iMaxBatchSize) {
            assert(nullptr != msg_.front());
            auto item = std::move(msg_.front());
            msg_.pop();
            assert(nullptr != item);
            vec.push_back(std::move(item));
            assert(nullptr != vec.back());
        }

        assert(false == vec.empty());
        return 0;
    }

    int BatchPopNoWait(
        size_t iMaxBatchSize, std::vector<std::unique_ptr<EntryType>>& vec,
        std::function<bool(const std::unique_ptr<EntryType>&)> pred) {
        vec.clear();
        assert(vec.empty());

        std::lock_guard<std::mutex> lock(mutex_);
        if (msg_.empty()) {
            assert(vec.empty());
            return 1;
        }

        assert(false == msg_.empty());
        while (false == msg_.empty() && vec.size() < iMaxBatchSize) {
            assert(nullptr != msg_.front());
            if (pred(msg_.front())) {
                break;
            }

            auto item = std::move(msg_.front());
            msg_.pop();
            assert(nullptr != item);
            vec.push_back(std::move(item));
            assert(nullptr != vec.back());
        }

        assert(false == vec.empty());
        return 0;
    }

    size_t Size() {
        std::lock_guard<std::mutex> lock(mutex_);
        return msg_.size();
    }

    const std::string& GetQueueName() const { return queue_name_; }

   private:
    const std::string queue_name_;
    const size_t max_queue_size_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::unique_ptr<EntryType>> msg_;
};

// simple circle queue:
// => free-lock, but constrained on: single thread write ahead && other single
// thread read tail;

// => QueueItem => must support default construct
template <typename QueueItem>
class clsSimpleCQueue {
   public:
    clsSimpleCQueue(size_t iMaxQueueSize)
        : m_iMaxQueueSize(iMaxQueueSize), m_llHead(0), m_llTail(0) {
        assert(0 < m_iMaxQueueSize);

        m_vecCircleQueue.resize(m_iMaxQueueSize);
        assert(m_vecCircleQueue.size() == m_iMaxQueueSize);
    }

    // sink..
    int Enqueue(QueueItem& item) {
        assert(m_vecCircleQueue.size() == m_iMaxQueueSize);
        assert(m_llHead >= m_llTail);
        assert(m_llHead - m_llTail <= m_iMaxQueueSize);
        if (m_llHead - m_llTail >=
            static_cast<uint64_t>(m_vecCircleQueue.size())) {
            return 1;  // full
        }

        assert(0 < m_vecCircleQueue.size());
        m_vecCircleQueue[m_llHead % m_vecCircleQueue.size()] = std::move(item);
        ++m_llHead;
        return 0;
    }

    int Dequeue(QueueItem& item) {
        assert(m_vecCircleQueue.size() == m_iMaxQueueSize);
        assert(m_llHead >= m_llTail);
        assert(m_llHead - m_llTail <= m_iMaxQueueSize);
        if (m_llHead == m_llTail) {
            return 1;  // empty
        }

        // =else
        assert(m_llHead > m_llTail);
        assert(0 < m_vecCircleQueue.size());
        item = std::move(m_vecCircleQueue[m_llTail % m_vecCircleQueue.size()]);
        ++m_llTail;
        return 0;
    }

    bool IsEmpty() { return m_llHead == m_llTail; }

   private:
    const size_t m_iMaxQueueSize;

    volatile uint64_t m_llHead;
    volatile uint64_t m_llTail;
    std::vector<QueueItem> m_vecCircleQueue;
};  //

}  // namespace cutils
