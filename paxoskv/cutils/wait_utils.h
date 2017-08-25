
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
#include <condition_variable>
#include <tuple>
#include <unordered_map>
#include <stdint.h>


namespace cutils {

template <typename WaitResponse>
class Wait {

public:
    int Register(uint64_t reqid, std::condition_variable* cv)
    {
        if (nullptr == cv) {
            return -1;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (map_cv_.end() != map_cv_.find(reqid)) {
            return 1;
        }

        assert(map_cv_.end() == map_cv_.find(reqid));
        map_cv_[reqid] = cv;
        assert(nullptr != map_cv_[reqid]);
        return 0;
    }

    void Trigger(uint64_t reqid, std::unique_ptr<WaitResponse> rsp)
    {
        std::condition_variable* cv = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (map_cv_.end() == map_cv_.find(reqid)) {
                return ;
            }

            assert(map_cv_.end() != map_cv_.find(reqid)); 
            cv = map_cv_[reqid];
            assert(nullptr != cv);
            mresp_[reqid] = std::move(rsp);
            assert(mresp_.end() != mresp_.find(reqid));
            map_cv_.erase(reqid);
        }
        assert(nullptr != cv);
        cv->notify_all();
    }

    std::tuple<int, std::unique_ptr<WaitResponse>> DoWait(uint64_t reqid)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (map_cv_.end() != map_cv_.find(reqid)) {
            auto cv = map_cv_[reqid];
            assert(nullptr != cv);
            cv->wait(lock, 
                    [&]() {
                        return mresp_.end() != mresp_.find(reqid); 
                    });
            assert(mresp_.end() != mresp_.find(reqid));
        }

        assert(map_cv_.end() == map_cv_.find(reqid));
        if (mresp_.end() == mresp_.find(reqid)) {
            return std::make_tuple(-1, nullptr);
        }

        assert(mresp_.end() != mresp_.find(reqid));
        auto resp = std::move(mresp_[reqid]);
        mresp_.erase(reqid);
        return std::make_tuple(0, std::move(resp));
    }

    std::tuple<int, std::unique_ptr<WaitResponse>> Take(uint64_t reqid)
    {
        std::lock_guard<std::mutex> lock(mutex_); 
        if (mresp_.end() == mresp_.find(reqid)) {
            return std::make_tuple(-1, nullptr);
        }

        assert(mresp_.end() != mresp_.find(reqid));
        auto resp = std::move(mresp_[reqid]);
        mresp_.erase(reqid);
        return std::make_tuple(0, std::move(resp));
    }

private:
    std::mutex mutex_;
    std::unordered_map<uint64_t, std::condition_variable*> map_cv_;
    std::unordered_map<uint64_t, std::unique_ptr<WaitResponse>> mresp_; 
};


} // namespace cutils


