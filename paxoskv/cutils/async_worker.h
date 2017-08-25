
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <future>
#include <atomic>
#include <cassert>

namespace cutils {

class AsyncWorker {

public:

    template <typename WorkerType, typename ...Args>
    AsyncWorker(WorkerType func, Args... args)
        : stop_(false)
        , worker_(std::async(std::launch::async, 
					func, std::forward<Args>(args)..., std::ref(stop_)))
    {
		assert(worker_.valid());
    }

    ~AsyncWorker() {
        stop_ = true;
		if (worker_.valid())
		{
        	worker_.get();
		}
    }

    // delete: copy construct, move construct, assginment..

private:
    bool stop_;
    std::future<void> worker_;
};


} // namespace cutils


