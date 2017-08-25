
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <vector>
#include <future>
#include "gtest/gtest.h"
#include "wait_utils.h"
#include "mem_utils.h"


using namespace std;


TEST(WaitTest, SimpleTest)
{
    cutils::Wait<int> w;
    uint64_t reqid = 1ull;
    condition_variable cv;
    auto err = w.Register(reqid, &cv);
    assert(0 == err);

    w.Trigger(reqid, cutils::make_unique<int>(-2));
    err = 0;
    unique_ptr<int> resp = nullptr;
    tie(err, resp) = w.Take(reqid);
    assert(0 == err);
    assert(nullptr != resp);
    assert(-2 == *resp);
}


TEST(WaitTest, ErrorCaseTest)
{
    // case 1
    {
        cutils::Wait<int> w;
        uint64_t reqid = 2ull;
        condition_variable cv;
        auto err = w.Register(reqid, &cv);
        assert(0 == err);

        err = w.Register(reqid, &cv);
        assert(1 == err);

        err = w.Register(reqid, nullptr);
        assert(-1 == err);
    }

    // case 2
    {
        cutils::Wait<int> w;
        uint64_t reqid = 3ull;
        int err = 0;
        std::unique_ptr<int> resp = nullptr;

        w.Trigger(reqid, cutils::make_unique<int>(1));
        tie(err, resp) = w.Take(reqid);
        assert(-1 == err);
        assert(nullptr == resp);
    }

    // case 3
    {
        cutils::Wait<int> w;
        uint64_t reqid = 4ull;
        condition_variable cv;
        auto err = w.Register(reqid, &cv);
        assert(0 == err);

        w.Trigger(reqid, cutils::make_unique<int>(2));
        w.Trigger(reqid, cutils::make_unique<int>(12));

        err = 0;
        std::unique_ptr<int> resp = nullptr;
        tie(err, resp) = w.Take(reqid);
        assert(0 == err);
        assert(nullptr != resp);
        assert(2 == *resp);

        err = 0;
        resp = nullptr;
        tie(err, resp) = w.Take(reqid);
        assert(-1 == err);
        assert(nullptr == resp);
    }
}

namespace {

int DoWaitTest(int id, int worker_cnt, cutils::Wait<int>& w)
{
    assert(0 <= id);
    assert(0 < worker_cnt);
    for (int reqid = id; reqid < 200; reqid += worker_cnt) {
        condition_variable cv;
        auto err = w.Register(reqid, &cv);
        assert(0 == err);

        w.Trigger(reqid, cutils::make_unique<int>(reqid));

        err = 0;
        std::unique_ptr<int> resp = nullptr;
        tie(err, resp) = w.DoWait(reqid);
        assert(0 == err);
        assert(nullptr != resp);
        assert(*resp == reqid);
    }

    return 0;
}

} // namespace 

TEST(WaitTest, ConcurrentTest)
{
    cutils::Wait<int> w;
    const int iWorkCnt = 5;    
    vector<future<int>> vec_fut;
    for (int i = 0; i < iWorkCnt; ++i) {
        vec_fut.emplace_back(
                std::async(std::launch::async, DoWaitTest, i, iWorkCnt, ref(w)));
    }

    for (auto& f : vec_fut) {
        assert(0 == f.get());
    }
}

