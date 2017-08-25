
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cassert>
#include "gtest/gtest.h"
#include "string_utils.h"




TEST(StringTest, SplitTest)
{
    // case 0
    {
        auto tokens = cutils::split("", '/');
        assert(true == tokens.empty());
    }
    // case 1
    {
        auto tokens = cutils::split("/", '/');
        assert(true == tokens.empty());
    }

    // case 2
    {
        auto tokens = cutils::split("/test", '/');
        assert(size_t{1} == tokens.size());
        auto iter = tokens.begin();
        assert(*iter == "test");
    }

    // case 3
    {
        auto tokens = cutils::split("/test/", '/');
        assert(size_t{1} == tokens.size());
        auto iter = tokens.begin();
        assert(*iter == "test");
    }
}

