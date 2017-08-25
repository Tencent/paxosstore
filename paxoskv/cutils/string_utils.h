
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <list>
#include <string>

namespace cutils {

inline std::list<std::string> 
split(const std::string& value, const char delm)
{
    std::list<std::string> tokens;
    size_t prev_pos = 0;
    size_t pos = 0;
    for (; pos < value.size(); ++pos) {
        if (delm == value[pos]) {
            if (pos - prev_pos > 0) {
                tokens.emplace_back(&value[prev_pos], pos - prev_pos);
            }

            prev_pos = pos + 1;
        }
    }

    if (pos - prev_pos > 0) {
        tokens.emplace_back(&value[prev_pos], pos - prev_pos);
    }

    return tokens;
}


} // namespace cutils;
