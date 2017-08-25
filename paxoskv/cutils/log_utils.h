
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>
#include <cstdio>

namespace {

void log_nothing(const char* /* format */, ...) 
    __attribute__((format(printf, 1, 2)));

void log_nothing(const char* /* format */, ...) {

}

} // namespace

#ifndef TEST_DEBUG

#define logdebug(format, ...) log_nothing(format, ##__VA_ARGS__)
#define logerr(format, ...) log_nothing(format, ##__VA_ARGS__)
#define logimpt(format, ...) log_nothing(format, ##__VA_ARGS__)

#else

#define logdebug(format, ...) \
    printf("[CONSENSUS DEBUG: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#define logerr(format, ...) \
    printf("[CONSENSUS ERROR: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#define logimpt(format, ...) \
    printf("[CONSENSUS IMPT: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#endif



