
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once


#include <stdint.h>


namespace cutils {


inline uint32_t dict_int_hash_func(uint64_t key) {

	key += ~(key << 15);
	key ^=  (key >> 10);
	key +=  (key << 3);
	key ^=  (key >> 6);
	key += ~(key << 11);
	key ^=  (key >> 16);
	return key;
}

inline uint32_t bkdr_hash(const char* str) {
    uint32_t seed = 131;
	uint32_t hash = 0;

	if (!str) return 0;
	while (*str)
	{
		hash = hash * seed + (*str++);
	}

	return (hash & 0x7FFFFFFF);
}

inline uint64_t bkdr_64hash(const char* str) {
    uint64_t seed = 1313;  // 31 131 1313 13131 131313 etc..
    uint64_t hash = 0;

    while (*str)
    {
        hash = hash * seed + (*str++);
    }

    return hash;
}

inline uint32_t cal_route_uin(const char* str) {
    uint32_t uin = bkdr_hash(str);
    return uin < 10000 ? uin + 10000 : uin;
}


} // namespace cutils


