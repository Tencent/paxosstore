
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UTILS_HASH_
#define CERTAIN_UTILS_HASH_

namespace leveldb
{
extern uint32_t Hash(const char* data, size_t n, uint32_t seed);
}

namespace Certain
{

inline uint32_t Hash(const char* pcData, uint32_t iLen)
{
	return leveldb::Hash(pcData, iLen, 20151208);
}

inline uint32_t Hash(const string &strData)
{
	return Hash(strData.c_str(), strData.size());
}

inline uint32_t Hash(uint64_t iData)
{
	return Hash((const char *)&iData, sizeof(iData));
}

inline uint32_t dictIntHashFunctionKvsvr(uint32_t key)
{
	key += ~(key << 15); 
	key ^=  (key >> 10); 
	key +=  (key << 3);
	key ^=  (key >> 6);
	key += ~(key << 11); 
	key ^=  (key >> 16); 
	return key;
}

} // namespace Certain

#endif // CERTAIN_UTIL_HASH_
