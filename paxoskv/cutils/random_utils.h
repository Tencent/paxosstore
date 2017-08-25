
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <random>

namespace cutils {

template <typename RNGType,
         typename INTType,
         INTType iMin=0, INTType iMax=std::numeric_limits<INTType>::max()>
class RandomIntGen
{
public:
    RandomIntGen()
        : m_tMyRNG(std::random_device()())
        , m_tUDist(iMin, iMax)
    {

    }

    INTType Next()
    {
        return m_tUDist(m_tMyRNG);
    }

private:
    RNGType m_tMyRNG;
    std::uniform_int_distribution<INTType> m_tUDist;
};

typedef RandomIntGen<std::mt19937_64, uint64_t> Random64BitGen;
typedef RandomIntGen<std::mt19937, uint32_t> Random32BitGen;

static const char DICTIONARY[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";


template <int iMin, int iMax>
class RandomStrGen
{
public:
    std::string Next()
    {
        auto iLen = m_tRLen.Next();
        std::string s;
        s.resize(iLen);
        for (auto i = 0; i < iLen; ++i)
        {
            auto j = m_tRIdx.Next();
            s[i] = DICTIONARY[j];
            assert(s[i] != '\0');
        }
        return s;
    }

private:
    RandomIntGen<std::mt19937, int, iMin, iMax> m_tRLen;
    RandomIntGen<std::mt19937, int, 0, sizeof(DICTIONARY)-2> m_tRIdx;
};

inline int random_int(int min, int max)
{
    // mark as thread local ?
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min, max);
    return dis(gen);
}

class RandomTimeout {

public:
    RandomTimeout(int min, int max)
        : gen_(std::random_device{}())
        , dis_(min, max)
    {

    }

    int operator()()
    {
        return dis_(gen_);
    }

private:
    std::mt19937 gen_;
    std::uniform_int_distribution<> dis_;
};


} // namespace cutils


