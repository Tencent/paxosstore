
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once


namespace paxos {

class PaxosLog;

} // namespace paxos


namespace memkv {

enum {
    MIN_COMPRESS_LEN = 16
};

class HeadWrapper;

inline bool enable_mem_compresse() {
    return true;
}

size_t MaxCompressedLength(const paxos::PaxosLog& oPLog);

int PLogToNewHead(const paxos::PaxosLog& oPLog, HeadWrapper& oHead);
int NewHeadToPlog(const HeadWrapper& oHead, paxos::PaxosLog& oPLog);
bool HasCompresseFlag(const HeadWrapper& oHead);
int MayUnCompresse(
        const HeadWrapper& oHead, 
        char*& pValue, uint32_t& iValLen);


} // namespace memkv;
