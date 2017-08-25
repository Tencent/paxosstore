
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>


namespace memkv {

class clsMemSizeMng {
   public:
    void AddUseSize(uint64_t iUseSize);
    bool IsMemEnough(uint64_t iAllocSize);
    void SetReserveMem(int32_t iReserveMem);
    int GetTotalMemSize();

    static clsMemSizeMng* GetDefault();

   private:
    clsMemSizeMng();
    ~clsMemSizeMng();

   private:
    uint64_t m_llUseSize;
    uint64_t m_llTotalSize;
    uint64_t m_llReserveSize;
};

} // namespace memkv
