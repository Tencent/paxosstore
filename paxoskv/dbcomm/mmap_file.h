
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <stdint.h>
#include <string>

namespace dbcomm {

class clsMMapFile {
   public:
    clsMMapFile();
    ~clsMMapFile();
    int MMap(int fd, int flags, int fildes);
    int UnMMap();

    char* Begin() const;
    char* End() const;

   private:
    uint32_t m_iFileSize;
    char* m_pMMapFile;
};

class clsReadOnlyMMapFile {
   public:
    int OpenFile(const std::string& sFileName);

    int CloseFile();

    typedef const char* iterator;
    const char* Begin() const;
    const char* End() const;

   private:
    clsMMapFile m_oMMapFile;
};
}
