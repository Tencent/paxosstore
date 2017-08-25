
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "mmap_file.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdlib>

namespace dbcomm {

clsMMapFile::clsMMapFile() : m_iFileSize(0), m_pMMapFile(NULL) {}

clsMMapFile::~clsMMapFile() { UnMMap(); }

int clsMMapFile::MMap(int fd, int flags, int fildes) {
    // unmmap if any
    UnMMap();

    //
    if (0 > fd) {
        return -1;
    }

    off_t iFileSize = lseek(fd, 0, SEEK_END);
    if (0 > iFileSize) {
        return -2;
    }

    char* pMMapFile = reinterpret_cast<char*>(
        mmap(NULL, iFileSize, PROT_READ, MAP_PRIVATE, fd, 0));
    if (NULL == pMMapFile) {
        return -3;
    }

    m_pMMapFile = pMMapFile;
    m_iFileSize = static_cast<uint32_t>(iFileSize);
    return 0;
}

int clsMMapFile::UnMMap() {
    int ret = 0;
    if (NULL != m_pMMapFile) {
        ret = munmap(m_pMMapFile, m_iFileSize);
        m_pMMapFile = NULL;
        m_iFileSize = 0;
    }
    return ret;
}

char* clsMMapFile::Begin() const { return m_pMMapFile; }

char* clsMMapFile::End() const {
    if (NULL != m_pMMapFile) {
        return m_pMMapFile + m_iFileSize;
    }
    return NULL;
}

int clsReadOnlyMMapFile::OpenFile(const std::string& sFileName) {
    int fd = open(sFileName.c_str(), O_RDONLY);
    if (0 > fd) {
        return -1;
    }

    int ret = m_oMMapFile.MMap(fd, PROT_READ, MAP_PRIVATE);
    close(fd);
    return ret;
}

int clsReadOnlyMMapFile::CloseFile() { return m_oMMapFile.UnMMap(); }

const char* clsReadOnlyMMapFile::Begin() const { return m_oMMapFile.Begin(); }

const char* clsReadOnlyMMapFile::End() const { return m_oMMapFile.End(); }

}  // namespace dbcomm
