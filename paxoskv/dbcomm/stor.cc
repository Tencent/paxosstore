
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cassert>
#include "cutils/log_utils.h"
#include "stor.h"


stor_cursor_t *stor_cursor_open(const char *filename) {
    stor_cursor_t *cursor = NULL;
    struct stat fs;
    void *mmap_addr = NULL;

    cursor = new stor_cursor_t;

    if (!cursor) {
        logerr("new stor_cursor_t == NULL");
        return NULL;
    }

    cursor->fd = -1;
    cursor->pos = NULL;

    // open the cursor file
    cursor->fd = open(filename, O_CREAT | O_RDWR, 0644);
    if (cursor->fd < 0) {
        logerr("open(%s) :%s", filename, strerror(errno));
        if (cursor) stor_cursor_close(cursor);
        return NULL;
    }

    pthread_mutex_init(&cursor->mutex, NULL);

    // Check filesize, if it's blank, write init value (1, 0)
    if (stor_cursor_lock(cursor, LOCK_EX) != 0 || fstat(cursor->fd, &fs) != 0) {
        if (cursor) stor_cursor_close(cursor);
        return NULL;
    }
    if (fs.st_size != sizeof(Pos_t)) {
        // we need to init pos here
        Pos_t tmp_pos;
        if (write(cursor->fd, &tmp_pos, sizeof(tmp_pos)) != sizeof(tmp_pos)) {
            if (cursor) stor_cursor_close(cursor);
            return NULL;
        }
    }
    stor_cursor_unlock(cursor);

    // mmap cursor
    mmap_addr = mmap(
            NULL, sizeof(Pos_t), PROT_READ | PROT_WRITE, MAP_SHARED, 
            cursor->fd, 0);
    if (!mmap_addr || mmap_addr == MAP_FAILED) {
        logerr("mmap(%s) :%s", filename, strerror(errno));
        if (cursor) stor_cursor_close(cursor);
        return NULL;
    }
    cursor->pos = (Pos_t *)mmap_addr;
    return cursor;
}

stor_cursor_t *stor_cursor_open_r(const char *filename) {
    stor_cursor_t *cursor = NULL;
    struct stat fs;
    void *mmap_addr = NULL;

    cursor = new stor_cursor_t;

    if (!cursor) {
        logerr("new stor_cursor_t == NULL");
        return NULL;
    }

    cursor->fd = -1;
    cursor->pos = NULL;

    // open the cursor file
    cursor->fd = open(filename, O_RDONLY, 0644);
    if (cursor->fd < 0) {
        logerr("open(%s) :%s", filename, strerror(errno));
        if (cursor) stor_cursor_close(cursor);
        return NULL;
    }

    pthread_mutex_init(&cursor->mutex, NULL);

    // Check filesize, if it's blank, write init value (1, 0)
    if (stor_cursor_lock(cursor, LOCK_EX) != 0 || fstat(cursor->fd, &fs) != 0) {
        if (cursor) stor_cursor_close(cursor);
        return NULL;
    }
    if (fs.st_size != sizeof(Pos_t)) {
        return NULL;
    }
    stor_cursor_unlock(cursor);

    // mmap cursor
    mmap_addr = mmap(NULL, sizeof(Pos_t), PROT_READ, MAP_SHARED, cursor->fd, 0);
    if (!mmap_addr || mmap_addr == MAP_FAILED) {
        logerr("mmap(%s) :%s", filename, strerror(errno));
        if (cursor) stor_cursor_close(cursor);
        return NULL;
    }
    cursor->pos = (Pos_t *)mmap_addr;
    return cursor;
}

int stor_cursor_sync(stor_cursor_t *cursor) {
    int ret = 0;
    ret = msync(cursor->pos, sizeof(Pos_t), MS_SYNC);
    if (ret < 0) {
        logerr("msync %s", strerror(errno));
        return -1;
    }

    return 0;
}
int stor_cursor_close(stor_cursor_t *cursor) {
    if (cursor->fd >= 0) {
        close(cursor->fd);
    }
    if (cursor->pos) {
        munmap(cursor->pos, sizeof(Pos_t));
    }

    pthread_mutex_destroy(&cursor->mutex);
    delete cursor;
    // free( cursor );
    return 0;
}

int stor_cursor_lock(stor_cursor_t *cursor, int type) {
    pthread_mutex_lock(&cursor->mutex);
    return 0;
}

int stor_cursor_unlock(stor_cursor_t *cursor) {
    pthread_mutex_unlock(&cursor->mutex);
    return 0;
}

namespace {

int ResetStorCursorPtr(stor_cursor_t *&ptr) {
    if (NULL == ptr) {
        return 0;
    }

    // stor_cursor_close will delete ptr!!!!
    int ret = stor_cursor_close(ptr);
    ptr = NULL;
    return ret;
}

}  // namespace

namespace dbcomm {

StorCursor::StorCursor() : m_pos(NULL) {}

StorCursor::~StorCursor() { ResetStorCursorPtr(m_pos); }

int StorCursor::OpenForRead(const std::string &sFileName) {
    stor_cursor_t *pos = stor_cursor_open_r(sFileName.c_str());
    if (NULL == pos) {
        return -1;
    }

    ResetStorCursorPtr(m_pos);
    assert(NULL == m_pos);
    m_pos = pos;
    assert(NULL != m_pos);
    return 0;
}

int StorCursor::OpenForWrite(const std::string &sFileName) {
    stor_cursor_t *pos = stor_cursor_open(sFileName.c_str());
    if (NULL == pos) {
        return -1;
    }

    ResetStorCursorPtr(m_pos);
    assert(NULL == m_pos);
    m_pos = pos;
    assert(NULL != m_pos);
    return 0;
}

void StorCursor::Get(int &iFileNo, int &iOffset) {
    assert(NULL != m_pos);
    assert(NULL != m_pos->pos);
    iFileNo = m_pos->pos->iFileNo;
    iOffset = m_pos->pos->iOffset;
}

void StorCursor::Update(int iFileNo, int iOffset) {
    assert(NULL != m_pos);
    assert(NULL != m_pos->pos);
    m_pos->pos->iFileNo = iFileNo;
    m_pos->pos->iOffset = iOffset;
}

int StorCursor::Lock() {
    assert(NULL != m_pos);
    return stor_cursor_lock(m_pos, 0);  // 0: type: unsed
}

int StorCursor::UnLock() {
    assert(NULL != m_pos);
    return stor_cursor_unlock(m_pos);
}

int StorCursor::Sync() {
    assert(NULL != m_pos);
    return stor_cursor_sync(m_pos);
}

int StorCursor::Close() { return ResetStorCursorPtr(m_pos); }

}  // namespace dbcomm
