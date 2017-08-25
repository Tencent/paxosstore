
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <unistd.h>
#include <stdint.h>
#include <sys/uio.h>
#include <pthread.h>
#include <string>


typedef struct s_Pos
{
	uint32_t iFileNo;
	uint32_t iOffset;

	s_Pos()
	{
		iFileNo = 0;
		iOffset = 0;
	};

	s_Pos(const s_Pos & right)
	{
		iFileNo = right.iFileNo;
		iOffset = right.iOffset;
	}

	s_Pos & operator=(const s_Pos & right)
	{
		iFileNo = right.iFileNo;
		iOffset = right.iOffset;
		return *this;
	}

	s_Pos & operator=(const s_Pos * right)
	{
		iFileNo = right->iFileNo;
		iOffset = right->iOffset;
		return *this;
	}
}Pos_t;

typedef struct stor_cursor_s 
{
	int fd;
	Pos_t *pos;
	pthread_mutex_t mutex;
} stor_cursor_t;



stor_cursor_t *stor_cursor_open( const char *filename );
stor_cursor_t  *stor_cursor_open_r( const char *filename );
int stor_cursor_close( stor_cursor_t * cursor );
int stor_cursor_sync( stor_cursor_t * cursor );
int stor_cursor_lock( stor_cursor_t * cursor, int type );
int stor_cursor_unlock( stor_cursor_t * cursor );

namespace dbcomm {

class StorCursor
{
public:
	StorCursor();
	~StorCursor();

	int OpenForRead(const std::string& sFileName);

	int OpenForWrite(const std::string& sFileName);

	void Get(int& iFileNo, int& iOffset);
	void Update(int iFileNo, int iOffset);

	int Lock();

	int UnLock();

	int Sync();

	int Close();

private:
	stor_cursor_t* m_pos;
};


} // namespace dbcomm

