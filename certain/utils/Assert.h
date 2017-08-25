
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef CERTAIN_UTILS_ASSERT_H_
#define CERTAIN_UTILS_ASSERT_H_

#include "utils/Logger.h"

namespace Certain
{

// Make false assert after fatal log.

#define Assert(x) \
	do { \
		long long _x = (long long)(x); \
		if (!(_x)) { \
			CertainLogFatal("Assert False %s", #x); \
		} \
	} while (0);

#define AssertSyscall(call) \
	do { \
		int iRet = (call); \
		if (iRet != 0) { \
			CertainLogFatal("%s failed errno %d", #call, errno); \
		} \
	} while (0);

#define AssertEqual(tx, ty) \
	do { \
		long long _x = (long long)(tx); \
		long long _y = (long long)(ty); \
		if (!(_x == _y)) { \
			CertainLogFatal("AssertEqual (%s)(%lld) == (%s)(%lld)", #tx, _x, #ty, _y); \
		} \
	} while (0);

#define AssertNotEqual(tx, ty) \
	do { \
		long long _x = (long long)(tx); \
		long long _y = (long long)(ty); \
		if (!(_x != _y)) { \
			CertainLogFatal("AssertNotEqual (%s)(%lld) != (%s)(%lld)", #tx, _x, #ty, _y); \
		} \
	} while (0);

#define AssertLess(tx, ty) \
	do { \
		long long _x = (long long)(tx); \
		long long _y = (long long)(ty); \
		if (!(_x < _y)) { \
			CertainLogFatal("AssertLess (%s)(%lld) < (%s)(%lld)", #tx, _x, #ty, _y); \
		} \
	} while (0);

#define AssertNotMore(tx, ty) \
	do { \
		long long _x = (long long)(tx); \
		long long _y = (long long)(ty); \
		if (!(_x <= _y)) { \
			CertainLogFatal("AssertNotMore (%s)(%lld) <= (%s)(%lld)", #tx, _x, #ty, _y); \
		} \
	} while (0);

} // namespace Certain

#endif // CERTAIN_ASSERT_H_
