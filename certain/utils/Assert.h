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
