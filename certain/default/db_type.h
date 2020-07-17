#pragma once
#ifdef LEVELDB_COMM
#include "co_aio.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
#include "util/aio.h"
namespace dbtype = leveldb;
#else
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
namespace dbtype = rocksdb;
#endif
