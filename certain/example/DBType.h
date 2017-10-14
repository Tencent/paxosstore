#pragma once

#if defined(LEVELDB_PLATFORM_TYPE)
#include "db/snapshot.h"
namespace dbtype = leveldb;
#elif defined(ROCKSDB_PLATFORM_TYPE)
#include "compaction_filter.h"
#include "db/dbformat.h"
#include "db/snapshot_impl.h"
namespace dbtype = rocksdb;
#endif

