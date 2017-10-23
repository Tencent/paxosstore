#pragma once

#include "db.h"
#include "db/db_impl.h"
#include "db/write_batch_internal.h"
#include "slice.h"
#include "write_batch.h"

#if defined(LEVELDB_PLATFORM_TYPE)
#include "db/snapshot.h"
namespace dbtype = leveldb;
#elif defined(ROCKSDB_PLATFORM_TYPE)
#include "compaction_filter.h"
#include "comparator.h"
#include "db/dbformat.h"
#include "db/snapshot_impl.h"
namespace dbtype = rocksdb;
#endif

