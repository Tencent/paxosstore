#pragma once

#if defined(ROCKSDB_PLATFORM_TYPE)
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/db.h"
namespace dbtype = rocksdb;
#endif
