#include <algorithm>
#include <cassert>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "utils/time.h"

#ifdef LEVELDB
#include "co_aio.h"
#include "co_routine.h"
#include "co_routine_inner.h"
#include "iRoutineSpecific.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "util/aio.h"
namespace dbtype = leveldb;
#endif
#ifdef ROCKSDB
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
namespace dbtype = rocksdb;
#endif

DEFINE_string(path, "test_db.o", "Path lf DB Files");
DEFINE_int64(record_num, 1000000, "Number of Record");
DEFINE_int32(print_per, 1000000, "Print per X Records");
DEFINE_int32(record_size, 100, "Size of Per Record");
DEFINE_bool(write, true, "Do Write Operations");
DEFINE_bool(read, true, "Do Read Operations");
DEFINE_int32(aio, 0, "Do AIO-Read Operations");
DEFINE_int32(seed, 233, "Seed of Random Generator");

DEFINE_int32(worker_num, 1, "Worker Num");
DEFINE_int32(routine_num, 1, "Routine Num");

#ifdef LEVELDB
struct RoutineArg {
  int32_t id;
  dbtype::DB* db;
  bool finished;
};

void* RoutineWork(void* arg_ptr) {
  co_enable_hook_sys();
  auto& arg = *reinterpret_cast<RoutineArg*>(arg_ptr);

  std::mt19937 random(FLAGS_seed);
  for (int i = 0; i < arg.id; ++i) {
    (void)random();
  }

  int batch = FLAGS_worker_num * FLAGS_routine_num;
  std::string value;
  dbtype::ReadOptions options;

  for (int64_t i = 0; i < FLAGS_record_num; i += batch) {
    uint32_t k = random();
    for (int j = 1; j < batch; ++j) {
      (void)random();
    }

    dbtype::Slice key((char*)&k, sizeof(k));
    dbtype::Status status = arg.db->Get(options, key, &value);
  }

  arg.finished = true;
  return nullptr;
}

int CheckQuit(void* arg_ptr) {
  const auto& args = *reinterpret_cast<std::vector<RoutineArg>*>(arg_ptr);
  if (std::all_of(args.begin(), args.end(),
                  [](const RoutineArg& a) { return a.finished; })) {
    return -1;
  }
  return 0;
}
#endif

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_aio) {
#ifdef LEVELDB
    co_set_sys_specific_func(RoutineSysGetSpecific, RoutineSysSetSpecific);
    RoutineSetSpecificCallback(co_getspecific, co_setspecific);

    if (FLAGS_aio == 2) {
      co_init_pread2();
    }
    leveldb::set_use_direct_io(FLAGS_aio);
#endif
  }

  // 1. open db
  dbtype::DB* db = nullptr;
  {
    if (FLAGS_write) {
      std::string command = "rm -rf " + FLAGS_path;
      system(command.c_str());  // delete exists plog db
    }
    static dbtype::Options options;
    options.create_if_missing = true;
    options.compression = dbtype::kNoCompression;
#ifdef LEVELDB
    options.filter_policy = dbtype::NewBloomFilterPolicy(10);
#endif
    printf("Open DB[%s]\n", FLAGS_path.c_str());
    dbtype::Status status = dbtype::DB::Open(options, FLAGS_path, &db);
    assert(status.ok());
  }
  std::string data(FLAGS_record_size, '\0');
  for (auto& ch : data) {
    ch = rand() % 26 + 'A';
  }

  // 2. do write operations
  if (FLAGS_write) {
    std::mt19937 random(FLAGS_seed);
    static dbtype::WriteOptions options;
    uint64_t start = certain::GetTimeByMsec();
    for (int64_t i = 0; i < FLAGS_record_num; ++i) {
      uint32_t k = random();
      dbtype::Slice key((char*)&k, sizeof(k));
      dbtype::Status status = db->Put(options, key, data);
      if ((i + 1) % FLAGS_print_per == 0) {
        uint64_t elapsed = certain::GetTimeByMsec() - start;
        printf("Write [%ld] records takes [%lu]ms\n", i + 1, elapsed);
      }
    }
  }

  // 3. do read operations
  if (FLAGS_read) {
    std::mt19937 random(FLAGS_seed);
    std::string value;
    static dbtype::ReadOptions options;
    uint64_t start = certain::GetTimeByMsec();
    for (int64_t i = 0; i < FLAGS_record_num; ++i) {
      uint32_t k = random();
      dbtype::Slice key((char*)&k, sizeof(k));
      dbtype::Status status = db->Get(options, key, &value);
      if ((i + 1) % FLAGS_print_per == 0) {
        uint64_t elapsed = certain::GetTimeByMsec() - start;
        printf("Read [%ld] records takes [%lu]ms\n", i + 1, elapsed);
      }
    }
  }

#ifdef LEVELDB
  // 4. do aio-read operations
  if (FLAGS_aio) {
    uint64_t start = certain::GetTimeByMsec();
    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_worker_num; ++i) {
      threads.emplace_back([&] {
        std::vector<RoutineArg> args(FLAGS_routine_num);
        for (int j = 0; j < FLAGS_routine_num; ++j) {
          args[j] = {i * FLAGS_routine_num + j, db, false};
          stCoRoutine_t* co = nullptr;
          co_create(&co, nullptr, RoutineWork, &args[j]);
          co_resume(co);
        }
        co_eventloop(co_get_epoll_ct(), CheckQuit, (void*)&args);
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    uint64_t elapsed = certain::GetTimeByMsec() - start;
    printf("Read [%ld] records takes [%lu]ms\n", FLAGS_record_num, elapsed);
  }
#endif

  delete db;
}
