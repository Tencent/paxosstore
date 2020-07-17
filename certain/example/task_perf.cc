#include "certain/certain.h"
#include "co_routine.h"
#include "default/db_impl.h"
#include "default/log_impl.h"
#include "default/monitor_impl.h"
#include "default/plog_impl.h"
#include "default/route_impl.h"
#include "gflags/gflags.h"
#include "utils/memory.h"
#include "utils/time.h"
#ifdef LEVELDB_COMM
#include "util/aio.h"
#else
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#endif
#ifdef CERTAIN_PROFILE
#include "gperftools/profiler.h"
#endif

#include "example/task.h"

DEFINE_string(task, "write", "Select write, read or conflict.");
DEFINE_string(ip0, "127.0.0.1", "Node0's ip");
DEFINE_string(ip1, "127.0.0.1", "Node1's ip");
DEFINE_string(ip2, "127.0.0.1", "Node2's ip");

DEFINE_int32(port0, 10066, "Node0's port");
DEFINE_int32(port1, 10067, "Node1's port");
DEFINE_int32(port2, 10068, "Node2's port");

DEFINE_int32(index, 0, "Use ip0:port0 as local address for node index 0.");
DEFINE_int32(task_num, 10, "Task worker num.");
DEFINE_int32(routine_num, 200, "Routine num for each task worker.");
DEFINE_int64(count, 10000, "Request num for each routine in task worker.");
DEFINE_bool(run, false, "Run task.");
DEFINE_bool(delete_plog, true, "Delete exists plog");
DEFINE_int32(plog_num, 16, "Open plog_num DB instance for plog.");
DEFINE_int32(log_level, 2,
             "kZero=0,kFatal=1,kError=2,kWarn=3,kInfo=4,kDebug=5");

#ifdef LEVELDB_COMM
DEFINE_int32(db_block_size, 2, "DB Block Size in KB");
DEFINE_uint64(db_mem_size, 128, "DB MemTable Size in MB");
DEFINE_int32(bloom_size_ratio, 2, "DB Memtable Bloom Size Ratio");
DEFINE_uint64(table_cache, 256, "Table Cache Size in MB");
DEFINE_uint64(block_cache, 128, "Block Cache Size in MB");
DEFINE_uint64(max_cache_block, 16, "Max Cache Block Size in KB");
DEFINE_int32(compression_level, 3, "Compression Level");
DEFINE_bool(full_filter, true, "Full Filter");
DEFINE_int32(no_filter_level, 2, "No Filter Level");
DEFINE_int32(num_levels, 4, "Num of Levels");
DEFINE_uint64(target_file, 64, "Target File Size in MB");
DEFINE_uint64(base_bytes, 512, "Max Bytes for Level Base in MB");
DEFINE_int32(compactions, 2, "Num of Background Compactions Thread");
#else
class PlogFilter : public dbtype::CompactionFilter {
 public:
  PlogFilter(DbImpl* db_impl) : db_impl_(db_impl) {}
  virtual ~PlogFilter() {}

  virtual bool Filter(int level, const dbtype::Slice& key,
                      const dbtype::Slice& existing_value,
                      std::string* new_value, bool* value_changed) const {
    if (level < 1) {
      // Better to use expiration time to judge.
      return false;
    }
    thread_local uint64_t prev_entity_id = 0;
    thread_local uint64_t prev_max_committed_entry = 0;

    uint64_t entity_id = 0;
    uint64_t entry = 0;
    PlogImpl::ParseKey(key, &entity_id, &entry);

    if (entity_id != prev_entity_id) {
      certain::Db::RecoverFlag flag;
      int ret =
          db_impl_->GetStatus(entity_id, &prev_max_committed_entry, &flag);
      if (ret != 0 || flag != certain::Db::kNormal) {
        return false;
      }
      prev_entity_id = entity_id;
      CERTAIN_LOG_ZERO("level %d prev committed E(%lu, %lu) entry %lu", level,
                       prev_entity_id, prev_max_committed_entry, entry);
    }
    return entry <= prev_max_committed_entry;
  }

  virtual const char* Name() const { return "plog_filter"; }

 private:
  DbImpl* db_impl_;
};
#endif

void Profile(int signum) {
#ifdef CERTAIN_PROFILE
  static bool g_perf_start = false;
  if (!g_perf_start) {
    printf("ProfilerStart\n");
    ProfilerStart("task_perf.prof");
    g_perf_start = true;

    struct itimerval t;
    t.it_interval.tv_sec = 0;
    t.it_interval.tv_usec = 1000;
    t.it_value = t.it_interval;
    setitimer(ITIMER_PROF, &t, NULL);
  } else {
    printf("ProfilerStop\n");
    ProfilerStop();
    g_perf_start = false;
  }
#endif
}

void OpenDbs(const std::vector<std::string>& db_paths, DbImpl* db_impl,
             std::vector<dbtype::DB*>* dbs) {
  for (auto& db_path : db_paths) {
    if (FLAGS_delete_plog) {
      std::string command = "rm -rf " + db_path;
      system(command.c_str());  // delete exists plog db
      printf("Delete existed plog and open plog_db[%s]\n", db_path.c_str());
    } else {
      printf("Open plog_db[%s]\n", db_path.c_str());
    }

    dbtype::DB* db = nullptr;
    dbtype::Options options;
    options.create_if_missing = true;

#ifdef LEVELDB_COMM
    dbtype::set_use_direct_io(1);
    options.env = dbtype::Env::NewPosixEnv();
    options.compression = dbtype::kNoCompression;
    options.filter_policy = dbtype::NewBloomFilterPolicy(10);

    options.arena_block_size = 1 << 20;
    options.level0_file_num_compaction_trigger = 2;
    options.write_worker_num = 5;

    options.block_size = FLAGS_db_block_size << 10;
    options.write_buffer_size = FLAGS_db_mem_size << 20;
    options.memtable_bloom_size_ratio = FLAGS_bloom_size_ratio;
    options.max_table_cache_size = FLAGS_table_cache << 20;
    options.block_cache = leveldb::NewLRUCache(FLAGS_block_cache << 20);
    options.max_cache_block_size = FLAGS_max_cache_block << 10;
    options.compression_level = FLAGS_compression_level;
    options.full_filter = FLAGS_full_filter;
    options.no_filter_level = FLAGS_no_filter_level;
    options.num_levels = FLAGS_num_levels;
    options.target_file_size = FLAGS_target_file << 20;
    options.max_bytes_for_level_base = FLAGS_base_bytes << 20;
    options.max_background_compactions = FLAGS_compactions;
#else
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    static PlogFilter plog_filter(db_impl);
    options.compaction_filter = &plog_filter;
#endif

    dbtype::Status status = dbtype::DB::Open(options, db_path, &db);
    assert(status.ok());
    dbs->push_back(db);
  }
}

int main(int argc, char* argv[]) {
  assert(sigset(SIGUSR1, Profile) != SIG_ERR);

  // 1. Parse arguments.
  google::ParseCommandLineFlags(&argc, &argv, true);

  google::CommandLineFlagInfo info;
  if ((GetCommandLineFlagInfo("index", &info) && info.is_default) ||
      !(0 <= FLAGS_index && FLAGS_index <= 2)) {
    printf("Local node index must be set, range 0 to 2.\n");
    return -1;
  }

  std::vector<std::string> addr_list;
  addr_list.push_back(FLAGS_ip0 + ":" + std::to_string(FLAGS_port0));
  addr_list.push_back(FLAGS_ip1 + ":" + std::to_string(FLAGS_port1));
  addr_list.push_back(FLAGS_ip2 + ":" + std::to_string(FLAGS_port2));
  std::string local_addr = addr_list[FLAGS_index];

  printf(
      "node0: %s node1: %s node2: %s local_addr: %s "
      "task_num %d routine_num %d count %ld run %u\n",
      addr_list[0].c_str(), addr_list[1].c_str(), addr_list[2].c_str(),
      local_addr.c_str(), FLAGS_task_num, FLAGS_routine_num, FLAGS_count,
      FLAGS_run);

  // 2. Initialize and start certain.
  certain::Options options;
  RouteImpl route_impl(local_addr, addr_list);
  LogImpl log_impl("mm", "./test_log.o",
                   static_cast<certain::LogLevel>(FLAGS_log_level));
  assert(log_impl.Init() == 0);

  std::vector<std::string> db_paths = {
      "/data1/qspace/temp/plog0", "/data2/qspace/temp/plog0",
      "/data3/qspace/temp/plog0", "/data4/qspace/temp/plog0",
      "/data1/qspace/temp/plog1", "/data2/qspace/temp/plog1",
      "/data3/qspace/temp/plog1", "/data4/qspace/temp/plog1",
      "/data1/qspace/temp/plog2", "/data2/qspace/temp/plog2",
      "/data3/qspace/temp/plog2", "/data4/qspace/temp/plog2",
      "/data1/qspace/temp/plog3", "/data2/qspace/temp/plog3",
      "/data3/qspace/temp/plog3", "/data4/qspace/temp/plog3",
  };
  assert(FLAGS_plog_num <= int(db_paths.size()));
  db_paths.resize(FLAGS_plog_num);
  std::vector<dbtype::DB*> dbs;
  DbImpl db_impl;
  OpenDbs(db_paths, &db_impl, &dbs);
  PlogImpl plog_impl(dbs);

  MonitorImpl monitor_impl(221345, 221369);
  options.set_log(&log_impl);
  options.set_monitor(&monitor_impl);

  int ret = certain::Certain::Init(&options, &route_impl, &plog_impl, &db_impl);
  if (ret != 0) {
    printf("wrapper->Init ret %d\n", ret);
    return -2;
  }
  certain::Certain::Start();

  while (!FLAGS_run) {
    sleep(1);
  }

  // 3. Start task workers.
  certain::StopHelper stop("test_task", FLAGS_task_num);
  certain::UseTimeStat stat("test");

  uint64_t start_time_msec = certain::GetTimeByMsec();
  std::unique_ptr<Task> task[FLAGS_task_num];
  for (int i = 0; i < FLAGS_task_num; ++i) {
    task[i] =
        std::make_unique<Task>(i, FLAGS_routine_num, FLAGS_count, &stat, &stop);
    task[i]->Start();
  }

  // 4. Exit if stop.
  while (!stop.IsStopNow()) {
    sleep(1);
    stat.LogStat();
  }
  uint64_t end_time_msec = certain::GetTimeByMsec();
  printf("Cost %lu msec.\n", end_time_msec - start_time_msec);

  // 5. Cleanup before quit.
  for (int i = 0; i < FLAGS_task_num; ++i) {
    task[i]->WaitExit();
  }

  certain::Certain::Stop();
  return 0;
}
