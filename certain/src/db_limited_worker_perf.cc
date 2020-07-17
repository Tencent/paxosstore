#include <atomic>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "co_aio.h"
#include "default/db_impl.h"
#include "default/log_impl.h"
#include "default/plog_impl.h"
#include "default/route_impl.h"
#include "gflags/gflags.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "src/command.h"
#include "src/db_limited_worker.h"
#include "src/plog_worker.h"
#include "src/wrapper.h"
#include "util/aio.h"
#include "utils/memory.h"
#include "utils/time.h"

DEFINE_uint64(entity_num, 20000000, "Entity Num");
DEFINE_uint64(record_num, 1000000, "Record Num");
DEFINE_int32(record_size, 100, "Record Size");
DEFINE_int32(worker_num, 8, "Worker Num");
DEFINE_int32(routine_num, 128, "Routine Num");
DEFINE_int32(db_num, 8, "DB Num");
DEFINE_int32(aio, 1, "AIO Mode (1 or 2)");
DEFINE_bool(write_plog, false, "Write Plog");
DEFINE_bool(write_db, false, "Write DB");
DEFINE_bool(read, true, "Read Mode");
DEFINE_bool(log_to_file, false, "Log Time to File");

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

void OpenDbs(const std::vector<std::string>& db_paths,
             std::vector<dbtype::DB*>* dbs) {
  for (auto& db_path : db_paths) {
    if (FLAGS_write_plog) {
      std::string command = "rm -rf " + db_path;
      system(command.c_str());  // delete exists plog db
    }
    printf("Open DB[%s]\n", db_path.c_str());

    dbtype::DB* db = nullptr;
    dbtype::Options options;
    options.create_if_missing = true;
#ifdef LEVELDB_COMM
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
#endif

    dbtype::Status status = dbtype::DB::Open(options, db_path, &db);
    assert(status.ok());
    dbs->push_back(db);
  }
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_record_num % FLAGS_worker_num == 0);

  LogImpl log("mm", "./test_log.o", certain::LogLevel::kInfo);
  assert(log.Init() == 0);
  certain::Log::GetInstance()->Init(&log);
#ifdef LEVELDB_COMM
  if (FLAGS_aio == 2) {
    co_init_pread2();
  }
  dbtype::set_use_direct_io(FLAGS_aio);
#endif
  // 1. init db list
  std::vector<std::string> db_paths = {
      "/data1/qspace/temp/plog_db0", "/data2/qspace/temp/plog_db0",
      "/data3/qspace/temp/plog_db0", "/data4/qspace/temp/plog_db0",
      "/data1/qspace/temp/plog_db1", "/data2/qspace/temp/plog_db1",
      "/data3/qspace/temp/plog_db1", "/data4/qspace/temp/plog_db1",
      "/data1/qspace/temp/plog_db2", "/data2/qspace/temp/plog_db2",
      "/data3/qspace/temp/plog_db2", "/data4/qspace/temp/plog_db2",
      "/data1/qspace/temp/plog_db3", "/data2/qspace/temp/plog_db3",
      "/data3/qspace/temp/plog_db3", "/data4/qspace/temp/plog_db3",
  };
  assert(FLAGS_db_num <= int(db_paths.size()));
  db_paths.resize(FLAGS_db_num);

  certain::Options options;
  options.set_plog_worker_num(FLAGS_worker_num);
  options.set_plog_routine_num(FLAGS_routine_num);
  options.set_entity_worker_num(FLAGS_worker_num);
  options.set_db_limited_worker_num(FLAGS_worker_num);
  options.set_db_routine_num(FLAGS_routine_num);
  options.set_db_max_kb_per_second(1 << 20);     // 1GB
  options.set_db_max_count_per_second(1 << 20);  // 1M
  options.set_log(&log);
  auto queue_mng = certain::AsyncQueueMng::GetInstance();

  std::vector<dbtype::DB*> dbs;
  OpenDbs(db_paths, &dbs);
  PlogImpl plog(dbs);
  DbImpl db;

  std::string local_addr = "127.0.0.1:8888";
  std::vector<std::string> addr_list(3, local_addr);
  RouteImpl route_impl(local_addr, addr_list);
  certain::Wrapper::GetInstance()->Init(&options, &route_impl, &plog, &db);

  // 2. write plog
  if (FLAGS_write_plog) {
    std::vector<std::unique_ptr<certain::PlogWorker>> workers;
    for (uint32_t i = 0; i < options.plog_worker_num(); ++i) {
      workers.push_back(
          std::make_unique<certain::PlogWorker>(&options, i, &plog, nullptr));
      workers.back()->Start();
    }
    poll(nullptr, 0, 100);

    uint64_t start = certain::GetTimeByMsec();
    for (int t = 0; t < FLAGS_worker_num; ++t) {
      auto entity_num = FLAGS_entity_num / FLAGS_worker_num;
      std::thread([&, t] {
        certain::ThreadBase::SetThreadName("send_worker_%d", t);
        for (uint64_t i = 0; i < FLAGS_record_num / FLAGS_worker_num; ++i) {
          auto cmd = std::make_unique<certain::PaxosCmd>();
          cmd->set_entity_id(t * entity_num + i % entity_num);
          cmd->set_entry(i / entity_num + 1);
          certain::EntryRecord record;
          record.set_value(std::string(FLAGS_record_size, 'A'));
          record.set_chosen(true);
          cmd->set_local_entry_record(record);
          cmd->set_plog_set_record(true);

          do {
            cmd->set_timestamp_usec(certain::GetTimeByUsec());
            int ret = certain::PlogWorker::GoToPlogReqQueue(cmd);
            if (ret == 0) {
              break;
            }
            poll(nullptr, 0, 1);
          } while (true);
        }
      }).detach();
    }

    std::atomic<uint64_t> finished{0};
    std::atomic<uint64_t> set_delay_sum{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < FLAGS_worker_num; ++t) {
      threads.emplace_back([&, t] {
        certain::ThreadBase::SetThreadName("recv_worker_%d", t);
        auto rsp_queue = queue_mng->GetPlogRspQueueByIdx(t);
        while (finished.load() < FLAGS_record_num) {
          if (rsp_queue->Size() == 0) {
            poll(nullptr, 0, 1);
            continue;
          }

          std::unique_ptr<certain::CmdBase> cmd;
          while (rsp_queue->PopByOneThread(&cmd) != 0) {
            poll(nullptr, 0, 1);
          }
          assert(cmd->result() == 0);
          set_delay_sum += certain::GetTimeByUsec() - cmd->timestamp_usec();

          if ((finished.fetch_add(1) + 1) % 10000000 == 0) {
            printf("%lu, ", certain::GetTimeByMsec() - start);
            fflush(stdout);

            if (FLAGS_log_to_file) {
              std::fstream f("plog_perf.txt", std::ios::out | std::ios::app);
              std::string buffer =
                  std::to_string(certain::GetTimeByMsec() - start) + "\n";
              f.write(buffer.data(), buffer.size());
              f.flush();
            }
          }
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }

    uint64_t set_elapsed = certain::GetTimeByMsec() - start;
    puts("");
    printf("Set %lu Records %u Threads %lums Delay %luus\n", FLAGS_record_num,
           FLAGS_worker_num, set_elapsed,
           set_delay_sum.load() / FLAGS_record_num);
    fflush(stdout);

    for (auto& worker : workers) {
      worker->set_exit_flag(true);
      worker->WaitExit();
    }
  }

  // 3. write mem db
  if (FLAGS_write_db) {
    std::vector<std::unique_ptr<certain::DbLimitedWorker>> workers;
    for (uint32_t i = 0; i < options.db_limited_worker_num(); ++i) {
      workers.push_back(
          std::make_unique<certain::DbLimitedWorker>(&options, i, &db));
      workers.back()->Start();
    }
    poll(nullptr, 0, 100);

    uint64_t start = certain::GetTimeByMsec();
    std::vector<std::thread> threads;
    for (int t = 0; t < FLAGS_worker_num; ++t) {
      auto entity_num = FLAGS_entity_num / FLAGS_worker_num;
      threads.emplace_back([&, t] {
        for (uint64_t i = 0; i < entity_num; ++i) {
          auto cmd =
              std::make_unique<certain::ClientCmd>(certain::CmdIds::kCmdWrite);
          cmd->set_entity_id(t * entity_num + i);
          cmd->set_entry(FLAGS_record_num / FLAGS_entity_num);

          do {
            int ret = certain::DbLimitedWorker::GoToDbLimitedReqQueue(cmd);
            if (ret == 0) {
              break;
            }
            poll(nullptr, 0, 100);
          } while (true);
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    for (uint64_t t = 0; t < FLAGS_entity_num; ++t) {
      uint64_t entry = 0;
      certain::Db::RecoverFlag flag;

      db.GetStatus(t, &entry, &flag);
      while (entry != FLAGS_record_num / FLAGS_entity_num) {
        poll(nullptr, 0, 100);
        db.GetStatus(t, &entry, &flag);
      };
    }

    uint64_t set_elapsed = certain::GetTimeByMsec() - start;
    printf("Set %lu Records %u Threads %lums Delay %lums\n", FLAGS_record_num,
           FLAGS_worker_num, set_elapsed, 0ul);
    fflush(stdout);

    for (auto& worker : workers) {
      worker->set_exit_flag(true);
      worker->WaitExit();
    }
  }
}
