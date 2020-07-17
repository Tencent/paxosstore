#include <atomic>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "default/db_impl.h"
#include "default/log_impl.h"
#include "gflags/gflags.h"
#include "src/command.h"
#include "src/db_worker.h"
#include "utils/memory.h"
#include "utils/time.h"

DEFINE_uint64(entity_num, 20000000, "Entity Num");
DEFINE_uint64(record_num, 1000000, "Record Num");
DEFINE_int32(record_size, 100, "Record Size");
DEFINE_int32(worker_num, 8, "Worker Num");
DEFINE_int32(routine_num, 128, "Routine Num");
DEFINE_int32(db_num, 8, "DB Num");
DEFINE_int32(aio, 1, "AIO Mode (1 or 2)");
DEFINE_bool(write, true, "Write Mode");
DEFINE_bool(read, true, "Read Mode");
DEFINE_bool(log_to_file, false, "Log Time to File");

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_record_num % FLAGS_worker_num == 0);

  LogImpl log("mm", "./test_log.o", certain::LogLevel::kInfo, true);
  assert(log.Init() == 0);
  certain::Log::GetInstance()->Init(&log);

  certain::Options options;
  options.set_db_worker_num(FLAGS_worker_num);
  options.set_db_routine_num(FLAGS_routine_num);
  options.set_entity_worker_num(FLAGS_worker_num);
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  queue_mng->Init(&options);

  DbImpl db;

  // 2. write records
  if (FLAGS_write) {
    std::vector<std::unique_ptr<certain::DbWorker>> workers;
    for (uint32_t i = 0; i < options.db_worker_num(); ++i) {
      workers.push_back(std::make_unique<certain::DbWorker>(&options, i, &db));
      workers.back()->Start();
    }
    poll(nullptr, 0, 100);

    uint64_t start = certain::GetTimeByMsec();
    std::vector<std::thread> threads;
    for (int t = 0; t < FLAGS_worker_num; ++t) {
      auto entity_num = FLAGS_entity_num / FLAGS_worker_num;
      threads.emplace_back([&, t] {
        for (uint64_t i = 0; i < FLAGS_record_num / FLAGS_worker_num; ++i) {
          auto cmd =
              std::make_unique<certain::ClientCmd>(certain::CmdIds::kCmdWrite);
          cmd->set_entity_id(t * entity_num + i % entity_num);
          cmd->set_entry(i / entity_num + 1);
          cmd->set_value(std::string(FLAGS_record_size, 'A'));

          do {
            int ret = certain::DbWorker::GoToDbReqQueue(cmd);
            if (ret == 0) {
              break;
            }
            poll(nullptr, 0, 1);
          } while (true);
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    for (int t = 0; t < FLAGS_worker_num; ++t) {
      auto queue = queue_mng->GetDbReqQueueByIdx(t);
      while (queue->Size()) {
        poll(nullptr, 0, 10);
      }
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
