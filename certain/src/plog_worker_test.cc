#include "src/plog_worker.h"

#include "default/db_impl.h"
#include "default/plog_impl.h"
#include "gtest/gtest.h"
#ifdef LEVELDB_COMM
#include "util/aio.h"
#endif

TEST(PlogWorker, Normal) {
  dbtype::DB* db = nullptr;
  {
    dbtype::Options options;
    options.create_if_missing = true;
    dbtype::Status status = dbtype::DB::Open(options, "test_plog.o", &db);
    ASSERT_TRUE(status.ok());
  }
  PlogImpl plog(db);

  certain::Options options;
  options.set_plog_worker_num(1);
  options.set_plog_readonly_worker_num(1);
  options.set_entity_worker_num(1);
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  queue_mng->Init(&options);

  certain::PlogWorker worker(&options, 0, &plog, nullptr);
  worker.Start();

  auto rsp_queue = queue_mng->GetPlogRspQueueByIdx(0);

  poll(nullptr, 0, 10);
  ASSERT_EQ(rsp_queue->Size(), 0);

  {
    // set record
    auto cmd = std::make_unique<certain::PaxosCmd>();
    cmd->set_entity_id(233);
    cmd->set_entry(1);
    certain::EntryRecord record;
    record.set_prepared_num(2020);
    cmd->set_local_entry_record(record);
    cmd->set_plog_set_record(true);

    ASSERT_EQ(certain::PlogWorker::GoToPlogReqQueue(cmd), 0);
    poll(nullptr, 0, 20);

    ASSERT_EQ(rsp_queue->Size(), 1);
    std::unique_ptr<certain::CmdBase> out;
    ASSERT_EQ(rsp_queue->PopByOneThread(&out), 0);
    ASSERT_EQ(out->result(), 0);
  }

  worker.set_exit_flag(true);
  worker.WaitExit();
  delete db;
}

TEST(PlogReadonlyWorker, Normal) {
  dbtype::DB* level_db = nullptr;
  {
    dbtype::Options options;
    options.create_if_missing = true;
    dbtype::Status status = dbtype::DB::Open(options, "test_plog.o", &level_db);
    ASSERT_TRUE(status.ok());
  }
  PlogImpl plog(level_db);
  DbImpl db;

  certain::Options options;
  options.set_plog_readonly_worker_num(1);
  options.set_entity_worker_num(1);
  auto queue_mng = certain::AsyncQueueMng::GetInstance();

  certain::PlogReadonlyWorker worker(&options, 0, &plog, &db);
  worker.Start();

  auto rsp_queue = queue_mng->GetPlogRspQueueByIdx(0);

  poll(nullptr, 0, 10);
  ASSERT_EQ(rsp_queue->Size(), 0);

  {
    // load max entry
    auto cmd = std::make_unique<certain::PaxosCmd>();
    cmd->set_entity_id(233);
    cmd->set_plog_load(true);

    ASSERT_EQ(certain::PlogWorker::GoToPlogReqQueue(cmd), 0);
    poll(nullptr, 0, 20);

    ASSERT_EQ(rsp_queue->Size(), 1);
    std::unique_ptr<certain::CmdBase> out;
    ASSERT_EQ(rsp_queue->PopByOneThread(&out), 0);
    ASSERT_EQ(out->result(), 0);
    ASSERT_EQ(out->entry(), 1);
  }

  {
    // get record
    auto cmd = std::make_unique<certain::PaxosCmd>();
    cmd->set_entity_id(233);
    cmd->set_entry(1);
    cmd->set_plog_get_record(true);

    ASSERT_EQ(certain::PlogWorker::GoToPlogReqQueue(cmd), 0);
    poll(nullptr, 0, 20);

    ASSERT_EQ(rsp_queue->Size(), 1);
    std::unique_ptr<certain::CmdBase> out;
    ASSERT_EQ(rsp_queue->PopByOneThread(&out), 0);
    ASSERT_EQ(out->result(), 0);

    const auto& record = ((certain::PaxosCmd*)out.get())->local_entry_record();
    ASSERT_EQ(record.prepared_num(), 2020);
  }

  {
    // get record
    auto cmd = std::make_unique<certain::PaxosCmd>();
    cmd->set_entity_id(233);
    cmd->set_entry(2);
    cmd->set_plog_get_record(true);

    ASSERT_EQ(certain::PlogWorker::GoToPlogReqQueue(cmd), 0);
    poll(nullptr, 0, 20);

    ASSERT_EQ(rsp_queue->Size(), 1);
    std::unique_ptr<certain::CmdBase> out;
    ASSERT_EQ(rsp_queue->PopByOneThread(&out), 0);
    ASSERT_EQ(out->result(), certain::kImplPlogNotFound);
  }

  worker.set_exit_flag(true);
  worker.WaitExit();
  delete level_db;
}

int main(int argc, char** argv) {
#ifdef LEVELDB_COMM
  leveldb::set_use_direct_io(1);
#endif
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
