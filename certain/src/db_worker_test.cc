#include "src/db_worker.h"

#include "default/db_impl.h"
#include "gtest/gtest.h"
#include "src/wrapper.h"
#include "utils/log.h"
#include "utils/memory.h"

TEST(DbWorker, Normal) {
  DbImpl db;

  certain::Options options;
  options.set_db_worker_num(1);
  options.set_db_limited_worker_num(1);
  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  queue_mng->Init(&options);

  certain::DbWorker worker(&options, 0, &db);
  worker.Start();

  {
    auto cmd = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
    cmd->set_entity_id(233);
    cmd->set_entry(1);
    cmd->set_value("Mock Value");

    ASSERT_EQ(worker.GoToDbReqQueue(cmd), 0);
    poll(nullptr, 0, 10);

    uint64_t max_committed_entry;
    certain::Db::RecoverFlag flag;
    ASSERT_EQ(0, db.GetStatus(233, &max_committed_entry, &flag));
    ASSERT_EQ(1, max_committed_entry);
    ASSERT_EQ(0, flag);

    cmd = std::make_unique<certain::ClientCmd>(certain::kCmdWrite);
    cmd->set_entity_id(233);
    cmd->set_entry(3);
    ASSERT_EQ(worker.GoToDbReqQueue(cmd), 0);
    poll(nullptr, 0, 10);
    ASSERT_EQ(queue_mng->GetDbLimitedReqQueueByIdx(0)->Size(), 1);
  }

  worker.set_exit_flag(true);
  worker.WaitExit();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
