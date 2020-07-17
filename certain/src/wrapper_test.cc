#include "src/wrapper.h"

#include <future>
#include <thread>

#include "certain/certain.h"
#include "certain/errors.h"
#include "default/log_impl.h"
#include "default/route_impl.h"
#include "gtest/gtest.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "src/entity_info_mng.h"

class MockDb : public certain::Db {
 public:
  int Commit(uint64_t entity_id, uint64_t entry, const std::string& value) {
    map_[entity_id] = std::make_pair(entry, certain::Db::kNormal);
    return 0;
  }

  int GetStatus(uint64_t entity_id, uint64_t* max_committed_entry,
                RecoverFlag* flag) {
    std::tie(*max_committed_entry, *flag) = map_[entity_id];
    return 0;
  }

  int SnapshotRecover(uint64_t entity_id, uint32_t start_acceptor_id,
                      uint64_t* max_committed_entry) {
    return 0;
  }

  void LockEntity(uint64_t entity_id) {}

  void UnlockEntity(uint64_t entity_id) {}

  // for unittest
  int SetStatus(uint64_t entity_id, uint64_t max_committed_entry,
                RecoverFlag flag) {
    map_[entity_id] = std::make_pair(max_committed_entry, flag);
    return 0;
  }

 private:
  std::unordered_map<uint64_t, std::pair<uint64_t, certain::Db::RecoverFlag>>
      map_;
};

class MockPlog : public certain::Plog {
 public:
  virtual int LoadMaxEntry(uint64_t entity_id, uint64_t* entry) { return -233; }

  virtual int GetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       std::string* value) {
    return 0;
  }

  virtual int SetValue(uint64_t entity_id, uint64_t entry, uint64_t value_id,
                       const std::string& value) {
    return 0;
  }

  virtual int GetRecord(uint64_t entity_id, uint64_t entry,
                        std::string* record) {
    poll(nullptr, 0, 1);
    certain::EntryRecord r;
    r.set_chosen(true);
    return r.SerializeToString(record) ? 0 : -1;
  }

  virtual int SetRecord(uint64_t entity_id, uint64_t entry,
                        const std::string& record) {
    return 0;
  }

  virtual uint32_t HashId(uint64_t entity_id) { return 0; }

  virtual int MultiSetRecords(uint32_t hash_id,
                              const std::vector<Record>& records) {
    return 0;
  }

  // Get and set entrys range in [begin_entry, end_entry) to records.
  virtual int RangeGetRecord(
      uint64_t entity_id, uint64_t begin_entry, uint64_t end_entry,
      std::vector<std::pair<uint64_t, std::string>>* records) {
    return 0;
  }
};

TEST(Wrapper, Normal) {
  certain::Options options;
  options.set_entity_worker_num(1);
  options.set_recover_worker_num(1);
  options.set_recover_routine_num(64);
  options.set_db_limited_worker_num(1);

  std::vector<std::string> addr_list{
      "127.0.0.1:12066",
      "127.0.0.1:12067",
      "127.0.0.1:12068",
  };
  RouteImpl route(addr_list.front(), addr_list);
  MockDb db;
  MockPlog plog;
  LogImpl log("mm", "./test_log.o", certain::LogLevel::kInfo);

  auto wrapper = certain::Wrapper::GetInstance();
  ASSERT_EQ(wrapper->Init(&options, &route, &plog, &db), 0);
  wrapper->Start();

  auto queue_mng = certain::AsyncQueueMng::GetInstance();
  auto user_req_queue = queue_mng->GetUserReqQueueByIdx(0);
  auto db_limited_queue = queue_mng->GetDbLimitedReqQueueByIdx(0);

  const uint64_t kEntityId = 233;
  const uint64_t kMaxCommittedEntry = 10;
  uint64_t entry;

  {
    // db in recover
    db.SetStatus(kEntityId, kMaxCommittedEntry, certain::Db::kRecover);
    ASSERT_EQ(certain::Certain::Replay(certain::CmdOptions::Default(),
                                       kEntityId, &entry),
              certain::kRetCodeRecoverPending);
    ASSERT_EQ(user_req_queue->Size(), 1);
    std::unique_ptr<certain::CmdBase> cmd;
    ASSERT_EQ(user_req_queue->PopByOneThread(&cmd), 0);
    ASSERT_EQ(cmd->entity_id(), kEntityId);
    ASSERT_EQ(cmd->cmd_id(), certain::kCmdRecover);
  }

  {
    // entity info not in memory, trigger catch up
    db.SetStatus(kEntityId, kMaxCommittedEntry, certain::Db::kNormal);
    ASSERT_EQ(certain::Certain::Replay(certain::CmdOptions::Default(),
                                       kEntityId, &entry),
              -233);
  }

  {
    // max_committed_entry + max_replay_num < max_chosen_entry
    // trigger db async replay
    certain::EntityInfoMng info_mng(&options);
    auto info = info_mng.CreateEntityInfo(kEntityId, 3, 0);
    info->max_chosen_entry = info->max_cont_chosen_entry =
        kMaxCommittedEntry + options.max_replay_num() + 1;
    ASSERT_EQ(certain::Certain::Replay(certain::CmdOptions::Default(),
                                       kEntityId, &entry),
              certain::kRetCodeReplayPending);
    ASSERT_EQ(db_limited_queue->Size(), 1);
    std::unique_ptr<certain::CmdBase> cmd;
    ASSERT_EQ(db_limited_queue->PopByOneThread(&cmd), 0);
    ASSERT_EQ(cmd->entity_id(), kEntityId);
    ASSERT_EQ(cmd->cmd_id(), certain::kCmdReplay);
    info_mng.DestroyEntityInfo(info);
  }

  {
    // normal replay
    certain::EntityInfoMng info_mng(&options);
    auto info = info_mng.CreateEntityInfo(kEntityId, 3, 0);
    ASSERT_TRUE(info != nullptr);
    info->max_chosen_entry = info->max_cont_chosen_entry =
        kMaxCommittedEntry + options.max_replay_num();
    ASSERT_EQ(certain::Certain::Replay(certain::CmdOptions::Default(),
                                       kEntityId, &entry),
              0);
    ASSERT_EQ(entry, info->max_chosen_entry);
    info_mng.DestroyEntityInfo(info);
  }

  {
    // timeout replay
    db.SetStatus(kEntityId, kMaxCommittedEntry, certain::Db::kNormal);
    certain::EntityInfoMng info_mng(&options);
    auto info = info_mng.CreateEntityInfo(kEntityId, 3, 0);
    ASSERT_TRUE(info != nullptr);
    info->max_chosen_entry = info->max_cont_chosen_entry =
        kMaxCommittedEntry + options.max_replay_num();
    certain::CmdOptions cmd_options;
    cmd_options.set_client_cmd_timeout_msec(1);
    ASSERT_EQ(certain::Certain::Replay(cmd_options, kEntityId, &entry),
              certain::kRetCodeTimeout);
    info_mng.DestroyEntityInfo(info);
  }

  std::vector<uint64_t> uuids;
  ASSERT_EQ(
      certain::Certain::Write(certain::CmdOptions::Default(), 1, 1, "", uuids),
      -233);
  ASSERT_EQ(certain::Certain::Read(certain::CmdOptions::Default(), 1, 1), -233);

  wrapper->set_exit_flag(true);
  wrapper->WaitExit();
  wrapper->Destroy();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
