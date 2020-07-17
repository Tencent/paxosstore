#include "src/entity_worker.h"

#include "certain/monitor.h"
#include "certain/route.h"
#include "default/log_impl.h"
#include "gtest/gtest.h"
#include "src/wrapper.h"

class MockRoute : public certain::Route {
 public:
  virtual std::string GetLocalAddr() override { return ""; }

  virtual int GetLocalAcceptorId(uint64_t entity_id,
                                 uint32_t* acceptor_id) override {
    *acceptor_id = entity_id % 3;
    return 0;
  }

  virtual int GetServerAddrId(uint64_t entity_id, uint32_t acceptor_id,
                              uint64_t* addr_id) override {
    return -1;
  }
};

class MockDb : public certain::Db {
  int Commit(uint64_t entity_id, uint64_t entry, const std::string& value) {
    return 0;
  }

  int GetStatus(uint64_t entity_id, uint64_t* max_committed_entry,
                RecoverFlag* flag) {
    return 0;
  }

  int SnapshotRecover(uint64_t entity_id, uint32_t start_acceptor_id,
                      uint64_t* max_committed_entry) {
    return 0;
  }

  void LockEntity(uint64_t entity_id) {}

  void UnlockEntity(uint64_t entity_id) {}
};

TEST(EntityWorker, Normal) {
  certain::Options options;
  options.set_entity_worker_num(1);
  MockRoute route;
  MockDb db;

  auto wrapper = certain::Wrapper::GetInstance();
  ASSERT_EQ(wrapper->Init(&options, &route, nullptr, &db), 0);

  poll(nullptr, 0, 100);
  wrapper->set_exit_flag(true);
  wrapper->WaitExit();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
