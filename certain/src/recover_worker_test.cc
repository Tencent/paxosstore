#include "src/recover_worker.h"

#include "default/db_impl.h"
#include "default/log_impl.h"
#include "default/route_impl.h"
#include "gtest/gtest.h"
#include "src/wrapper.h"
#include "tiny_rpc/tiny_client.h"
#include "tiny_rpc/tiny_server.h"

static std::atomic<uint32_t> recovered{0};
class MockService : public certain::TinyRpcService {
 public:
  void SnapshotRecover(google::protobuf::RpcController* controller,
                       const certain::SnapshotRecoverReq* req,
                       certain::SnapshotRecoverRsp* rsp,
                       google::protobuf::Closure* done) override {
    poll(nullptr, 0, 50);  // simulate latency
    rsp->set_max_apply_entry(666);
    rsp->set_data("\0\0\0\0", 4);
    ++recovered;
  }
} service;

TEST(RecoverWorker, Normal) {
  certain::Options options;
  options.set_recover_worker_num(1);
  options.set_recover_routine_num(64);
  options.set_recover_max_count_per_second(100);
  options.set_entity_worker_num(1);

  std::vector<std::string> addr_list{
      "127.0.0.1:17066",
      "127.0.0.1:17067",
      "127.0.0.1:17068",
  };
  RouteImpl route_impl(addr_list.front(), addr_list);

  DbImpl db;
  certain::Wrapper::GetInstance()->Init(&options, &route_impl, nullptr, &db);

  // Start Mock RPC Server
  certain::TinyServer server("127.0.0.1:18068", &service);
  ASSERT_EQ(server.Init(), 0);
  server.Start();

  certain::RecoverWorker worker(&options, 0);
  worker.Start();

  ASSERT_EQ(recovered.load(), 0);
  static constexpr uint64_t N = 64;
  for (uint32_t i = 0; i < N; ++i) {
    auto cmd = std::make_unique<certain::PaxosCmd>();
    cmd->set_entity_id(233 + i);
    ASSERT_EQ(certain::RecoverWorker::GoToRecoverReqQueue(cmd), 0);
  }
  poll(nullptr, 0, 80);
  ASSERT_EQ(recovered.load(), N);

  worker.set_exit_flag(true);
  worker.WaitExit();

  server.Stop();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
