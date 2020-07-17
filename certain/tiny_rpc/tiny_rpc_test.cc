#include "gtest/gtest.h"
#include "tiny_rpc/tiny_client.h"
#include "tiny_rpc/tiny_server.h"
#include "utils/log.h"

using google::protobuf::Closure;
using google::protobuf::RpcController;

class TinyServiceMock : public certain::TinyRpcService {
  void SnapshotRecover(RpcController* controller,
                       const certain::SnapshotRecoverReq* req,
                       certain::SnapshotRecoverRsp* rsp,
                       Closure* done) override {
    rsp->set_max_apply_entry(req->entity_id() * 1000);
    ((certain::TinyController*)controller)->SetRetCode(0);
  }

  void Write(RpcController* controller, const certain::WriteReq* req,
             certain::WriteRsp* rsp, Closure* done) override {
    ((certain::TinyController*)controller)->SetRetCode(req->entity_id() * 100);
  }

  void Read(RpcController* controller, const certain::ReadReq* req,
            certain::ReadRsp* rsp, Closure* done) override {
    ((certain::TinyController*)controller)->SetRetCode(req->entity_id() * 10);
  }

  void AppendString(RpcController* controller,
                    const certain::AppendStringReq* req,
                    certain::AppendStringRsp* rsp, Closure* done) override {
    ((certain::TinyController*)controller)->SetRetCode(req->entity_id() * 1000);
  }

  void GetStringStatus(RpcController* controller,
                       const certain::GetStringStatusReq* req,
                       certain::GetStringStatusRsp* rsp,
                       Closure* done) override {
    rsp->set_current_crc32(222);
    rsp->set_current_entry(666);
    ((certain::TinyController*)controller)->SetRetCode(req->entity_id() * 666);
  }
} service;

TEST(TinyRpc, TinyServerImpl_SnapshotRecover) {
  std::string server_addr = "127.0.0.1:13069";
  certain::TinyServer server(server_addr, &service);
  ASSERT_EQ(server.Init(), 0);
  server.Start();
  usleep(10 * 1000);

  for (uint64_t entity_id = 1; entity_id < 10; ++entity_id) {
    certain::TinyClient client(server_addr);
    certain::TinyController controller;
    certain::SnapshotRecoverReq req;
    req.set_entity_id(entity_id);
    certain::SnapshotRecoverRsp rsp;
    client.SnapshotRecover(&controller, &req, &rsp, nullptr);
    ASSERT_EQ(controller.RetCode(), 0);
    ASSERT_EQ(rsp.max_apply_entry(), entity_id * 1000);
  }

  server.Stop();
}

TEST(TinyRpc, TinyServerImpl_Write) {
  std::string server_addr = "127.0.0.1:13069";
  certain::TinyServer server(server_addr, &service);
  ASSERT_EQ(server.Init(), 0);
  server.Start();
  usleep(10 * 1000);

  for (uint64_t entity_id = 1; entity_id < 10; ++entity_id) {
    certain::TinyClient client(server_addr);
    certain::TinyController controller;
    certain::WriteReq req;
    req.set_entity_id(entity_id);
    certain::WriteRsp rsp;
    client.Write(&controller, &req, &rsp, nullptr);
    ASSERT_EQ(controller.RetCode(), entity_id * 100);
  }

  server.Stop();
}

TEST(TinyRpc, TinyServerImpl_Read) {
  std::string server_addr = "127.0.0.1:13069";
  certain::TinyServer server(server_addr, &service);
  ASSERT_EQ(server.Init(), 0);
  server.Start();
  usleep(10 * 1000);

  for (uint64_t entity_id = 1; entity_id < 10; ++entity_id) {
    certain::TinyClient client(server_addr);
    certain::TinyController controller;
    certain::ReadReq req;
    req.set_entity_id(entity_id);
    certain::ReadRsp rsp;
    client.Read(&controller, &req, &rsp, nullptr);
    ASSERT_EQ(controller.RetCode(), entity_id * 10);
  }

  server.Stop();
}

TEST(TinyRpc, TinyServerImpl_AppendString) {
  std::string server_addr = "127.0.0.1:13069";
  certain::TinyServer server(server_addr, &service);
  ASSERT_EQ(server.Init(), 0);
  server.Start();
  usleep(10 * 1000);

  for (uint64_t entity_id = 1; entity_id < 10; ++entity_id) {
    certain::TinyClient client(server_addr);
    certain::TinyController controller;
    certain::AppendStringReq req;
    req.set_entity_id(entity_id);
    certain::AppendStringRsp rsp;
    client.AppendString(&controller, &req, &rsp, nullptr);
    ASSERT_EQ(controller.RetCode(), entity_id * 1000);
  }

  server.Stop();
}

TEST(TinyRpc, TinyServerImpl_GetStringStatus) {
  std::string server_addr = "127.0.0.1:13069";
  certain::TinyServer server(server_addr, &service);
  ASSERT_EQ(server.Init(), 0);
  server.Start();
  usleep(10 * 1000);

  for (uint64_t entity_id = 1; entity_id < 10; ++entity_id) {
    certain::TinyClient client(server_addr);
    certain::TinyController controller;
    certain::GetStringStatusReq req;
    req.set_entity_id(entity_id);
    certain::GetStringStatusRsp rsp;
    client.GetStringStatus(&controller, &req, &rsp, nullptr);
    ASSERT_EQ(controller.RetCode(), entity_id * 666);
  }

  server.Stop();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
