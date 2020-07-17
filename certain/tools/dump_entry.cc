#include "default/log_impl.h"
#include "gflags/gflags.h"
#include "proto/tools.pb.h"
#include "tiny_rpc/tiny_client.h"

DEFINE_string(server_ip, "127.0.0.1", "server's ip");
DEFINE_int32(server_port, 11066, "server's port");
DEFINE_uint64(entity_id, 1, "Entity Id");
DEFINE_uint64(entry, 1, "Entry");

int main(int argc, char* argv[]) {
  // 1. Parse arguments.
  google::ParseCommandLineFlags(&argc, &argv, true);

  auto server_addr = FLAGS_server_ip + ":" + std::to_string(FLAGS_server_port);
  uint64_t entity_id = FLAGS_entity_id;
  uint64_t entry = FLAGS_entry;

  printf("server_addr: %s entity_id %lu entry %lu\n", server_addr.c_str(),
         entity_id, entry);

  // 2. Init log.
  LogImpl log("mm", "./test_log.o", certain::LogLevel::kInfo, 1);
  int ret = log.Init();
  if (ret != 0) {
    printf("Log Init ret %d", ret);
    return -1;
  }
  certain::Log::GetInstance()->Init(&log);

  // 3. Execute cmd.
  certain::TinyController controller;
  certain::TinyChannel channel(server_addr);
  certain::ToolsService_Stub client(&channel);
  certain::DumpEntryReq req;
  req.set_entity_id(entity_id);
  req.set_entry(entry);
  certain::DumpEntryRsp rsp;
  client.DumpEntry(&controller, &req, &rsp, nullptr);
  printf("DumpEntry entity_id %lu entry %lu ret %d\n", entity_id, entry,
         controller.RetCode());

  return 0;
}
