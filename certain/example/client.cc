#include "default/log_impl.h"
#include "gflags/gflags.h"
#include "tiny_rpc/tiny_client.h"

DEFINE_string(cmd, "write", "Select write/read/appendstring/getstringstatus.");
DEFINE_string(server_ip, "127.0.0.1", "server's ip");
DEFINE_int32(server_port, 11066, "server's port");
DEFINE_uint64(entity_id, 13131, "Entity Id");
DEFINE_uint64(entry, 10, "Entry");
DEFINE_string(value, "hello", "The value for write cmd.");

int main(int argc, char* argv[]) {
  // 1. Parse arguments.
  google::ParseCommandLineFlags(&argc, &argv, true);

  auto server_addr = FLAGS_server_ip + ":" + std::to_string(FLAGS_server_port);
  uint64_t entity_id = FLAGS_entity_id;
  uint64_t entry = FLAGS_entry;

  printf("server_addr: %s cmd %s entity_id %lu entry %lu value.sz %ld\n",
         server_addr.c_str(), FLAGS_cmd.c_str(), entity_id, entry,
         FLAGS_value.size());

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
  if (FLAGS_cmd == "write") {
    std::string value = FLAGS_value;
    certain::TinyClient client(server_addr);
    certain::WriteReq req;
    req.set_entity_id(entity_id);
    req.set_entry(entry);
    req.set_value(value);
    certain::WriteRsp rsp;
    client.Write(&controller, &req, &rsp, nullptr);
    printf("Write E(%lu, %lu) value.sz %lu ret %d\n", entity_id, entry,
           value.size(), controller.RetCode());
  } else if (FLAGS_cmd == "read") {
    certain::TinyClient client(server_addr);
    certain::ReadReq req;
    req.set_entity_id(entity_id);
    req.set_entry(entry);
    certain::ReadRsp rsp;
    client.Read(&controller, &req, &rsp, nullptr);
    printf("Read E(%lu, %lu) ret %d\n", entity_id, entry, controller.RetCode());
  } else if (FLAGS_cmd == "appendstring") {
    certain::TinyClient client(server_addr);
    certain::AppendStringReq req;
    req.set_entity_id(entity_id);
    req.set_value(FLAGS_value);
    certain::AppendStringRsp rsp;
    client.AppendString(&controller, &req, &rsp, nullptr);
    printf("AppendString entity_id %lu value.sz %lu ret %d\n", entity_id,
           FLAGS_value.size(), controller.RetCode());
    printf("rsp.write_entry %lu rsp.current_entry %lu rsp.current_crc32 %u\n",
           rsp.write_entry(), rsp.current_entry(), rsp.current_crc32());
  } else if (FLAGS_cmd == "getstringstatus") {
    certain::TinyClient client(server_addr);
    certain::GetStringStatusReq req;
    req.set_entity_id(entity_id);
    certain::GetStringStatusRsp rsp;
    client.GetStringStatus(&controller, &req, &rsp, nullptr);
    printf("GetStringStatus entity_id %lu value.sz %lu ret %d\n", entity_id,
           FLAGS_value.size(), controller.RetCode());
    printf("rsp.read_entry %lu rsp.current_entry %lu rsp.current_crc32 %u\n",
           rsp.read_entry(), rsp.current_entry(), rsp.current_crc32());
  } else {
    assert(false);
  }

  return 0;
}
