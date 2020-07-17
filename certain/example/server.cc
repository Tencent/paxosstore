#include "certain/certain.h"
#include "default/db_impl.h"
#include "default/log_impl.h"
#include "default/plog_impl.h"
#include "default/route_impl.h"
#include "default/tiny_service_impl.h"
#include "gflags/gflags.h"
#include "tiny_rpc/tiny_server.h"
#include "tools/tools_service.h"

bool g_stop = false;

DEFINE_string(ip0, "127.0.0.1", "Node0's ip");
DEFINE_string(ip1, "127.0.0.1", "Node1's ip");
DEFINE_string(ip2, "127.0.0.1", "Node2's ip");

DEFINE_int32(port0, 10066, "Node0's port");
DEFINE_int32(port1, 10067, "Node1's port");
DEFINE_int32(port2, 10068, "Node2's port");

DEFINE_int32(index, 0, "Use ip0:port0 as local address for node index 0.");
DEFINE_int32(server_port_offset, 1000, "Node0's server port is port0+offset.");
DEFINE_int32(log_level, 4,
             "kZero=0,kFatal=1,kError=2,kWarn=3,kInfo=4,kDebug=5");

int main(int argc, char* argv[]) {
  // 1. Parse arguments.
  google::ParseCommandLineFlags(&argc, &argv, true);

  google::CommandLineFlagInfo info;
  if ((GetCommandLineFlagInfo("index", &info) && info.is_default) ||
      !(0 <= FLAGS_index && FLAGS_index <= 2)) {
    printf("Local node index must be set, range 0 to 2.\n");
    return -1;
  }

  std::vector<std::string> addr_list;
  addr_list.push_back(FLAGS_ip0 + ":" + std::to_string(FLAGS_port0));
  addr_list.push_back(FLAGS_ip1 + ":" + std::to_string(FLAGS_port1));
  addr_list.push_back(FLAGS_ip2 + ":" + std::to_string(FLAGS_port2));
  std::string local_addr = addr_list[FLAGS_index];

  std::vector<int32_t> ports = {FLAGS_port0, FLAGS_port1, FLAGS_port2};
  std::vector<std::string> ips = {FLAGS_ip0, FLAGS_ip1, FLAGS_ip2};
  int32_t port = ports[FLAGS_index] + FLAGS_server_port_offset;
  std::string server_addr = ips[FLAGS_index] + ":" + std::to_string(port);
  std::string tools_addr =
      ips[FLAGS_index] + ":" + std::to_string(port + FLAGS_server_port_offset);

  printf("node0: %s node1: %s node2: %s local_addr: %s server_addr: %s\n",
         addr_list[0].c_str(), addr_list[1].c_str(), addr_list[2].c_str(),
         local_addr.c_str(), server_addr.c_str());

#ifdef LEVELDB_COMM
  leveldb::set_use_direct_io(1);
#endif

  // 2. Initialize and start certain.
  certain::Options options;
  options.set_entry_timeout_sec(30);
  RouteImpl route_impl(local_addr, addr_list);
  DbImpl db_impl;

  dbtype::DB* db4plog = nullptr;
  {
    dbtype::Options options;
    options.create_if_missing = true;
    dbtype::Status status = dbtype::DB::Open(options, "test_plog.o", &db4plog);
    assert(status.ok());
  }
  PlogImpl plog_impl(db4plog);

  int ret;
  LogImpl log_impl("mm", "./test_log.o",
                   static_cast<certain::LogLevel>(FLAGS_log_level));
  options.set_log(&log_impl);
  ret = certain::Certain::Init(&options, &route_impl, &plog_impl, &db_impl);
  if (ret != 0) {
    printf("certain::Certain::Init ret %d\n", ret);
    return -1;
  }
  certain::Certain::Start();

  // 3. Start server.
  TinyServiceImpl service;
  certain::TinyServer server(server_addr, &service);
  ret = server.Init();
  if (ret != 0) {
    printf("server.Init ret %d\n", ret);
    return -2;
  }
  server.Start();

  certain::ToolsServiceImpl tools_service;
  certain::TinyServer tools_server(tools_addr, &tools_service);
  ret = tools_server.Init();
  if (ret != 0) {
    printf("tools_server.Init ret %d\n", ret);
    return -3;
  }
  tools_server.Start();

  // 4. Exit if stop.
  while (!g_stop) {
    sleep(1);
  }
  certain::Certain::Stop();
  return 0;
}
