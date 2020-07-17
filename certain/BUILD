load("/code_generator/sktools", "protoc")

cppflags = [
  "-Wall",
  "-Werror",
  "-std=c++11",
  "-g",
  "-DLEVELDB_PLATFORM_POSIX",
]

protoc(
  name = "certain.proto",
  srcs = [
    "proto/certain.proto",
  ],
  outs = [
    "proto/certain.pb.cc",
    "proto/certain.pb.h",
  ],
)

cc_library(
  name = "certain_proto",
  srcs = [
    "proto/certain.pb.cc",
  ],
  hdrs = [
    "proto/certain.pb.h",
  ],
  deps = [
    "//mm3rd/protobuf:protobuf",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

protoc(
  name = "tiny_rpc.proto",
  srcs = [
    "proto/tiny_rpc.proto",
  ],
  outs = [
    "proto/tiny_rpc.pb.cc",
    "proto/tiny_rpc.pb.h",
  ],
)

cc_library(
  name = "tiny_rpc_proto",
  srcs = [
    "proto/tiny_rpc.pb.cc",
  ],
  hdrs = [
    "proto/tiny_rpc.pb.h",
  ],
  deps = [
    "//mm3rd/protobuf:protobuf",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

protoc(
  name = "tools.proto",
  srcs = [
    "proto/tools.proto",
  ],
  outs = [
    "proto/tools.pb.cc",
    "proto/tools.pb.h",
  ],
)

cc_library(
  name = "tools_proto",
  srcs = [
    "proto/tools.pb.cc",
  ],
  hdrs = [
    "proto/tools.pb.h",
  ],
  deps = [
    "//mm3rd/protobuf:protobuf",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

cc_library(
  name = "utils",
  srcs = [
    "utils/co_lock.cc",
    "utils/crc32.cc",
    "utils/mem_pool.cc",
    "utils/thread.cc",
  ],
  hdrs = [
    "include/certain/log.h",
    "utils/array_timer.h",
    "utils/capacity_limiter.h",
    "utils/co_lock.h",
    "utils/crc32.h",
    "utils/hash.h",
    "utils/header.h",
    "utils/light_list.h",
    "utils/lock_free_queue.h",
    "utils/log.h",
    "utils/lru_table.h",
    "utils/macro_helper.h",
    "utils/memory.h",
    "utils/mem_pool.h",
    "utils/routine_worker.h",
    "utils/singleton.h",
    "utils/thread.h",
    "utils/time.h",
    "utils/traffic_limiter.h",
    "utils/usetime_stat.h",
    "utils/uuid_mng.h",
  ],
  deps = [
    "//basic/colib:colib",
    "//mm3rd/glog:glog",
  ],
  includes = [
    ".",
    "include",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

cc_library(
  name = "network",
  srcs = [
    "network/msg_channel.cc",
    "network/msg_header.cc",
    "network/poller.cc",
    "network/tcp_socket.cc",
    "network/write_item_list.cc",
  ],
  hdrs = [
    "network/fd_obj.h",
    "network/inet_addr.h",
    "network/msg_channel.h",
    "network/msg_header.h",
    "network/poller.h",
    "network/tcp_socket.h",
    "network/write_item_list.h",
  ],
  includes = [
    ".",
    "include",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

cc_library(
  name = "tiny_rpc",
  srcs = [
    "tiny_rpc/tiny_client.cc",
    "tiny_rpc/tiny_rpc.cc",
    "tiny_rpc/tiny_server.cc",
  ],
  hdrs = [
    "tiny_rpc/tiny_client.h",
    "tiny_rpc/tiny_rpc.h",
    "tiny_rpc/tiny_server.h",
  ],
  includes = ["."],
  deps = [
    ":network",
    ":tiny_rpc_proto",
    ":utils",
    "//mm3rd/protobuf:protobuf",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

cc_library(
  name = "certain",
  srcs = [
    "src/async_queue_mng.cc",
    "src/async_queue_mng.h",
    "src/catchup_worker.cc",
    "src/catchup_worker.h",
    "src/certain.cc",
    "src/command.cc",
    "src/command.h",
    "src/common.h",
    "src/conn_mng.cc",
    "src/conn_mng.h",
    "src/conn_worker.cc",
    "src/conn_worker.h",
    "src/db_limited_worker.cc",
    "src/db_limited_worker.h",
    "src/db_worker.cc",
    "src/db_worker.h",
    "src/entity_helper.cc",
    "src/entity_helper.h",
    "src/entity_info_mng.cc",
    "src/entity_info_mng.h",
    "src/entity_worker.cc",
    "src/entity_worker.h",
    "src/entry_info_mng.cc",
    "src/entry_info_mng.h",
    "src/entry_state.cc",
    "src/entry_state.h",
    "src/libco_notify_helper.cc",
    "src/libco_notify_helper.h",
    "src/msg_channel_helper.cc",
    "src/msg_channel_helper.h",
    "src/msg_serialize.cc",
    "src/msg_serialize.h",
    "src/msg_worker.cc",
    "src/msg_worker.h",
    "src/notify_helper.cc",
    "src/notify_helper.h",
    "src/plog_worker.cc",
    "src/plog_worker.h",
    "src/recover_worker.cc",
    "src/recover_worker.h",
    "src/tools_worker.cc",
    "src/tools_worker.h",
    "src/wrapper.cc",
    "src/wrapper.h",
  ],
  hdrs = [
    "include/certain/certain.h",
    "include/certain/db.h",
    "include/certain/errors.h",
    "include/certain/options.h",
    "include/certain/plog.h",
    "include/certain/route.h",
  ],
  includes = [
    ".",
    "include",
  ],
  deps = [
    ":certain_proto",
    ":network",
    ":utils",
    "//basic/colib:colib",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

cc_library(
  name = "tools",
  srcs = [
    "tools/tools_service.cc",
  ],
  hdrs = [
    "tools/tools_service.h",
  ],
  deps = [
    ":certain",
    ":default",
    ":tiny_rpc",
    ":tools_proto",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)

cc_library(
  name = "default",
  srcs = [
    "default/db_impl.cc",
    "default/log_impl.cc",
    "default/plog_impl.cc",
    "default/route_impl.cc",
    "default/tiny_service_impl.cc",
  ],
  hdrs = [
    "default/db_impl.h",
    "default/db_type.h",
    "default/log_impl.h",
    "default/monitor_impl.h",
    "default/plog_impl.h",
    "default/route_impl.h",
    "default/tiny_service_impl.h",
  ],
  deps = [
    ":certain",
    ":tiny_rpc",
    ":utils",
    "//platform/leveldb-comm:leveldb",
    "//basic/colib:colib",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  copts = cppflags,
  visibility = ["//visibility:public"],
)


### utils tests ###

cc_test(
  name = "array_timer_test",
  srcs = [
    "utils/array_timer_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "capacity_limiter_test",
  srcs = [
    "utils/capacity_limiter_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "crc32_test",
  srcs = [
    "utils/crc32_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "hash_test",
  srcs = [
    "utils/hash_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "light_list_test",
  srcs = [
    "utils/light_list_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "lock_free_queue_test",
  srcs = [
    "utils/lock_free_queue_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "log_test",
  srcs = [
    "default/log_impl_test.cc",
  ],
  deps = [
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "lru_table_test",
  srcs = [
    "utils/lru_table_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "mem_pool_test",
  srcs = [
    "utils/mem_pool_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "thread_test",
  srcs = [
    "utils/thread_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "time_test",
  srcs = [
    "utils/time_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "traffic_limiter_test",
  srcs = [
    "utils/traffic_limiter_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "uuid_mng_test",
  srcs = [
    "utils/uuid_mng_test.cc",
  ],
  deps = [
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)


### network tests ###

cc_test(
  name = "inet_addr_test",
  srcs = [
    "network/inet_addr_test.cc",
  ],
  deps = [
    ":network",
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "msg_channel_test",
  srcs = [
    "network/msg_channel_test.cc",
  ],
  deps = [
    ":network",
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "msg_header_test",
  srcs = [
    "network/msg_header_test.cc",
  ],
  deps = [
    ":network",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "poller_test",
  srcs = [
    "network/poller_test.cc",
  ],
  deps = [
    ":network",
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "tcp_socket_test",
  srcs = [
    "network/tcp_socket_test.cc",
  ],
  deps = [
    ":network",
    ":utils",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "write_item_list_test",
  srcs = [
    "network/write_item_list_test.cc",
  ],
  deps = [
    ":network",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)


### tiny_rpc tests

cc_test(
  name = "tiny_rpc_test",
  srcs = [
    "tiny_rpc/tiny_rpc_test.cc",
  ],
  deps = [
    ":tiny_rpc",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)


### certain tests

cc_test(
  name = "async_queue_mng_test",
  srcs = [
    "src/async_queue_mng_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "conn_mng_test",
  srcs = [
    "src/conn_mng_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "command_test",
  srcs = [
    "src/command_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "common_test",
  srcs = [
    "src/common_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "conn_worker_test",
  srcs = [
    "src/conn_worker_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "db_limited_worker_test",
  srcs = [
    "src/db_limited_worker_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  copts = cppflags,
)

cc_test(
  name = "db_worker_test",
  srcs = [
    "src/db_worker_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "entity_helper_test",
  srcs = [
    "src/entity_helper_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "entity_info_mng_test",
  srcs = [
    "src/entity_info_mng_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "entity_worker_test",
  srcs = [
    "src/entity_worker_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "entry_info_mng_test",
  srcs = [
    "src/entry_info_mng_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "entry_state_test",
  srcs = [
    "src/entry_state_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "msg_channel_helper_test",
  srcs = [
    "src/msg_channel_helper_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "msg_worker_test",
  srcs = [
    "src/msg_worker_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "msg_serialize_test",
  srcs = [
    "src/msg_serialize_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "notify_helper_test",
  srcs = [
    "src/notify_helper_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "five_replica_test",
  srcs = [
    "src/five_replica_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "libco_notify_helper_test",
  srcs = [
    "src/libco_notify_helper_test.cc",
  ],
  deps = [
    ":certain",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "plog_worker_test",
  srcs = [
    "src/plog_worker_test.cc",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "recover_worker_test",
  srcs = [
    "src/recover_worker_test.cc",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "wrapper_test",
  srcs = [
    "src/wrapper_test.cc",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_test(
  name = "co_lock_test",
  srcs = [
    "utils/co_lock_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  defines = [
    "LEVELDB_COMM"
  ],
  copts = cppflags,
)

cc_test(
  name = "db_impl_test",
  srcs = [
    "default/db_impl_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  copts = cppflags,
)

cc_test(
  name = "plog_impl_test",
  srcs = [
    "default/plog_impl_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  copts = cppflags,
)

cc_test(
  name = "route_impl_test",
  srcs = [
    "default/route_impl_test.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gtest:gtest",
  ],
  copts = cppflags,
)

cc_binary(
  name = "task_perf",
  srcs = [
    "example/task_perf.cc",
    "example/task.cc",
  ],
  defines = [
    "CERTAIN_PROFILE",
    "LEVELDB_COMM",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gperftools:profiler",
  ],
  copts = cppflags,
)

cc_binary(
  name = "db_worker_perf",
  srcs = [
    "src/db_worker_perf.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gflags:gflags",
  ],
  defines = [
    "LEVELDB_COMM"
  ],
  copts = cppflags,
)

cc_binary(
  name = "db_limited_worker_perf",
  srcs = [
    "src/db_limited_worker_perf.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gflags:gflags",
  ],
  defines = [
    "LEVELDB_COMM"
  ],
  copts = cppflags,
)

cc_binary(
  name = "plog_worker_perf",
  srcs = [
    "src/plog_worker_perf.cc",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gflags:gflags",
  ],
  defines = [
    "LEVELDB_COMM"
  ],
  copts = cppflags,
)

cc_binary(
  name = "leveldb_perf",
  srcs = [
    "default/db_perf.cc",
  ],
  defines = [
    "LEVELDB",
  ],
  deps = [
    ":certain",
    ":default",
    "//mm3rd/gflags:gflags",
  ],
  copts = cppflags,
)

# cc_binary(
#   name = "rocksdb_perf",
#   srcs = [
#     "default/db_perf.cc",
#   ],
#   defines = [
#     "ROCKSDB",
#   ],
#   deps = [
#     ":certain",
#     "//platform/raft_log/rocksdb:rocksdb",
#     "//mm3rd/gflags:gflags",
#   ],
#   copts = cppflags,
# )

cc_binary(
  name = "example_client",
  srcs = [
    "example/client.cc",
  ],
  deps = [
    ":certain",
    ":default",
    ":tiny_rpc",
    "//mm3rd/gflags:gflags",
  ],
  copts = cppflags,
)

cc_binary(
  name = "example_server",
  srcs = [
    "example/server.cc",
  ],
  defines = [
    "LEVELDB_COMM",
  ],
  deps = [
    ":certain",
    ":default",
    ":tools",
    "//mm3rd/gflags:gflags",
  ],
  copts = cppflags,
)

cc_binary(
  name = "dump_entry",
  srcs = [
    "tools/dump_entry.cc",
  ],
  deps = [
    ":certain",
    ":default",
    ":tools",
    "//mm3rd/gflags:gflags",
  ],
  copts = cppflags,
)
