#----------------------------------------------------------------------------
# For path.

CERTAIN_PATH=./
CERTAIN_UTILS_PATH=$(CERTAIN_PATH)/utils
CERTAIN_NETWORK_PATH=$(CERTAIN_PATH)/network
CERTAIN_SRC_PATH=$(CERTAIN_PATH)/src
CERTAIN_DEFAULT_PATH=$(CERTAIN_PATH)/default
CERTAIN_TINY_RPC_PATH=$(CERTAIN_PATH)/tiny_rpc
CERTAIN_TOOLS_PATH=$(CERTAIN_PATH)/tools
CERTAIN_EXAMPLE_PATH=$(CERTAIN_PATH)/example

THIRD_PATH = ./third
PROTOBUF_PATH = ./third/protobuf/src
GTEST_PATH = ./third/googletest/googletest
GLOG_PATH = ./third/glog
GFLAGS_PATH = ./third/gflags
LIBCO_PATH = ./third/libco
LEVELDB_PATH = ./third/rocksdb
OSSATTRAPI_PATH = ./third/ossattrapi
GPERFTOOLS_PATH = ./third/gperftools

GXX = g++

GXXFLAGS = -O2 -g2 -DCERTAIN_PROFILE \
           -Wall -Werror -Wno-format -std=c++11 -I./ -I./include/ \
           -I$(PROTOBUF_PATH) -I$(GTEST_PATH)/include -I$(GLOG_PATH)/src -I$(GFLAGS_PATH)/include \
           -I$(LIBCO_PATH) -I$(LEVELDB_PATH)/include -I$(OSSATTRAPI_PATH) -I$(GPERFTOOLS_PATH)/src

ifeq ($(cov), yes)
GXXFLAGS = -fprofile-arcs -ftest-coverage -O0 -g2\
           -Wall -Werror -Wno-format -std=c++11 -I./ -I./include/ \
           -I$(PROTOBUF_PATH) -I$(GTEST_PATH)/include -I$(GLOG_PATH)/src -I$(GFLAGS_PATH)/include \
           -I$(LIBCO_PATH) -I$(LEVELDB_PATH)/include
endif

DLIBS = -pthread -ldl -lz -lunwind
SLIBS = $(GTEST_PATH)/libgtest.a $(PROTOBUF_PATH)/.libs/libprotobuf.a \
				$(GLOG_PATH)/.libs/libglog.a $(GFLAGS_PATH)/lib/libgflags.a \
				$(LEVELDB_PATH)/librocksdb.a $(LIBCO_PATH)/lib/libcolib.a \
				$(GPERFTOOLS_PATH)/.libs/libtcmalloc_and_profiler.a
#$(OSSATTRAPI_PATH)/libnanooss.a

#----------------------------------------------------------------------------
# For objs.

UTILS_OBJS = proto/certain.pb.o \
						 utils/crc32.o \
						 utils/mem_pool.o \
						 utils/thread.o \
						 utils/co_lock.o

NETWORK_OBJS = network/tcp_socket.o \
							 network/poller.o \
							 network/write_item_list.o \
							 network/msg_header.o \
							 network/msg_channel.o

SRC_OBJS = src/conn_mng.o \
					 src/conn_worker.o \
					 src/command.o \
					 src/msg_serialize.o \
					 src/notify_helper.o \
					 src/libco_notify_helper.o \
					 src/async_queue_mng.o \
					 src/entry_state.o \
					 src/msg_channel_helper.o \
					 src/entity_helper.o \
					 src/entity_info_mng.o \
					 src/entry_info_mng.o \
					 src/msg_worker.o \
					 src/entity_worker.o \
					 src/plog_worker.o \
					 src/db_worker.o \
					 src/db_limited_worker.o \
					 src/recover_worker.o \
					 src/catchup_worker.o \
					 src/tools_worker.o \
					 src/wrapper.o \
					 src/certain.o

TINY_RPC_OBJS = proto/tiny_rpc.pb.o \
								tiny_rpc/tiny_rpc.o \
								tiny_rpc/tiny_client.o \
								tiny_rpc/tiny_server.o

TOOLS_OBJS = proto/tools.pb.o \
						 tools/tools_service.o

DEFAULT_OBJS = default/route_impl.o \
							 default/plog_impl.o \
							 default/db_impl.o \
							 default/log_impl.o \
							 default/tiny_service_impl.o

#----------------------------------------------------------------------------
# For pb.

%.pb.cc: %.proto
	$(PROTOBUF_PATH)/protoc --cpp_out=./ $<

# Generate the pb code one by one.
proto/certain.pb.cc: proto/tiny_rpc.pb.cc

%.o: %.cc proto/certain.pb.cc
	$(GXX) $(GXXFLAGS) -c $< -o $@

#----------------------------------------------------------------------------
# For libcertain.
libcertain.a: $(UTILS_OBJS) $(NETWORK_OBJS) $(SRC_OBJS) $(TINY_RPC_OBJS) $(TOOLS_OBJS)
	ar cr $@ $^

CERTAIN_LIB = libcertain.a
lib: $(CERTAIN_LIB)

#----------------------------------------------------------------------------
# For utils_tests.

BUILD_UTILS_TEST_EXEC=$(GXX) $(GXXFLAGS) $< -o $@ $(UTILS_OBJS) $(SLIBS) $(DLIBS)

utils/%_test: utils/%_test.o $(UTILS_OBJS)
	$(BUILD_UTILS_TEST_EXEC)

CERTAIN_UTILS_TESTS = utils/crc32_test \
											utils/lock_free_queue_test \
											utils/light_list_test \
											utils/array_timer_test \
											utils/lru_table_test \
											utils/mem_pool_test \
											utils/thread_test \
											utils/hash_test \
											utils/time_test \
											utils/capacity_limiter_test \
											utils/uuid_mng_test \
											utils/traffic_limiter_test \
											utils/co_lock_test

utils_tests: $(CERTAIN_UTILS_TESTS)

#----------------------------------------------------------------------------
# For network_tests.

BUILD_NETWORK_TEST_EXEC = $(GXX) $(GXXFLAGS) $< -o $@ $(UTILS_OBJS) \
													$(NETWORK_OBJS) $(SLIBS) $(DLIBS)

network/%_test: network/%_test.o $(UTILS_OBJS) $(NETWORK_OBJS)
	$(BUILD_NETWORK_TEST_EXEC)

CERTAIN_NETWORK_TESTS = network/inet_addr_test \
												network/tcp_socket_test \
												network/poller_test \
												network/write_item_list_test \
												network/msg_header_test \
												network/msg_channel_test

network_tests: $(CERTAIN_NETWORK_TESTS)

#----------------------------------------------------------------------------
# For default_tests.

BUILD_DEFAULT_TEST_EXEC = $(GXX) $(GXXFLAGS) $< -o $@ $(DEFAULT_OBJS) \
													$(CERTAIN_LIB)  $(SLIBS) $(DLIBS)

default/%_test: default/%_test.o $(DEFAULT_OBJS) $(CERTAIN_LIB)
	$(BUILD_DEFAULT_TEST_EXEC)

CERTAIN_DEFAULT_TESTS =	default/route_impl_test \
												default/plog_impl_test \
												default/db_impl_test

default_tests: $(CERTAIN_DEFAULT_TESTS)

#----------------------------------------------------------------------------
# For src_tests.

BUILD_SRC_TEST_EXEC = $(GXX) $(GXXFLAGS) $< -o $@ $(DEFAULT_OBJS) \
											$(CERTAIN_LIB) $(SLIBS) $(DLIBS)

src/%_test: src/%_test.o $(DEFAULT_OBJS) $(CERTAIN_LIB)
	$(BUILD_SRC_TEST_EXEC)

CERTAIN_SRC_TESTS = src/conn_mng_test \
										src/conn_worker_test \
										src/command_test \
										src/msg_serialize_test \
										src/notify_helper_test \
										src/libco_notify_helper_test \
										src/async_queue_mng_test \
										src/entry_state_test \
										src/msg_channel_helper_test \
										src/entity_helper_test \
										src/entity_info_mng_test \
										src/entry_info_mng_test \
										src/msg_worker_test \
										src/plog_worker_test \
										src/db_worker_test \
										src/db_limited_worker_test \
										src/recover_worker_test \
										src/wrapper_test \
										src/five_replica_test \
										src/entity_worker_test

src_tests: $(CERTAIN_SRC_TESTS)

#----------------------------------------------------------------------------
# For tiny_rpc_tests.

BUILD_TINY_RPC_TEST_EXEC = $(GXX) $(GXXFLAGS) $< -o $@ $(CERTAIN_LIB) \
													 $(SLIBS) $(DLIBS)

tiny_rpc/%_test: tiny_rpc/%_test.o $(CERTAIN_LIB)
	$(BUILD_TINY_RPC_TEST_EXEC)

CERTAIN_TINY_RPC_TESTS = tiny_rpc/tiny_rpc_test

tiny_rpc_tests: $(CERTAIN_TINY_RPC_TESTS)

#----------------------------------------------------------------------------
# For task_perf.

CERTAIN_TASK_PERF = task_perf

TASK_PERF_OBJS = example/task.o \
								 example/task_perf.o

BUILD_TASK_PERF_EXEC = $(GXX) $(GXXFLAGS) -o $(CERTAIN_TASK_PERF) $(TASK_PERF_OBJS) \
										   $(DEFAULT_OBJS) $(CERTAIN_LIB) $(SLIBS) $(DLIBS)

task_perf: $(TASK_PERF_OBJS) $(DEFAULT_OBJS) $(CERTAIN_LIB)
	$(BUILD_TASK_PERF_EXEC)

#----------------------------------------------------------------------------
# For plog_worker_perf.

CERTAIN_PLOG_WORKER_PERF = plog_worker_perf

PLOG_WORKER_PERF_OBJS = src/plog_worker_perf.o

BUILD_PLOG_WORKER_PERF_EXEC = $(GXX) $(GXXFLAGS) -o $(CERTAIN_PLOG_WORKER_PERF) \
															$(PLOG_WORKER_PERF_OBJS) $(DEFAULT_OBJS) $(CERTAIN_LIB) \
															$(SLIBS) $(DLIBS)

plog_worker_perf: $(PLOG_WORKER_PERF_OBJS) $(DEFAULT_OBJS) $(CERTAIN_LIB)
	$(BUILD_PLOG_WORKER_PERF_EXEC)

#----------------------------------------------------------------------------
# For server.

CERTAIN_SERVER = server

SERVER_OBJS = example/server.o

BUILD_SERVER_EXEC = $(GXX) $(GXXFLAGS) -o $(CERTAIN_SERVER) $(SERVER_OBJS) \
									  $(DEFAULT_OBJS) $(CERTAIN_LIB) $(SLIBS) $(DLIBS)

server: $(SERVER_OBJS) $(DEFAULT_OBJS) $(CERTAIN_LIB)
	$(BUILD_SERVER_EXEC)

#----------------------------------------------------------------------------
# For client.

CERTAIN_CLIENT = client

CLIENT_OBJS = example/client.o

BUILD_CLIENT_EXEC = $(GXX) $(GXXFLAGS) -o $(CERTAIN_CLIENT) $(CLIENT_OBJS) \
									  $(DEFAULT_OBJS) $(CERTAIN_LIB) $(SLIBS) $(DLIBS)

client: $(CLIENT_OBJS) $(DEFAULT_OBJS) $(CERTAIN_LIB)
	$(BUILD_CLIENT_EXEC)

#----------------------------------------------------------------------------

run_utils_tests:
	for t in $(CERTAIN_UTILS_TESTS); do \
			./$$t || exit; \
	done;

run_network_tests:
	for t in $(CERTAIN_NETWORK_TESTS); do \
			./$$t || exit; \
	done;

run_src_tests:
	for t in $(CERTAIN_SRC_TESTS); do \
			./$$t || exit; \
	done;

run_tiny_rpc_tests:
	for t in $(CERTAIN_TINY_RPC_TESTS); do \
			./$$t || exit; \
	done;

run_default_tests:
	for t in $(CERTAIN_DEFAULT_TESTS); do \
			./$$t || exit; \
	done;

CERTAIN_ALL_TESTS = $(CERTAIN_UTILS_TESTS) $(CERTAIN_NETWORK_TESTS) \
									  $(CERTAIN_SRC_TESTS) $(CERTAIN_TINY_RPC_TESTS) \
									  $(CERTAIN_DEFAULT_TESTS)

all_tests: $(CERTAIN_ALL_TESTS)

run_all_tests:
	for t in $(CERTAIN_ALL_TESTS); do \
			./$$t || exit; \
	done;

clean: 
	rm test_cov.info.* test.cov.root */*.gcda */*.gcno -rf; \
  rm $(CERTAIN_UTILS_TESTS) $(CERTAIN_UTILS_PATH)/*.o -rf; \
  rm $(CERTAIN_NETWORK_TESTS) $(CERTAIN_NETWORK_PATH)/*.o -rf; \
  rm $(CERTAIN_SRC_TESTS) $(CERTAIN_SRC_PATH)/*.o -rf; \
  rm $(CERTAIN_DEFAULT_TESTS) $(CERTAIN_DEFAULT_PATH)/*.o -rf; \
  rm $(CERTAIN_TINY_RPC_TESTS) $(CERTAIN_TINY_RPC_PATH)/*.o -rf; \
  rm $(CERTAIN_TOOLS_PATH)/*.o -rf; \
  rm $(CERTAIN_EXAMPLE_PATH)/*.o -rf; \
  rm $(CERTAIN_TASK_PERF) $(CERTAIN_PLOG_WORKER_PERF) -rf; \
  rm $(CERTAIN_SERVER) $(CERTAIN_CLIENT) -rf; \
  rm $(CERTAIN_LIB) ./*.o ./*.gcov ./proto/*.pb.* -rf;

#----------------------------------------------------------------------------
