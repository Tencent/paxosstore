#-----------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to switch between compilation modes.

CERTAIN_DEBUG = 0

CERTAIN_PATH=./
CERTAIN_TOOLS_PATH=$(CERTAIN_PATH)/tools
CERTAIN_EXAMPLE_PATH=$(CERTAIN_PATH)/example
CERTAIN_UTILS_PATH=$(CERTAIN_PATH)/utils

THIRD_PATH = ./third
PROTOBUF_PATH = ./third/protobuf/src
COLIB_PATH = ./third/libco
# ----- For leveldb -----
#DBTYPE_PATH = ./third/leveldb
#DBTYPE_NAME = leveldb
#DB_PLATFORM_TYPE = LEVELDB_PLATFORM_TYPE
#DB_PLATFORM_POSIX = LEVELDB_PLATFORM_POSIX
#DB_STATIC_LIBRARY = $(DBTYPE_PATH)/out-static/libleveldb.a 
# ----- For rocksdb -----
DBTYPE_PATH = ./third/rocksdb
DBTYPE_NAME = rocksdb
DB_PLATFORM_TYPE = ROCKSDB_PLATFORM_TYPE
DB_PLATFORM_POSIX = ROCKSDB_PLATFORM_POSIX
DB_STATIC_LIBRARY = $(DBTYPE_PATH)/librocksdb.a
GTEST_PATH = ./third/googletest/googletest
GRPC_PATH= ./third/grpc

GXX = g++

GXXFLAGS = -O0 -g2 -DCERTAIN_DEBUG=$(CERTAIN_DEBUG) \
		   -Wall -Werror -std=c++11 -I./ -I./src/ -I./include/ \
		   -I$(PROTOBUF_PATH) -I$(COLIB_PATH) -I$(GTEST_PATH)/include \
           -I$(GRPC_PATH)/include -I$(THIRD_PATH) -I$(CERTAIN_EXAMPLE_PATH) \
		   -I$(DBTYPE_PATH) -I$(DBTYPE_PATH)/include -I$(DBTYPE_PATH)/include/$(DBTYPE_NAME) \
		   -D$(DB_PLATFORM_TYPE) -D$(DB_PLATFORM_POSIX)

BUILDFLAGS = -static-libgcc -static-libstdc++ -std=c++11
DLIBS = -pthread -ldl
SLIBS = $(PROTOBUF_PATH)/.libs/libprotobuf.a $(COLIB_PATH)/lib/libcolib.a \
        $(DB_STATIC_LIBRARY) \
        $(GRPC_PATH)/libs/opt/libares.a \
        $(GRPC_PATH)/libs/opt/libboringssl.a \
        $(GRPC_PATH)/libs/opt/libgpr.a \
        $(GRPC_PATH)/libs/opt/libgrpc.a \
        $(GRPC_PATH)/libs/opt/libgrpc++.a \
        $(GRPC_PATH)/libs/opt/libgrpc++_core_stats.a \
        $(GRPC_PATH)/libs/opt/libgrpc++_cronet.a \
        $(GRPC_PATH)/libs/opt/libgrpc_cronet.a \
        $(GRPC_PATH)/libs/opt/libgrpc++_error_details.a \
        $(GRPC_PATH)/libs/opt/libgrpc_plugin_support.a \
        $(GRPC_PATH)/libs/opt/libgrpc_unsecure.a \
        $(GRPC_PATH)/libs/opt/libgrpc++_unsecure.a \
        $(GRPC_PATH)/libs/opt/libz.a \
        -Wl,--no-as-needed $(GRPC_PATH)/libs/opt/libgrpc++_reflection.a -Wl,--as-needed

#-----------------------------------------------

CERTAIN_TOOLS = stress_tool
CERTAIN_EXAMPLE = card_srv card_tool db_tool bench_tool
CERTAIN_LIBS = libcertain.a

CERTAIN_OBJS = 	src/Certain.pb.o \
				utils/Logger.o \
				utils/AOF.o \
				utils/Thread.o \
				utils/FixSizePool.o \
				utils/OSSReport.o \
				utils/CRC32.o \
				utils/UseTimeStat.o \
				network/SocketHelper.o \
				network/EpollIO.o \
				network/IOChannel.o \
				src/AsyncQueueMng.o \
				src/Configure.o \
				src/PerfLog.o \
				src/Command.o \
				src/EntryState.o \
				src/EntityInfoMng.o \
				src/EntryInfoMng.o \
				src/IOWorker.o \
				src/ConnWorker.o \
				src/EntityWorker.o \
				src/PLogWorker.o \
				src/DBWorker.o \
				src/CatchUpWorker.o \
				src/AsyncPipeMng.o \
				src/UUIDGroupMng.o \
				src/CertainWrapper.o \
				src/GetAllWorker.o

default: $(CERTAIN_LIBS)
example: $(CERTAIN_LIBS) $(CERTAIN_EXAMPLE)
tools: $(CERTAIN_TOOLS)

%.grpc.pb.cc: %.proto
	$(PROTOBUF_PATH)/protoc --grpc_out=./ --plugin=protoc-gen-grpc=$(GRPC_PATH)/bins/opt/grpc_cpp_plugin $^

%.pb.cc: %.proto
	$(PROTOBUF_PATH)/protoc --cpp_out=./ $^

%.pb.o: %.pb.cc
	$(GXX) $(GXXFLAGS) -c $^ -o $@

%.o: %.cpp
	$(GXX) $(GXXFLAGS) -c $^ -o $@

%.o: %.cc
	$(GXX) $(GXXFLAGS) -c $^ -o $@

BUILD_EXEC=$(GXX) $(BUILDFLAGS) $^ -o $@ $(SLIBS) $(DLIBS)

lib: $(CERTAIN_LIBS)

libcertain.a: $(CERTAIN_OBJS)
	ar cr $@ $^

#-----------------------------------------------
# For example.

CERTAIN_EXAMPLE_OBJS = 	$(CERTAIN_EXAMPLE_PATH)/example.pb.o \
						$(CERTAIN_EXAMPLE_PATH)/example.grpc.pb.o \
					  	$(CERTAIN_EXAMPLE_PATH)/Client.o \
					  	$(CERTAIN_EXAMPLE_PATH)/Coding.o \
					  	$(CERTAIN_EXAMPLE_PATH)/DBImpl.o \
					  	$(CERTAIN_EXAMPLE_PATH)/PLogImpl.o \
					  	$(CERTAIN_EXAMPLE_PATH)/CertainUserImpl.o \
						$(CERTAIN_EXAMPLE_PATH)/UserWorker.o \
						$(CERTAIN_EXAMPLE_PATH)/ServiceImpl.o \
						$(CERTAIN_EXAMPLE_PATH)/UUIDGenerator.o

example/example.pb.cc: example/example.proto
	$(PROTOBUF_PATH)/protoc --cpp_out=./ example/example.proto
		
card_srv: $(CERTAIN_EXAMPLE_OBJS) example/Server.o libcertain.a
	$(BUILD_EXEC)

card_tool: $(CERTAIN_EXAMPLE_OBJS) example/CardTool.o libcertain.a
	$(BUILD_EXEC)

db_tool: $(CERTAIN_EXAMPLE_OBJS) example/DBTool.o libcertain.a
	$(BUILD_EXEC)

bench_tool: $(CERTAIN_EXAMPLE_OBJS) example/BenchTool.o libcertain.a
	$(BUILD_EXEC)

BUILD_EXAMPLE_TEST_EXEC=$(GXX) $(BUILDFLAGS) $^ -o $@ $(GTEST_PATH)/libgtest.a $(SLIBS) $(DLIBS)

example/tests/%Test: example/tests/%Test.o $(CERTAIN_EXAMPLE_OBJS) libcertain.a
	$(BUILD_EXAMPLE_TEST_EXEC)

CERTAIN_EXAMPLE_TESTS = example/tests/CodingTest \
						example/tests/DBImplTest \
						example/tests/PLogImplTest \
						example/tests/UUIDGeneratorTest

example_tests: $(CERTAIN_EXAMPLE_TESTS)

#-----------------------------------------------
# For utils_test.

utils/tests/%Test: utils/tests/%Test.o libcertain.a
	$(BUILD_UTILS_TEST_EXEC)

BUILD_UTILS_TEST_EXEC=$(GXX) $(BUILDFLAGS) $^ -o $@ $(GTEST_PATH)/libgtest.a $(SLIBS) $(DLIBS)

CERTAIN_UTILS_TESTS = utils/tests/ArrayTimerTest \
					  utils/tests/FixSizePoolTest \
					  utils/tests/CRC32Test

utils_tests: $(CERTAIN_UTILS_TESTS)

#-----------------------------------------------

clean:
	rm src/*.pb.* src/*.o tools/*.o network/*.o example/*.pb.* -rf; \
    rm $(CERTAIN_EXAMPLE_TESTS) $(CERTAIN_EXAMPLE_PATH)/*.o $(CERTAIN_EXAMPLE_PATH)/tests/*.o -rf; \
	rm $(CERTAIN_UTILS_TESTS) $(CERTAIN_UTILS_PATH)/*.o $(CERTAIN_UTILS_PATH)/tests/*.o -rf; \
    rm $(CERTAIN_TOOLS) $(CERTAIN_EXAMPLE) $(CERTAIN_LIBS) -rf;

