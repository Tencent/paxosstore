#-----------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to switch between compilation modes.

# The simple example contains request/response packet handle,
# only for simple_cli to call simple_srv.
# You should probably have your another RPC frame and replace it.
CERTAIN_SIMPLE_EXAMPLE = 1
CERTAIN_DEBUG = 0

PROTOBUF_PATH = ./third/protobuf/src
COLIB_PATH = ./third/libco
LEVELDB_PATH = ./third/leveldb
GTEST_PATH = ./third/googletest/googletest

GXX = g++

GXXFLAGS = -O0 -g2 \
		   -DCERTAIN_DEBUG=$(CERTAIN_DEBUG) -DCERTAIN_SIMPLE_EXAMPLE=$(CERTAIN_SIMPLE_EXAMPLE) \
		   -Wall -Werror -std=c++11 -I./ -I./src/ -I./include/ \
		   -I$(PROTOBUF_PATH) -I$(COLIB_PATH) -I$(LEVELDB_PATH)/include -I$(GTEST_PATH)/include

BUILDFLAGS = -static-libgcc -static-libstdc++ -std=c++11
DLIBS = -pthread -ldl -lsnappy
SLIBS = $(PROTOBUF_PATH)/.libs/libprotobuf.a $(COLIB_PATH)/lib/libcolib.a $(LEVELDB_PATH)/out-static/libleveldb.a

#-----------------------------------------------

CERTAIN_TOOLS = stress_tool
CERTAIN_EXAMPLE = simple_srv simple_cli simple_st
CERTAIN_LIBS = libcertain.a

CERTAIN_PATH=./
CERTAIN_TOOLS_PATH=$(CERTAIN_PATH)/tools
CERTAIN_EXAMPLE_PATH=$(CERTAIN_PATH)/example

CERTAIN_OBJS = 	src/Certain.pb.o \
				utils/Logger.o \
				utils/AOF.o \
				utils/Thread.o \
				utils/FixSizePool.o \
				utils/OSSReport.o \
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

src/Certain.pb.cc: src/Certain.proto
	$(PROTOBUF_PATH)/protoc --cpp_out=./ src/Certain.proto

%.pb.o: %.pb.cc
	$(GXX) $(GXXFLAGS) -c $^ -o $@

%.o: %.cpp
	$(GXX) $(GXXFLAGS) -c $^ -o $@

BUILD_EXEC=$(GXX) $(BUILDFLAGS) $^ -o $@ $(SLIBS) $(DLIBS)
BUILD_TEST_EXEC=$(GXX) $(BUILDFLAGS) $< -o $@ $(GTEST_PATH)/libgtest.a libcertain.a $(DLIBS)

lib: $(CERTAIN_LIBS)

libcertain.a: $(CERTAIN_OBJS)
	ar cr $@ $^

#-----------------------------------------------
# For example.

CERTAIN_EXAMPLE_OBJS = 	$(CERTAIN_EXAMPLE_PATH)/DBImpl.o \
					  	$(CERTAIN_EXAMPLE_PATH)/PLogImpl.o \
					  	$(CERTAIN_EXAMPLE_PATH)/CertainUserImpl.o \
						$(CERTAIN_EXAMPLE_PATH)/UserWorker.o \
						$(CERTAIN_EXAMPLE_PATH)/SimpleCmd.o \
						$(CERTAIN_EXAMPLE_PATH)/simple/KVEngine.o

simple_srv: $(CERTAIN_EXAMPLE_PATH)/Server.o $(CERTAIN_EXAMPLE_OBJS) libcertain.a
	$(BUILD_EXEC)

simple_cli: $(CERTAIN_EXAMPLE_PATH)/Client.o $(CERTAIN_EXAMPLE_OBJS) libcertain.a
	$(BUILD_EXEC)

simple_st: $(CERTAIN_EXAMPLE_PATH)/StressTest.o $(CERTAIN_EXAMPLE_OBJS) libcertain.a
	$(BUILD_EXEC)

#-----------------------------------------------
# For test.

CERTAIN_TESTS = tests/unit/ArrayTimerTest

tests: $(CERTAIN_TESTS)

%Test: %Test.o libcertain.a
	$(BUILD_TEST_EXEC)

#-----------------------------------------------

clean:
	rm src/*.pb.* src/*.o tools/*.o utils/*.o network/*.o example/*.o example/*/*.o -rf; \
    rm $(CERTAIN_PATH)/tests/unit/*.o -rf; \
    rm $(CERTAIN_TOOLS) $(CERTAIN_EXAMPLE) $(CERTAIN_LIBS) $(CERTAIN_TESTS) -rf;

