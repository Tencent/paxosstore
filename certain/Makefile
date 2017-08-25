#-----------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to switch between compilation modes.

MAKE_FOR_KVSVR = 0

# (A) Production use (optimized mode)
# OPT = -O2 -DCERTAIN_DEBUG=0
# (B) Debug mode, w/ full line-level debugging symbols
# OPT = -O0 -g2 -DCERTAIN_DEBUG=1
# (C) Debug mode, w/ full line-level debugging symbols
 OPT = -O0 -g2 -DCERTAIN_DEBUG=0 -DCERTAIN_MAKE_FOR_KVSVR=$(MAKE_FOR_KVSVR)

PROTOBUF_PATH = ./third/protobuf-3.1.0/src
COLIB_PATH = ./third/libco-master
LEVELDB_PATH = ./third/leveldb-1.18

GXX = g++
GXXFLAGS = $(OPT) -Wall -Werror -std=c++11 -I./ -I./include/ -I$(PROTOBUF_PATH) -I$(COLIB_PATH) -I$(LEVELDB_PATH)/include
BUILDFLAGS = -static-libgcc -static-libstdc++ -std=c++11
DLIBS = -lpthread -ldl
SLIBS = $(PROTOBUF_PATH)/.libs/libprotobuf.a $(COLIB_PATH)/lib/libcolib.a $(LEVELDB_PATH)/libleveldb.a

#-----------------------------------------------

SRV_PROG = certain_simple
CLI_PROG = certain_cli
TOOLS_PROG = stress_tool certain_cli
EXAMPLE_PROG = test_PLogImpl test_DBImpl certain_simple

CERTAIN_PATH=./
CERTAIN_TOOLS_PATH=$(CERTAIN_PATH)/tools
CERTAIN_EXAMPLE_PATH=$(CERTAIN_PATH)/example

CERTAIN_OBJS = 	Certain.pb.o \
				utils/Logger.o \
				utils/AOF.o \
				utils/Thread.o \
				utils/FixSizePool.o \
				utils/OSSReport.o \
				network/TCPSocket.o \
				network/EpollIO.o \
				network/IOChannel.o \
				AsyncQueueMng.o \
				Configure.o \
				PerfLog.o \
				Command.o \
				EntryState.o \
				EntityInfoMng.o \
				EntryInfoMng.o \
				IOWorker.o \
				ConnWorker.o \
				EntityWorker.o \
				PLogWorker.o \
				DBWorker.o \
				CatchUpWorker.o \
				AsyncPipeMng.o \
				UUIDGroupMng.o \
				CertainWrapper.o \
				GetAllWorker.o \
                UserWorker.o

default: $(SRV_PROG) $(CLI_PROG)
tools: $(TOOLS_PROG)

Certain.pb.cc: Certain.proto
	$(PROTOBUF_PATH)/protoc --cpp_out=. Certain.proto

%.pb.o: %.pb.cc
	$(GXX) $(GXXFLAGS) -c $^ -o $@

%.o: %.cpp
	$(GXX) $(GXXFLAGS) -c $^ -o $@

BUILD_EXEC=$(GXX) $(BUILDFLAGS) $(DLIBS) $^ -o $@ $(SLIBS)

certain: $(CERTAIN_OBJS)
	$(BUILD_EXEC)

#-----------------------------------------------
# The following is for tools.

certain_cli: $(CERTAIN_OBJS) $(CERTAIN_TOOLS_PATH)/CertainClient.o
	$(BUILD_EXEC)

stress_tool: $(CERTAIN_OBJS) $(CERTAIN_TOOLS_PATH)/StressTool.o
	$(BUILD_EXEC)

#-----------------------------------------------
# For example.

CERTAIN_EXAMPLE_OBJS = 	$(CERTAIN_EXAMPLE_PATH)/DBImpl.o \
					  	$(CERTAIN_EXAMPLE_PATH)/PLogImpl.o \
					  	$(CERTAIN_EXAMPLE_PATH)/CertainUserImpl.o \
						$(CERTAIN_EXAMPLE_PATH)/simple/KVEngine.o

certain_simple: $(CERTAIN_OBJS) $(CERTAIN_EXAMPLE_PATH)/Main.o $(CERTAIN_EXAMPLE_OBJS)
	$(BUILD_EXEC)

test_PLogImpl: $(CERTAIN_OBJS) $(CERTAIN_EXAMPLE_PATH)/test_PLogImpl.o $(CERTAIN_EXAMPLE_OBJS)
	$(BUILD_EXEC)

test_DBImpl: $(CERTAIN_OBJS) $(CERTAIN_EXAMPLE_PATH)/test_DBImpl.o $(CERTAIN_EXAMPLE_OBJS)
	$(BUILD_EXEC)

#-----------------------------------------------

clean:
	rm *.pb.* *.o tools/*.o utils/*.o network/*.o example/*/*.o $(SRV_PROG) $(CLI_PROG) $(TOOLS_PROG) $(EXAMPLE_PROG) -rf
