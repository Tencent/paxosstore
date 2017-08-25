
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG

TESTS = utils_test paxos_instance_test paxos_test paxos_pb_test config_test glog_test

INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
INCLS += -I../
LINKS += -L/usr/local/lib/
#LINKS += -L/Users/test/project/lib
LINKS += -lpthread -lprotobuf -lleveldb -lsnappy

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

#PROTOC = /Users/test/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o libpaxoskv.a hmemkv_example

libpaxoskv.a: db_helper.o  db_impl.o hard_memkv.o db_route.o
	$(ARSTATICLIB)


hmemkv_example: hmemkv_example.o libpaxoskv.a \
	../memkv/libpaxoskv_memkv.a ../core/libpaxoskv_core.a \
	../dbcomm/libpaxoskv_dbcomm.a ../comm/libpaxoskv_comm.a \
	../libco/lib/libcolib.a
	$(BUILDEXE)

%.o:%.cc
	$(CPPCOMPILE)

%.o:%.cpp
	$(CPPCOMPILE)


#.cc.o:
#	$(CPPCOMPILE)

