
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG

TESTS = 

INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
INCLS += -I../
LINKS += -L/usr/local/lib/
LINKS += -lpthread -lprotobuf -lsnappy -lleveldb

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

#PROTOC = /Users/test/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o test/*.o membase_paxoskv


membase_paxoskv: membase_paxoskv.o \
	../core/libpaxoskv_core.a ../msg_svr/libpaxoskv_msgsvr.a \
	../libco/lib/libcolib.a ../kv/libpaxoskv.a ../dbcomm/libpaxoskv_dbcomm.a \
	../comm/libpaxoskv_comm.a ../memkv/libpaxoskv_memkv.a
	$(BUILDEXE)

%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

