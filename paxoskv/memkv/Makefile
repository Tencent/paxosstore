
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG

TESTS = 

INCLS += -I/usr/local/include/
INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
INCLS += -I..
INCLS += -I../libco
LINKS += -L/usr/local/lib/
LINKS += -lpthread -lprotobuf -lleveldb -lsnappy

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

#PROTOC = /Users/test/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o test/*.o libpaxoskv_memkv.a memkv_example

libpaxoskv_memkv.a: memcomm.o memidx.o memsize_mng.o mem_compressor.o \
	datablock.o datablock_mgr.o memkv.o mem_assert.o memloader.o \
	merge_comm.o pmergetor.o
	$(ARSTATICLIB)

memkv_example: memkv_example.o libpaxoskv_memkv.a \
	../core/libpaxoskv_core.a ../dbcomm/libpaxoskv_dbcomm.a \
	../comm/libpaxoskv_comm.a ../libco/lib/libcolib.a
	$(BUILDEXE)


%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

