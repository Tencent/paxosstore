
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG

TESTS = 

INCLS += -I/usr/local/include/
INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
INCLS += -I..
#LINKS += -L/Users/test/project/lib
LINKS += -lpthread -lprotobuf

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

#PROTOC = /Users/test/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o test/*.o libpaxoskv_dbcomm.a

libpaxoskv_dbcomm.a: db_comm.o bitcask_log_impl.o bitcask_log.o \
	leveldb_log_impl.o leveldb_log.o hashlock.o mmap_file.o \
	db_compresse.o pipe_alloc.o universal_batch.o stor.o \
	newstorage.o recyclefile.o 
	$(ARSTATICLIB)


%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

