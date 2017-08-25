
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG -D__MSGSVR_USE_LIBCO__

TESTS = 

INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
INCLS += -I../
INCLS += -I../libco
LINKS += -L/usr/local/lib/
LINKS += -lpthread -lprotobuf

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

#PROTOC = /Users/test/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o test/*.o libpaxoskv_msgsvr.a msgsvr_example

libpaxoskv_msgsvr.a: msg_svr.o msg_cli.o \
	msg_comm.o msg_helper.o msg_epoll.o 
	$(ARSTATICLIB)

msgsvr_example: msgsvr_example.o libpaxoskv_msgsvr.a ../core/libpaxoskv_core.a ../libco/lib/libcolib.a
	$(BUILDEXE)
%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

