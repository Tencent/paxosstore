COMM_MAKE = 1
COMM_ECHO = 1

v=release

CFLAGS += -fno-strict-aliasing -O2 -g -Wall -D_PTHREADS -DTIXML_USE_STL \
		  -export-dynamic -Wall -pipe -D_NEW_LIC \
		  -D_GNU_SOURCE -D_REENTRANT -fPIC -Wno-deprecated -g\
		  -rdynamic -static-libgcc -static-libstdc++ -std=c++11 -DTEST_DEBUG #-Dkvlogsvr_CO #--std=c++11#-Werror

BFLAGS += -O2 -g -rdynamic -static-libgcc -static-libstdc++ #-Werror



TESTS = pins_wrapper_test plog_wrapper_test pb_test

INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
INCLS += -I../
INCLS += -I../../cutils/

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)


all: $(TESTS)

clean :
	rm -f $(TESTS) gtest.a gtest_main.a *.o ../*.o ../paxospb/*.o 

pins_wrapper_test: DEPS = gtest gtest_main kvsvr_comm
pins_wrapper_test: pins_wrapper_test.o ../plog_wrapper.o ../pins_wrapper.o \
	../paxos.pb.o test_helper.o
	$(BUILDEXE)

plog_wrapper_test: DEPS = gtest gtest_main kvsvr_comm 
plog_wrapper_test: plog_wrapper_test.o ../plog_wrapper.o ../pins_wrapper.o \
	../paxospb/paxos.pb.o test_helper.o
	$(BUILDEXE)

pb_test: DEPS = gtest gtest_main
pb_test: pb_test.o ../paxospb/paxos.pb.o
	$(BUILDEXE)

%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

