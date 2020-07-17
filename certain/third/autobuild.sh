cd third;

if test ! -e protobuf/src/.libs/libprotobuf.a; then
    cd protobuf; sh ./autogen.sh; ./configure; make -j 4; cd ..;
fi

if test ! -e rocksdb/librocksdb.a; then
    cd rocksdb; make -j 4 static_lib; cd ..;
fi

if test ! -e libco/lib/libcolib.a; then
    cd libco; make -j 4 colib; cd ..;
fi

if test ! -e googletest/googletest/libgtest.a; then
    cd googletest/googletest; cmake .; make -j 4; cd ../..;
fi

if test ! -e gflags/lib/libgflags.a; then
    cd gflags; cmake .; make -j 4; cd ..;
fi

if test ! -e glog/.libs/libglog.a; then
    cd glog; sh ./autogen.sh; ./configure; make -j 4; cd ..;
fi

if test ! -e gperftools/.libs/libtcmalloc_and_profiler.a; then
    cd gperftools; sh ./autogen.sh; ./configure; make -j 4; cd ..;
fi

cd ..;
