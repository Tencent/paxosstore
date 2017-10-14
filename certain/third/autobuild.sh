cd third;

if test ! -e protobuf/src/.libs/libprotobuf.a; then
    cd protobuf; sh ./autogen.sh; ./configure; make -j 4; cd ..;
fi

if test ! -e rocksdb/librocksdb.a; then
    cd rocksdb; make -j 4; cd ..;
fi

if test ! -e libco/lib/libcolib.a; then
    cd libco; make -j 4 colib; cd ..;
fi

if test ! -e googletest/googletest/libgtest.a; then
    cd googletest/googletest; cmake .; make -j 4; cd ../..;
fi

if test ! -e grpc/libs/opt/libgrpc.a; then
    cd grpc; cmake .; make -j 4; cd ../..;
fi

cd ..;
