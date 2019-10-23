pushd third;

if test ! -e protobuf/src/.libs/libprotobuf.a; then
    pushd protobuf; sh ./autogen.sh; ./configure; make -j 4; popd;
fi

if test ! -e rocksdb/librocksdb.a; then
    pushd rocksdb; make -j 4 static_lib; popd;
fi

if test ! -e libco/lib/libcolib.a; then
    pushd libco; make -j 4 colib; popd;
fi

if test ! -e googletest/googletest/libgtest.a; then
    pushd googletest/googletest; cmake .; make -j 4; popd;
fi

if test ! -e grpc/libs/opt/libgrpc.a; then
    pushd grpc; cmake .; make -j 4; popd;
fi

popd;
