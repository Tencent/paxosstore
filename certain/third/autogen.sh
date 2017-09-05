cd third;

cd protobuf; sh ./autogen.sh; ./configure; make -j 4; cd ..;

cd leveldb; make -j 4; cd ..;

cd libco; make -j 4 colib; cd ..;

cd ..;
