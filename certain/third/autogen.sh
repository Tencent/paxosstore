cd third;

# download leveldb(https://github.com/google/leveldb/tree/v1.18), move to third/leveldb-1.18 and make,
wget https://github.com/google/protobuf/archive/v3.1.0.zip;
unzip v3.1.0.zip;
cd protobuf-3.1.0; make -j 4; cd ..;

# download protobuf(https://github.com/google/protobuf/tree/v3.1.0), move to third/protobuf-3.1.0 and make,
wget https://github.com/google/leveldb/archive/v1.18.zip
unzip v1.18.zip;
cd leveldb-1.18; make -j 4; cd ..;

# download libco(https://github.com/Tencent/libco), move to third/libco-master and make,
wget https://github.com/Tencent/libco/archive/master.zip
unzip master.zip;
cd libco-master; make -j 4 colib; cd ..;

cd ..;
