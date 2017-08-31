#!/bin/sh

rm -rf leveldb
mkdir leveldb
cd leveldb
wget https://github.com/google/leveldb/archive/v1.20.tar.gz -O leveldb-1.20.tar.gz
tar zxvf leveldb-1.20.tar.gz
cd leveldb-1.20

make -j2
sudo cp out-shared/libleveldb.so* /usr/local/lib/
sudo cp out-static/libleveldb.a /usr/local/lib/
sudo ldconfig
