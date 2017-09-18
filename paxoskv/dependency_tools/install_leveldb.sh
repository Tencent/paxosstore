#!/bin/sh

set -ex
base_dir=$(cd `dirname $0`;pwd)
cd $(dirname $base_dir)/leveldb
make -j2 && sudo cp out-shared/libleveldb.so* /usr/local/lib/ && sudo cp out-static/libleveldb.a /usr/local/lib/ && sudo cp include/leveldb /usr/local/include/ -r && sudo ldconfig
