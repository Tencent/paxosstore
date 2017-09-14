#!/bin/sh

set -ex
base_dir=$(cd `dirname $0`;pwd)
cd $(dirname $base_dir)/protobuf
./autogen.sh && ./configure && make -j3 && make check && sudo make install && sudo ldconfig
