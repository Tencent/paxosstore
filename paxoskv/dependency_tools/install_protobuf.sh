#!/bin/sh

rm -rf protobuf
mkdir protobuf
cd protobuf
wget https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz -O protobuf-2.6.1.tar.gz
tar zxvf protobuf-2.6.1.tar.gz
cd protobuf-2.6.1

./configure
make -j3
make check
sudo make install
sudo ldconfig
