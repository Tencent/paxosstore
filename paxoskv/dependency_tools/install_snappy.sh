#!/bin/sh

rm -rf snappy
mkdir snappy
cd snappy
wget https://github.com/google/snappy/archive/1.1.6.tar.gz -O snappy-1.1.6.tar.gz
tar zxvf snappy-1.1.6.tar.gz
cd snappy-1.1.6

mkdir build
cd build && cmake ../ && make -j2
sudo make install
sudo ldconfig
