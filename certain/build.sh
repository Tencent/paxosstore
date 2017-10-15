#!/bin/sh

if [ $# != 1 ] ; then
    echo "Usage: build.sh [libcertain.a|example]"
    exit
fi

set -e  # exit immediately on error
set -x  # display all commands

git submodule update --init --recursive

if [ $1 = "libcertain.a" ] ; then
    if test ! -e third/protobuf/src/.libs/libprotobuf.a; then
        cd third;
        cd protobuf; sh ./autogen.sh; ./configure; make -j 4; cd ..;
        cd ..;
    fi
    make -j 4 lib;
elif [ $1 = "example" ] ; then
    sh third/autobuild.sh; make -j 4 example;
else
    echo "Usage: build.sh [libcertain.a|example]"
    exit
fi
