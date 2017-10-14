#!/bin/sh

set -e  # exit immediately on error
set -x  # display all commands

git submodule update --init --recursive

# For certain/example.
sh third/autobuild.sh; make -j 10 example;
