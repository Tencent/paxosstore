#!/bin/bash

make -j 10 all_tests cov=yes;
make run_all_tests;

# Run the following shell after .gcno files generated in each dirs, and at
# least one test run to generate .gcda file. And you can see all files'
# coverage.

lcov -c -i -d ./src -d ./utils -d ./network -d ./tiny_rpc -d ./default -o test_cov.info.base
lcov -c -d ./src -d ./utils -d ./network -d ./tiny_rpc -d ./default -o test_cov.info.0
lcov -a test_cov.info.base -a test_cov.info.0 -o test_cov.info.1
lcov --remove test_cov.info.1 '*/usr/*' '*/third/*' '*/proto/*' -o test_cov.info.2
genhtml  test_cov.info.2 -o test.cov.root
