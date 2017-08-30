cmake_minimum_required(VERSION 2.8)

project(example)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11 -DTEST_DEBUG")


#FIND_PACKAGE(Protobuf REQUIRED)
#include_directories(${PROTOBUF_INCLUDE_DIR})
#
#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../cmake_module")
#FIND_PACKAGE(LevelDB REQUIRED)
#include_directories(${LEVELDB_INCLUDE_DIR})

#FIND_PACKAGE(Snappy REQUIRED)
#include_directories(${SNAPPY_INCLUDE_DIRS})

#include_directories(../cutils)
#include_directories(${CMAKE_CURRENT_BINARY_DIR})
#add_subdirectory(cutils)

add_executable(membase_paxoskv membase_paxoskv.cc)
target_link_libraries(membase_paxoskv paxoskv paxoskv_msgsvr)

