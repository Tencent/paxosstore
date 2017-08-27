cmake_minimum_required(VERSION 2.8)

project(paxoskv)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11 -DTEST_DEBUG")

set(
    SOURCES
    db_helper.cc
    db_impl.cc
    hard_memkv.cc
)



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

add_library(paxoskv STATIC ${SOURCES})
target_link_libraries(paxoskv 
    ${PROTOBUF_LIBRARIES} ${LEVELDB_LIBRARIES} pthread
    paxoskv_memkv paxoskv_core paxoskv_dbcomm
    paxoskv_comm colib dl ${SNAPPY_LIBRARIES})

message(STATUS "snappy: ${SNAPPY_INCLUDE_DIRS} ${SNAPPY_LIBRARIES}")

add_executable(hmemkv_example hmemkv_example.cc)
target_link_libraries(hmemkv_example paxoskv)
