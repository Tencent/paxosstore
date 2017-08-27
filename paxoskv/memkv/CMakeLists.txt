cmake_minimum_required(VERSION 2.8)

project(paxoskv_memkv)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11")

set(
    SOURCES
    memcomm.cc
    memidx.cc
    memsize_mng.cc
    mem_compressor.cc
    datablock.cc
    datablock_mgr.cc
    mem_assert.cc
    memkv.cc
    memloader.cc
    merge_comm.cc
    pmergetor.cc
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

add_library(paxoskv_memkv STATIC ${SOURCES})
target_link_libraries(paxoskv_memkv 
    ${PROTOBUF_LIBRARIES} ${LEVELDB_LIBRARIES} pthread
    paxoskv_core paxoskv_dbcomm paxoskv_comm colib dl
    ${SNAPPY_LIBRARIES})

message(STATUS "snappy: ${SNAPPY_INCLUDE_DIRS} ${SNAPPY_LIBRARIES}")

add_executable(memkv_example memkv_example.cc)
target_link_libraries(memkv_example paxoskv_memkv)

