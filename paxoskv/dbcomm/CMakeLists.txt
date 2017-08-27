cmake_minimum_required(VERSION 2.8)

project(paxoskv_dbcomm)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11 -DTEST_DEBUG")

set(
    SOURCES
    db_comm.cc
    db_compresse.cc
    bitcask_log_impl.cc
    bitcask_log.cc
    leveldb_log_impl.cc
    leveldb_log.cc
    hashlock.cc
    mmap_file.cc
    pipe_alloc.cc
    universal_batch.cc
    stor.cc
    newstorage.cc
)



#FIND_PACKAGE(Protobuf REQUIRED)
#include_directories(${PROTOBUF_INCLUDE_DIR})
#
#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../cmake_module")
#FIND_PACKAGE(LevelDB REQUIRED)
#include_directories(${LEVELDB_INCLUDE_DIR})

#include_directories(../cutils)
#include_directories(${CMAKE_CURRENT_BINARY_DIR})
#add_subdirectory(cutils)

add_library(paxoskv_dbcomm STATIC ${SOURCES})

if (APPLE)
  target_link_libraries(paxoskv_dbcomm ${PROTOBUF_LIBRARIES} ${LEVELDB_LIBRARIES} pthread)
else ()
  target_link_libraries(paxoskv_dbcomm ${PROTOBUF_LIBRARIES} ${LEVELDB_LIBRARIES} pthread rt)
endif (APPLE)

