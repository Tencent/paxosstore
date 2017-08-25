cmake_minimum_required(VERSION 2.8)

project(paxoskv_comm)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11")

set(
    SOURCES
    conhash.cc
    hash_header.cc
    kvsvrcomm.cpp
    iconfig_hash.cpp
    iconfig_impl.cpp
    iconfig.cpp
    svrlist_config_base.cpp
    svrlist_config.cpp
    kvclient_route.cpp
    svr_route.cpp
    id_calculator.cpp
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

add_library(paxoskv_comm STATIC ${SOURCES})
target_link_libraries(paxoskv_comm pthread)

