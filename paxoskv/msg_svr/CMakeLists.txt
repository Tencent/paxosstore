cmake_minimum_required(VERSION 2.8)

project(paxoskv_msgsvr)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11 -D__MSGSVR_USE_LIBCO__")

set(
    SOURCES
    msg_svr.cc
    msg_cli.cc
    msg_comm.cc
    msg_helper.cc
    msg_epoll.cc
)



#FIND_PACKAGE(Protobuf REQUIRED)
#include_directories(${PROTOBUF_INCLUDE_DIR})
#FIND_PACKAGE(paxoskv_core REQUIRED)

#include_directories(..)
#message(STATUS "project binary ${PROJECT_BINARY_DIR}")
#include_directories("${PROJECT_BINARY_DIR}")
#include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(paxoskv_msgsvr STATIC ${SOURCES})
target_link_libraries(paxoskv_msgsvr pthread paxoskv_core colib)

add_executable(msgsvr_example msgsvr_example.cc)
target_link_libraries(msgsvr_example paxoskv_msgsvr)
