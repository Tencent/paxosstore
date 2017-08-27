cmake_minimum_required(VERSION 2.8)

project(paxoskv_core)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11")

set(
    SOURCES
    pins_wrapper.cc
    plog_wrapper.cc
    plog_helper.cc
)



FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS paxos.proto)


#include_directories(../)

include_directories("${PROJECT_BINARY_DIR}")
#include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(paxoskv_core STATIC ${SOURCES} ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(paxoskv_core ${PROTOBUF_LIBRARIES} pthread)

 
add_subdirectory(test) 
