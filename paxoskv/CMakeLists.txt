cmake_minimum_required(VERSION 2.8)

project(paxoskv)

include_directories("${CMAKE_SOURCE_DIR}")
include_directories("${CMAKE_SOURCE_DIR}/libco")
include_directories("${PROJECT_BINARY_DIR}")
include_directories("${PROJECT_BINARY_DIR}/core")
link_directories("${PROJECT_BINARY_DIR}/libco")
message(STATUS "root: ${PROJECT_BINARY_DIR}")

FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules")
FIND_PACKAGE(LevelDB REQUIRED)
include_directories(${LEVELDB_INCLUDE_DIR})

FIND_PACKAGE(Snappy REQUIRED)
include_directories(${SNAPPY_INCLUDE_DIRS})

message(STATUS "leveldb: ${LEVELDB_LIBRARIES}")

message(STATUS "snappy: ${SNAPPY_INCLUDE_DIRS} ${SNAPPY_LIBRARIES} ")


add_subdirectory(libco)
add_subdirectory(core)
add_subdirectory(msg_svr)
add_subdirectory(comm)
add_subdirectory(dbcomm)
add_subdirectory(memkv)
add_subdirectory(kv)
add_subdirectory(example)
#add_subdirectory(cutils)
#add_subdirectory(ssd)


set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11")


