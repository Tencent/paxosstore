cmake_minimum_required(VERSION 2.8)

project(paxoskv)

# Use static library
#set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX})

include_directories("${CMAKE_SOURCE_DIR}")
include_directories("${PROJECT_BINARY_DIR}")
include_directories("${PROJECT_BINARY_DIR}/core")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules")

message(STATUS "root: ${PROJECT_BINARY_DIR}")
message(STATUS "source dir: ${CMAKE_SOURCE_DIR}")
message(STATUS "module path: ${CMAKE_MODULE_PATH}")
message(STATUS "library suffixes: ${CMAKE_STATIC_LIBRARY_SUFFIX}")

FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})

FIND_PACKAGE(LevelDB REQUIRED)
include_directories(${LEVELDB_INCLUDE_DIR})

FIND_PACKAGE(Snappy REQUIRED)
include_directories(${SNAPPY_INCLUDE_DIRS})

message(STATUS "Protobuf: ${PROTOBUF_INCLUDE_DIR} ${PROTOBUF_LIBRARIES}")
message(STATUS "LevelDB: ${LEVELDB_INCLUDE_DIR} ${LEVELDB_LIBRARIES}")
message(STATUS "Snappy: ${SNAPPY_INCLUDE_DIRS} ${SNAPPY_LIBRARIES} ")

include_directories("${CMAKE_SOURCE_DIR}/libco")
link_directories("${PROJECT_BINARY_DIR}/libco")
include_directories("${CMAKE_SOURCE_DIR}/googletest/googletest/include")
link_directories("${PROJECT_BINARY_DIR}/googletest/googlemock/gtest")

add_subdirectory(googletest)
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


