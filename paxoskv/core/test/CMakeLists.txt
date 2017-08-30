cmake_minimum_required(VERSION 2.8)

project(paxoskv_core_test)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11")

set(
    SOURCES
    test_helper.cc
)



FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})


include_directories("${PROJECT_BINARY_DIR}")

add_library(paxoskv_core_test STATIC ${SOURCES})
target_link_libraries(paxoskv_core_test paxoskv_core)

# Macro for add example target
macro(add_test_target EXAMPLE_TARGET)
    add_executable("${EXAMPLE_TARGET}_test" "${EXAMPLE_TARGET}_test.cc")
    target_link_libraries("${EXAMPLE_TARGET}_test" paxoskv_core_test paxoskv_core gtest gtest_main)
endmacro(add_test_target)


add_executable(test_pb test_pb.cc)
target_link_libraries(test_pb paxoskv_core_test paxoskv_core)

add_test_target(pins_wrapper)
add_test_target(plog_wrapper)
#add_executable(plog_wrapper_test plog_wrapper_test.cc)
#target_link_libraries(plog_wrapper_test paxoskv_core_test paxoskv_core)
