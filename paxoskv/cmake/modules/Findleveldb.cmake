# Try to find leveldb
# Once done, this will define
#
# LEVELDB_FOUND        -  system has leveldb
# LEVELDB_INCLUDE_DIRS -  leveldb include directories
# LEVELDB_LIBRARIES    -  libraries needed to use leveldb

include(FindPackageHandleStandardArgs)

if (LEVELDB_INCLUDE_DIRS AND LEVELDB_LIBRARIES)
  set(LEVELDB_FIND_QUIETLY TRUE)
else()
  find_path(
    LEVELDB_INCLUDE_DIR
    NAMES leveldb/db.h
    HINTS ${LEVELDB_ROOT_DIR} $ENV{LEVELDB_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    LEVELDB_LIBRARY
    NAMES leveldb
    HINTS ${LEVELDB_ROOT_DIR} $ENV{LEVELDB_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(LEVELDB_INCLUDE_DIRS ${LEVELDB_INCLUDE_DIR})
  set(LEVELDB_LIBRARIES ${LEVELDB_LIBRARY})

  find_package_handle_standard_args(
    leveldb
    DEFAULT_MSG LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR)

  mark_as_advanced(LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR)
endif()
