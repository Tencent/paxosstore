# This code is released under the
# Apache License Version 2.0 http://www.apache.org/licenses/.
#
# Copyright (c) 2012 Louis Dionne
#
# Find SNAPPY compression library and includes. This module defines:
#   SNAPPY_INCLUDE_DIRS - The directories containing SNAPPY's headers.
#   SNAPPY_LIBRARIES    - A list of SNAPPY's libraries.
#   SNAPPY_FOUND        - Whether SNAPPY was found.
#
# This module can be controlled by setting the following variables:
#   SNAPPY_ROOT - The root directory where to find SNAPPY. If this is not
#                 set, the default paths are searched.

if(NOT SNAPPY_ROOT)
    find_path(
      SNAPPY_INCLUDE_DIRS 
      NAMES snappy.h
      HINTS ${CMAKE_SOURCE_DIR}/snappy
      PATH_SUFFIXES include)
    find_library(
      SNAPPY_LIBRARIES 
      NAMES snappy
      HINTS ${CMAKE_SOURCE_DIR}/snappy
      PATH_SUFFIXES build)
else()
  find_path(SNAPPY_INCLUDE_DIRS NAMES snappy.h NO_DEFAULT_PATH PATHS ${SNAPPY_ROOT})
    find_library(SNAPPY_LIBRARIES NAMES snappy NO_DEFAULT_PATH PATHS ${SNAPPY_ROOT})
endif()

if(SNAPPY_INCLUDE_DIRS AND SNAPPY_LIBRARIES)
    set(SNAPPY_FOUND TRUE)
else()
    set(SNAPPY_FOUND FALSE)
    set(SNAPPY_INCLUDE_DIR)
    set(SNAPPY_LIBRARIES)
endif()

mark_as_advanced(SNAPPY_LIBRARIES SNAPPY_INCLUDE_DIRS)
