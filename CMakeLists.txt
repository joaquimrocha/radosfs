#
# Rados Filesystem - A filesystem library based in librados
#
# Copyright (C) 2014 CERN, Switzerland
#
# Author: Joaquim Rocha <joaquim.rocha@cern.ch>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License at http://www.gnu.org/licenses/lgpl-3.0.txt
# for more details.

set( VERSION_MAJOR "0" )
set( VERSION_MINOR "5" )
set( VERSION_PATCH "3")
set( RELEASE       "1")

cmake_minimum_required( VERSION 2.6 )
project( radosfs )

enable_testing()

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/cmake
     ${PROJECT_SOURCE_DIR}/test )

if ( CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT )
  set( CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "Default install path" FORCE )
endif ( CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

if( MacOSX )
  set( LIB_INSTALL_DIR lib )
else( MacOSX )
  set( LIB_INSTALL_DIR lib64 )
endif( MacOSX )

# include GNU path settings
include (GNUInstallDirs)

# This is so we only build the targets for creating the
# tarball and rpm packages (helpful for the build system)
set( NO_SOURCE CACHE BOOL false )

if( NOT NO_SOURCE )
  if ( "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR
       "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" )
    set( WARNING_LEVEL "-Wall -Wno-unused-local-typedefs" )
    set( CMAKE_CXX_FLAGS ${WARNING_LEVEL}
         CACHE STRING "C++ compiler flags." FORCE )
    set( CMAKE_C_FLAGS  ${WARNING_LEVEL}
         CACHE STRING "C compiler flags." FORCE )
  endif()

  add_subdirectory( src )
  add_subdirectory( test )
  add_subdirectory( benchmark )
  add_subdirectory( tools )
endif( NOT NO_SOURCE )

#-------------------------------------------------------------------------------
# 'uninstall' target
#-------------------------------------------------------------------------------

CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

ADD_CUSTOM_TARGET(uninstall
  "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake")

#-------------------------------------------------------------------------------
# 'distcheck' target
#-------------------------------------------------------------------------------

set(SPEC_FILE_IN "pkg/rhel/radosfs.spec.in")
set(SPEC_FILE "pkg/rhel/radosfs.spec")

add_custom_target(spec
  COMMAND cat ${SPEC_FILE_IN} | sed s/__VERSION_MAJOR__/${VERSION_MAJOR}/ |
          sed s/__VERSION_MINOR__/${VERSION_MINOR}/ |
          sed s/__VERSION_PATCH__/${VERSION_PATCH}/ |
          sed s/__RELEASE__/${RELEASE}/ > ${SPEC_FILE}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(distcheck
  COMMAND tar cvzf ${CMAKE_BINARY_DIR}/${CMAKE_PROJECT_NAME}-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${RELEASE}.tar.gz
    --transform "s,^,${CMAKE_PROJECT_NAME}-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${RELEASE}/,"
    `git ls-files` ${SPEC_FILE}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
ADD_DEPENDENCIES(distcheck spec)

#-------------------------------------------------------------------------------
# 'gitdist' target
#-------------------------------------------------------------------------------
add_custom_target(gitdist
    COMMAND git archive --prefix=${CMAKE_PROJECT_NAME}-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${RELEASE}/ HEAD
        | gzip > ${CMAKE_BINARY_DIR}/${CMAKE_PROJECT_NAME}-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${RELEASE}.tar.gz
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

#-------------------------------------------------------------------------------
# 'srpm' target
#-------------------------------------------------------------------------------
ADD_CUSTOM_TARGET(srpm COMMAND rpmbuild -ts ${CMAKE_PROJECT_NAME}-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${RELEASE}.tar.gz)
ADD_DEPENDENCIES(srpm distcheck)

#-------------------------------------------------------------------------------
# 'rpm' target
#-------------------------------------------------------------------------------
ADD_CUSTOM_TARGET(rpm COMMAND rpmbuild -tb ${CMAKE_PROJECT_NAME}-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${RELEASE}.tar.gz)
ADD_DEPENDENCIES(rpm distcheck)

message( STATUS "Installation prefix: ${CMAKE_INSTALL_PREFIX}" )
