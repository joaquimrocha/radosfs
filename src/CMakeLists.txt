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

find_package( LibRados REQUIRED )
find_package( Threads REQUIRED )
find_package( Uuid REQUIRED )
find_package( Boost REQUIRED COMPONENTS thread chrono )

set( CONF_DIR "/etc/${PROJECT_NAME}" )
set( LOG_LEVEL_FILE_BASE_NAME loglevel )
set( LOG_LEVEL_FILE "${CONF_DIR}/${LOG_LEVEL_FILE_BASE_NAME}" )

configure_file(
  "${PROJECT_SOURCE_DIR}/src/radosfsdefines.h.in"
  "${PROJECT_SOURCE_DIR}/src/radosfsdefines.h"
)

add_custom_command(
  OUTPUT ${LOG_LEVEL_FILE_BASE_NAME}
  COMMAND echo DEBUG > ${LOG_LEVEL_FILE_BASE_NAME}
  )
add_custom_target(${LOG_LEVEL_FILE_BASE_NAME}_target
  ALL DEPENDS ${LOG_LEVEL_FILE_BASE_NAME}
)

add_library( radosfs SHARED
             radosfscommon.cc radosfscommon.h
             Filesystem.cc Filesystem.hh FilesystemPriv.hh
             DirCache.cc DirCache.hh
             radosfsdefines.h
             hash64.c hash64.h
             File.cc File.hh FilePriv.hh
             Dir.cc Dir.hh DirPriv.hh
             FsObj.cc FsObj.hh FsObjPriv.hh
             FileIO.cc FileIO.hh
             Logger.cc Logger.hh
             Finder.cc Finder.hh
             AsyncOp.cc AsyncOp.cc AsyncOpPriv.hh
             FileInode.cc FileInode.hh FileInodePriv.hh
             FileInlineBuffer.cc FileInlineBuffer.hh
             Quota.cc Quota.hh QuotaPriv.hh
)

include_directories( ${RADOS_INCLUDE_DIR} ${Boost_INCLUDE_DIRS} )

add_definitions( -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 )

target_link_libraries( radosfs ${RADOS_LIB} ${UUID_LIB} ${Boost_LIBRARIES} )

if( Linux )
  set_target_properties( radosfs PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif( Linux )

install( TARGETS radosfs LIBRARY DESTINATION ${LIB_INSTALL_DIR} )
install( FILES
         Filesystem.hh Dir.hh File.hh FsObj.hh FileInode.hh
         DESTINATION include/radosfs )
install( FILES
         libradosfs.hh
         DESTINATION include )
install( FILES
         ${CMAKE_CURRENT_BINARY_DIR}/${LOG_LEVEL_FILE_BASE_NAME}
         DESTINATION ${CONF_DIR} )
