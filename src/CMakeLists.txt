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

add_library( radosfs SHARED
             radosfscommon.cc radosfscommon.h
             RadosFs.cc RadosFs.hh RadosFsPriv.hh
             DirCache.cc DirCache.hh
             radosfsdefines.h
             hash64.c hash64.h
             RadosFsFile.cc RadosFsFile.hh RadosFsFilePriv.hh
             RadosFsDir.cc RadosFsDir.hh RadosFsDirPriv.hh
             RadosFsInfo.cc RadosFsInfo.hh RadosFsInfoPriv.hh
             RadosFsIO.cc RadosFsIO.hh
             RadosFsLogger.cc RadosFsLogger.hh
)

include_directories( ${RADOS_INCLUDE_DIR} )

add_definitions( -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 )

target_link_libraries( radosfs ${RADOS_LIB} )

if( Linux )
  set_target_properties( radosfs PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif( Linux )

install( TARGETS radosfs LIBRARY DESTINATION ${LIB_INSTALL_DIR} )
install( FILES
         RadosFs.hh RadosFsDir.hh RadosFsFile.hh RadosFsInfo.hh
         DESTINATION include/radosfs )
install( FILES
         libradosfs.hh
         DESTINATION include )
