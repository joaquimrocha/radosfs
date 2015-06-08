#
# Rados Filesystem - A filesystem library based in librados
#
# Copyright (C) 2015 CERN, Switzerland
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

#-------------------------------------------------------------------------------
# 'doc' target
#-------------------------------------------------------------------------------

set( DOXYGEN_MAIN_PAGE ${CMAKE_CURRENT_SOURCE_DIR}/main.md )

set( DOC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/main.md" )

foreach( NAME ${PUBLIC_FILE_NAMES} )
  set( DOC_FILES "${CMAKE_SOURCE_DIR}/src/${NAME}.cc ${DOC_FILES}" )
endforeach()

foreach( NAME ${PUBLIC_HEADERS} )
  set( DOC_FILES "${CMAKE_SOURCE_DIR}/src/${NAME} ${DOC_FILES}" )
endforeach()

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile"
)

ADD_CUSTOM_TARGET( doc COMMAND doxygen ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile )
