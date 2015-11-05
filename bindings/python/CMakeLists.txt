#
# Rados Filesystem - A filesystem library based in librados
#
# Copyright (C) 2014 CERN, Switzerland
#
# Author: Michal Simon <michal.simon@cern.ch>
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

cmake_minimum_required( VERSION 2.6 )

FIND_PACKAGE( PythonEasy REQUIRED )
include_directories( ${PYTHON_INCLUDE_PATH} )
include_directories( BEFORE ${PROJECT_SOURCE_DIR}/src/ )

find_package( Boost COMPONENTS python REQUIRED )
execute_process( COMMAND python -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)"
                 OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
                 OUTPUT_STRIP_TRAILING_WHITESPACE )
                 
file(GLOB radosfs_python_sources "*.cc")

add_library(radosfspy SHARED ${radosfs_python_sources})
target_link_libraries(radosfspy radosfs ${Boost_PYTHON_LIBRARIES} ${PYTHON_LIBRARIES})

install( TARGETS radosfspy LIBRARY DESTINATION ${PYTHON_SITE_PACKAGES} )
install( PROGRAMS radosfs.py DESTINATION ${PYTHON_SITE_PACKAGES} )
