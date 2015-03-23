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
find_package( Boost REQUIRED COMPONENTS thread chrono )

include_directories( ${PROJECT_SOURCE_DIR}/src ${RADOS_INCLUDE_DIR} ${Boost_INCLUDE_DIRS} )

add_executable( radosfsck radosfsck.cc RadosFsChecker.cc RadosFsChecker.hh)
target_link_libraries( radosfsck ${RADOS_LIB} radosfs ${Boost_LIBRARIES} )

install( TARGETS radosfsck RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR} )
