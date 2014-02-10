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

find_package( GTest REQUIRED )
find_package( LibRados REQUIRED )

include_directories( ${PROJECT_SOURCE_DIR}/src ${GTEST_INCLUDE_DIRS} ${RADOS_INCLUDE_DIR} )

add_executable( runTests tests.cc RadosFsTest.cc RadosFsTest.hh )
target_link_libraries( runTests ${GTEST_LIB} ${RADOS_LIB} radosfs )

add_test(
    NAME runTests
    COMMAND runTests
)
