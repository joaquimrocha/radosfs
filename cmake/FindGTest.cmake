FIND_PATH(GTEST_INCLUDE_DIR gtest.h
  HINTS
  $ENV{XROOTD_DIR}
  /usr
  /opt/gtest/
  PATH_SUFFIXES include/gtest
  PATHS /opt/gtest
)

FIND_LIBRARY(GTEST_LIB libgtest gtest
  HINTS
  /usr
  /opt/gtest/
  PATH_SUFFIXES lib
  lib64
  lib/gtest/
  lib64/gtest/
)

# GET_FILENAME_COMPONENT( XROOTD_LIB_DIR ${XROOTD_UTILS} PATH )

INCLUDE( FindPackageHandleStandardArgs )
FIND_PACKAGE_HANDLE_STANDARD_ARGS( GTest DEFAULT_MSG
                                        GTEST_LIB
                                        GTEST_INCLUDE_DIR )
