FIND_PATH(RADOS_INCLUDE_DIR librados.h
  HINTS
  $ENV{XROOTD_DIR}
  /usr
  /opt/rados/
  PATH_SUFFIXES include/rados
  PATHS /opt/rados
)

FIND_LIBRARY(RADOS_LIB librados rados
  HINTS
  /usr
  /opt/rados/
  PATH_SUFFIXES lib
  lib64
  lib/rados/
  lib64/rados/
)

# GET_FILENAME_COMPONENT( XROOTD_LIB_DIR ${XROOTD_UTILS} PATH )

INCLUDE( FindPackageHandleStandardArgs )
FIND_PACKAGE_HANDLE_STANDARD_ARGS( Ceph DEFAULT_MSG
                                        RADOS_LIB
                                        RADOS_INCLUDE_DIR )
