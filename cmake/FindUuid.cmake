FIND_PATH(UUID_INCLUDE_DIR uuid.h
  HINTS
  /usr
  PATH_SUFFIXES include/uuid
  PATH_SUFFIXES include/
  PATHS /opt/uuid
)

FIND_LIBRARY(UUID_LIB uuid
  HINTS
  /usr
  /opt/local/
  PATH_SUFFIXES lib
  lib64
  lib/
  lib64/
  lib/uuid/
  lib64/uuid/
)

# GET_FILENAME_COMPONENT( XROOTD_LIB_DIR ${XROOTD_UTILS} PATH )

INCLUDE( FindPackageHandleStandardArgs )
FIND_PACKAGE_HANDLE_STANDARD_ARGS( Uuid DEFAULT_MSG
                                        UUID_LIB
                                        UUID_INCLUDE_DIR )
