/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2014 CERN, Switzerland
 *
 * Author: Joaquim Rocha <joaquim.rocha@cern.ch>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License at http://www.gnu.org/licenses/lgpl-3.0.txt
 * for more details.
 */

#ifndef __RADOS_FS_DEFINES_HH__
#define __RADOS_FS_DEFINES_HH__

#define RADOS_FS_BEGIN_NAMESPACE namespace radosfs {
#define RADOS_FS_END_NAMESPACE }

#define ROOT_UID 0
#define PATH_SEP '/'
#define XATTR_SYS_PREFIX "sys."
#define XATTR_USER_PREFIX "usr."
#define XATTR_UID "uid="
#define XATTR_GID "gid="
#define XATTR_MODE "mode="
#define XATTR_PERMISSIONS_LENGTH 50
#define XATTR_PERMISSIONS XATTR_SYS_PREFIX "permissions"
#define DEFAULT_MODE (S_IRWXU | S_IRGRP | S_IROTH)
#define DEFAULT_MODE_FILE (S_IFREG | DEFAULT_MODE)
#define DEFAULT_MODE_DIR (S_IFDIR | DEFAULT_MODE)
#define INDEX_NAME_KEY "name"
#define MEGABYTE_CONVERSION (1024 * 1024) // 1MB
#define DEFAULT_DIR_CACHE_MAX_SIZE 1000000
#define DEFAULT_DIR_CACHE_CLEAN_PERCENTAGE .2
#define DIR_LOG_UPDATED "updated"
#define DIR_LOG_UPDATED_FALSE "false"
#define DIR_LOG_UPDATED_TRUE "true"
#define DEFAULT_DIR_COMPACT_RATIO .2
#define INDEX_METADATA_PREFIX "md"

#endif /* __RADOS_FS_DEFINES_HH__ */
