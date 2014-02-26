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

#ifndef __RADOS_FS_COMMON_HH__
#define __RADOS_FS_COMMON_HH__

#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <rados/librados.h>
#include <fcntl.h>
#include <map>
#include <string>
#include <sstream>

#include "hash64.h"

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
#define INDEX_NAME_KEY "name="
#define MEGABYTE_CONVERSION (1024 * 1024) // 1MB
#define DEFAULT_DIR_CACHE_MAX_SIZE 1000000
#define DEFAULT_DIR_CACHE_CLEAN_PERCENTAGE .2

typedef struct {
  std::string name;
  int size;
  rados_ioctx_t ioctx;
} RadosFsPool;

static ino_t
hash(const char *path)
{
  return hash64((ub1 *) path, strlen(path), 0);
}

int genericStat(rados_ioctx_t &ioctx,
                const char* path,
                struct stat* buff);

bool
statBuffHasPermission(const struct stat &buff,
              const uid_t uid,
              const gid_t gid,
              const int permission);

int
getPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    mode_t *mode,
                    uid_t *uid,
                    gid_t *gid);

int
setPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    long int mode,
                    uid_t uid,
                    gid_t gid);

bool checkIfPathExists(rados_ioctx_t &ioctx, const char *path, mode_t *filetype);

std::string getParentDir(const std::string &path, int *pos);

std::string escapeObjName(const std::string &obj);

int indexObject(rados_ioctx_t &ioctx, const std::string &obj, char op);

bool verifyIsOctal(const char *mode);

std::string getDirPath(const char *path);

std::string getRealPath(rados_ioctx_t ioctx, const std::string &path);

int setXAttrFromPath(rados_ioctx_t ioctx,
                     const struct stat &statBuff,
                     uid_t uid,
                     gid_t gid,
                     const std::string &path,
                     const std::string &attrName,
                     const std::string &value);

int getXAttrFromPath(rados_ioctx_t ioctx,
                     const struct stat &statBuff,
                     uid_t uid,
                     gid_t gid,
                     const std::string &path,
                     const std::string &attrName,
                     std::string &value,
                     size_t length);

int removeXAttrFromPath(rados_ioctx_t ioctx,
                        const struct stat &statBuff,
                        uid_t uid,
                        gid_t gid,
                        const std::string &path,
                        const std::string &attrName);

int getMapOfXAttrFromPath(rados_ioctx_t ioctx,
                          const struct stat &statBuff,
                          uid_t uid,
                          gid_t gid,
                          const std::string &path,
                          std::map<std::string, std::string> &map);

#endif /* __RADOS_FS_COMMON_HH__ */
