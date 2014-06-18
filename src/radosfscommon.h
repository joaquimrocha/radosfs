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
#include "radosfsdefines.h"

typedef struct {
  std::string name;
  int size;
  rados_ioctx_t ioctx;
} RadosFsPool;

typedef struct {
  std::string path;
  std::string translatedPath;
  struct stat statBuff;
  rados_ioctx_t ioctx;
} RadosFsStat;

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

std::string makePermissionsXAttr(long int mode, uid_t uid, gid_t gid);

bool checkIfPathExists(rados_ioctx_t &ioctx,
                       const char *path,
                       mode_t *filetype,
                       char **linkTarget = 0);

int getLinkTarget(rados_ioctx_t ioctx,
                  const std::string &path,
                  char **linkTarget);

std::string getParentDir(const std::string &path, int *pos);

std::string escapeObjName(const std::string &obj);

std::string unescapeObjName(const std::string &obj);

int indexObject(const RadosFsStat *stat, char op);

std::string getObjectIndexLine(const std::string &obj, char op);

int indexObjectMetadata(rados_ioctx_t ioctx,
                        const std::string &obj,
                        std::map<std::string, std::string> &metadata,
                        char op);

bool verifyIsOctal(const char *mode);

std::string getDirPath(const std::string &path);

std::string getFilePath(const std::string &path);

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

int splitToken(const std::string &line,
               int startPos,
               std::string &key,
               std::string &value,
               std::string *op = 0);

int writeContentsAtomically(rados_ioctx_t ioctx,
                            const std::string &obj,
                            const std::string &contents,
                            const std::string &xattrKey = "",
                            const std::string &xattrValue = "");

std::string sanitizePath(const std::string &path);

int statFromXAttr(const std::string &path,
                  const std::string &xattrValue,
                  struct stat* buff,
                  std::string &link);


#endif /* __RADOS_FS_COMMON_HH__ */
