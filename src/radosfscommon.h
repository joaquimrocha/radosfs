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
#include <tr1/memory>
#include <time.h>

#include "hash64.h"
#include "radosfsdefines.h"

struct RadosFsPool {
  std::string name;
  size_t size;
  rados_ioctx_t ioctx;

  RadosFsPool(const std::string &poolName, size_t poolSize, rados_ioctx_t ioctx)
    : name(poolName),
      size(poolSize),
      ioctx(ioctx)
  {}

  ~RadosFsPool(void)
  {
    rados_ioctx_destroy(ioctx);
    ioctx = 0;
  }
};

typedef std::tr1::shared_ptr<RadosFsPool> RadosFsPoolSP;

struct RadosFsStat {
  std::string path;
  std::string translatedPath;
  struct stat statBuff;
  RadosFsPoolSP pool;
  std::map<std::string, std::string> extraData;

  void reset(void)
  {
    struct stat buff;
    path = "";
    translatedPath = "";
    statBuff = buff;
    statBuff.st_uid = NOBODY_UID;
    statBuff.st_gid = NOBODY_UID;
    statBuff.st_mode = 0;
    statBuff.st_ctime = 0;
    pool.reset();
    extraData.clear();
  }
};

typedef struct {
  std::string inode;
  RadosFsPoolSP pool;
} RadosFsInode;

ino_t hash(const char *path);

int genericStat(rados_ioctx_t ioctx, const std::string &object,
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

std::string makePermissionsXAttr(long int mode, uid_t uid, gid_t gid);

std::string getParentDir(const std::string &path, int *pos);

std::string escapeObjName(const std::string &obj);

std::string unescapeObjName(const std::string &obj);

int indexObject(const RadosFsStat *parentStat, const RadosFsStat *stat, char op);

std::string getObjectIndexLine(const std::string &obj, char op);

int indexObjectMetadata(rados_ioctx_t ioctx,
                        const std::string &dirName,
                        const std::string &baseName,
                        std::map<std::string, std::string> &metadata,
                        char op);

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
                  std::string &link,
                  std::string &pool,
                  std::map<std::string, std::string> &extraData);

std::string makeFileStripeName(const std::string &filePath, size_t stripeIndex);

bool nameIsStripe(const std::string &name);

std::string getFileXAttrDirRecord(const RadosFsStat *stat);

bool isDirPath(const std::string &path);

std::string generateInode(void);

int createDirAndInode(const RadosFsStat *stat);

int createDirObject(const RadosFsStat *stat);

int getInodeAndPool(rados_ioctx_t ioctx, const std::string &path,
                    std::string &inode, std::string &pool);

std::map<std::string, std::string> stringAttrsToMap(const std::string &attrs);

std::string timespecToStr(const timespec *spec);

void strToTimespec(const std::string &specStr, timespec *spec);

std::string getCurrentTimeStr(void);

void updateDirTimeAsync(const RadosFsStat *stat, const char *timeXAttrKey);

int getTimeFromXAttr(const RadosFsStat *stat, const std::string &xattr,
                     timespec *spec, time_t *basicTime);

bool hasTMTimeEnabled(mode_t mode);

#endif /* __RADOS_FS_COMMON_HH__ */
