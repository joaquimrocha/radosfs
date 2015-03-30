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
#include <rados/librados.hpp>
#include <fcntl.h>
#include <map>
#include <string>
#include <sstream>
#include <tr1/memory>
#include <time.h>

#include "hash64.h"
#include "radosfsdefines.h"

struct Pool {
  std::string name;
  size_t size;
  librados::IoCtx ioctx;
  u_int64_t alignment;

  Pool(const std::string &poolName, size_t poolSize,
       librados::IoCtx &ioctx)
    : name(poolName),
      size(poolSize),
      ioctx(ioctx),
      alignment(0)
  {}

  ~Pool(void)
  {}

  void setAlignment(u_int64_t alignment) { this->alignment = alignment; }

  bool hasAlignment(void) { return alignment != 0; }
};

typedef std::tr1::shared_ptr<Pool> PoolSP;

struct Stat {
  std::string path;
  std::string translatedPath;
  struct stat statBuff;
  PoolSP pool;
  std::map<std::string, std::string> extraData;

  void reset(void)
  {
    memset(&statBuff, sizeof (struct stat), 0);
    path = "";
    translatedPath = "";
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
  PoolSP pool;
} Inode;

ino_t hash(const char *path);

int genericStat(librados::IoCtx &ioctx, const std::string &object,
                struct stat* buff);

void genericStatFromAttrs(const std::string &object,
                          const std::string &permXAttr,
                          const std::string &ctimeXAttr,
                          const std::string &mtimeXAttr,
                          u_int64_t psize,
                          time_t pmtime,
                          struct stat* buff);

bool
statBuffHasPermission(const struct stat &buff,
              const uid_t uid,
              const gid_t gid,
              const int permission);

int
getPermissionsXAttr(const std::string &permXAttr,
                    mode_t *mode,
                    uid_t *uid,
                    gid_t *gid);

std::string makePermissionsXAttr(long int mode, uid_t uid, gid_t gid);

std::string getParentDir(const std::string &path, int *pos);

std::string escapeObjName(const std::string &obj);

std::string unescapeObjName(const std::string &obj);

int indexObject(const Stat *parentStat, const Stat *stat, char op);

std::string getObjectIndexLine(const std::string &obj, char op);

int indexObjectMetadata(librados::IoCtx &ioctx,
                        const std::string &dirName,
                        const std::string &baseName,
                        std::map<std::string, std::string> &metadata,
                        char op);

std::string getDirPath(const std::string &path);

std::string getFilePath(const std::string &path);

int setXAttrFromPath(Stat &stat, uid_t uid, gid_t gid,
                     const std::string &attrName, const std::string &value);

int getXAttrFromPath(librados::IoCtx &ioctx,
                     const struct stat &statBuff,
                     uid_t uid,
                     gid_t gid,
                     const std::string &path,
                     const std::string &attrName,
                     std::string &value);

int removeXAttrFromPath(librados::IoCtx &ioctx,
                        const struct stat &statBuff,
                        uid_t uid,
                        gid_t gid,
                        const std::string &path,
                        const std::string &attrName);

int getMapOfXAttrFromPath(librados::IoCtx &ioctx,
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

int writeContentsAtomically(librados::IoCtx &ioctx,
              const std::string &obj,
              const std::string &contents,
              const std::map<std::string, librados::bufferlist> *xattrs = 0);

std::string sanitizePath(const std::string &path);

int statFromXAttr(const std::string &path,
                  const std::string &xattrValue,
                  struct stat* buff,
                  std::string &link,
                  std::string &pool,
                  std::map<std::string, std::string> &extraData);

std::string makeFileStripeName(const std::string &filePath, size_t stripeIndex);

bool nameIsStripe(const std::string &name);

bool nameIsInode(const std::string &name);

std::string getBaseInode(const std::string &name);

std::string getFileXAttrDirRecord(const Stat *stat);

bool isDirPath(const std::string &path);

std::string generateUuid(void);

int createDirAndInode(const Stat *stat);

int createDirObject(const Stat *stat);

int getInodeAndPool(librados::IoCtx &ioctx, const std::string &path,
                    std::string &inode, std::string &pool);

std::map<std::string, std::string> stringAttrsToMap(const std::string &attrs);

std::string timespecToStr(const timespec *spec);

void strToTimespec(const std::string &specStr, timespec *spec);

std::string getCurrentTimeStr(void);

void updateTimeAsync(const Stat *stat, const char *timeXAttrKey,
                     const std::string &time = "");

void updateTimeAsyncInOmap(const PoolSP &pool, const std::string &inode,
                           const char *timeXAttrKey,
                           const std::string &time = "");

void updateTimeAsyncInXAttr(const PoolSP &pool, const std::string &inode,
                            const char *timeKey,
                            const std::string &time = "");

int getTimeFromXAttr(const Stat *stat, const std::string &xattr,
                     timespec *spec, time_t *basicTime);

bool hasTMTimeEnabled(mode_t mode);

size_t alignStripeSize(size_t stripeSize, size_t alignment);

int statAndGetXAttrs(librados::IoCtx &ioctx, const std::string &obj,
                     u_int64_t *size, time_t *mtime,
                     std::map<std::string, std::string> &xattrs);

std::string fileSizeToHex(size_t num);

void setInodeBacklinkAsync(PoolSP pool, const std::string &backlink,
                           const std::string &inode, const std::string *compare = 0,
                           rados_callback_t callback = 0, void *arg = 0);

int moveLogicalFile(Stat &oldParent, Stat &newParent,
                    const std::string &oldFilePath,
                    const std::string &newFilePath);

int getFileInodeBackLink(Pool *pool, const std::string &inode,
                         std::string *backLink);

int getDirInodeBackLink(Pool *pool, const std::string &inode,
                        std::string *backLink);

int setFileInodeBackLink(Pool *pool, const std::string &inode,
                         const std::string &backLink);

int setDirInodeBackLink(Pool *pool, const std::string &inode,
                        const std::string &backLink);

std::string makeInodeXattr(const Stat *stat);

#endif /* __RADOS_FS_COMMON_HH__ */
