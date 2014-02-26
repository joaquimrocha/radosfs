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

#ifndef __RADOS_FS_HH__
#define __RADOS_FS_HH__

#include <map>
#include <string>
#include <sys/types.h>
#include <stdint.h>
#include <vector>

#define RADOS_FS_BEGIN_NAMESPACE namespace radosfs {
#define RADOS_FS_END_NAMESPACE }

RADOS_FS_BEGIN_NAMESPACE

class RadosFsPriv;

class RadosFs
{
public:
  RadosFs();
  ~RadosFs();

  int init(const std::string &userName = "",
           const std::string &configurationFile = "");

  int addPool(const std::string &name, const std::string &prefix, int size = 0);

  int removePool(const std::string &name);

  std::vector<std::string> pools() const;

  std::string poolPrefix(const std::string &pool) const;

  std::string poolFromPrefix(const std::string &prefix) const;

  int poolSize(const std::string &pool) const;

  void setIds(uid_t uid, gid_t gid);

  void getIds(uid_t *uid, gid_t *gid) const;

  uid_t uid(void) const;

  uid_t gid(void) const;

  int statCluster(uint64_t *totalSpaceKb,
                  uint64_t *usedSpaceKb,
                  uint64_t *availableSpaceKb,
                  uint64_t *numberOfObjects);

  int stat(const std::string &path, struct stat *buff);

  std::vector<std::string> allPoolsInCluster(void) const;

  int setXAttr(const std::string &path,
               const std::string &attrName,
               const std::string &value);

  int getXAttr(const std::string &path,
               const std::string &attrName,
               std::string &value,
               size_t length);

  int removeXAttr(const std::string &path, const std::string &attrName);

  int getXAttrsMap(const std::string &path,
                   std::map<std::string, std::string> &map);

  void setDirCacheMaxSize(size_t size);

  size_t dirCacheMaxSize(void) const;

private:
  RadosFsPriv *mPriv;

friend class RadosFsInfoPriv;
friend class RadosFsFilePriv;
friend class RadosFsDirPriv;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_FS_HH__ */
