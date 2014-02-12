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

#ifndef __RADOS_IMPL_HH__
#define __RADOS_IMPL_HH__

#include <map>
#include <vector>
#include <set>
#include <string>
#include <tr1/memory>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "DirCache.hh"
#include "RadosFsIO.hh"

RADOS_FS_BEGIN_NAMESPACE

class RadosFs;

class RadosFsPriv
{
public:
  RadosFsPriv(RadosFs *radosFs);
  ~RadosFsPriv();

  int createCluster(const std::string &userName,
                    const std::string &confFile);

  void setUid(const uid_t uid) { this->uid = uid; }
  void setGid(const gid_t gid) { this->gid = gid; }

  uid_t getUid(void) const { return uid; }
  gid_t getGid(void) const { return gid; }

  int addPool(const std::string &name,
              const std::string &prefix,
              int size);

  int createRootIfNeeded(const RadosFsPool &pool);

  const RadosFsPool * getPoolFromPath(const std::string &path);

  const std::string getParentDir(const std::string &obj, int *pos);

  int indexObject(rados_ioctx_t &ioctx, const std::string &obj, char op);

  int getIoctxFromPath(const std::string &objectName, rados_ioctx_t *ioctx);

  std::tr1::shared_ptr<DirCache> getDirInfo(const char *path);

  int checkIfPathExists(rados_ioctx_t &ioctx,
                        const char *path,
                        mode_t *filetype);

  std::string getDirPath(const char *path);

  std::tr1::shared_ptr<RadosFsIO> getRadosFsIO(const std::string &path);

  void setRadosFsIO(std::tr1::shared_ptr<RadosFsIO> sharedFsIO);
  void removeRadosFsIO(std::tr1::shared_ptr<RadosFsIO> sharedFsIO);

  rados_t radosCluster;
  static __thread uid_t uid;
  static __thread gid_t gid;
  std::vector<rados_completion_t> completionList;
  std::map<std::string, RadosFsPool> poolMap;
  std::set<std::string> poolPrefixSet;
  std::map<std::string, std::tr1::shared_ptr<DirCache> > dirCache;
  std::map<std::string, std::tr1::weak_ptr<RadosFsIO> > operations;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_IMPL_HH__ */
