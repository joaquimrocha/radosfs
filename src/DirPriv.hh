/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2014-2015 CERN, Switzerland
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

#ifndef RADOS_FS_DIR_PRIV_HH
#define RADOS_FS_DIR_PRIV_HH

#include <tr1/memory>

#include "radosfsdefines.h"
#include "DirCache.hh"
#include "Finder.hh"
#include "Quota.hh"
#include "QuotaPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

class Dir;

class DirPriv
{
public:
  DirPriv(Dir *dirObj);
  DirPriv(Dir *dirObj, bool cacheable);

  virtual ~DirPriv();

  int makeDirsRecursively(Stat *buff,
                          const char *path,
                          uid_t uid,
                          gid_t gid);

  void updatePath(void);

  bool updateDirInfoPtr(void);

  const PoolSP getPool(void);

  Stat *fsStat(void) const;

  void updateFsDirCache();

  int find(std::set<std::string> &entries,
           std::set<std::string> &results,
           const std::map<Finder::FindOptions, FinderArg> &args);

  FilesystemPriv *radosFsPriv(void);

  int rename(const std::string &newName);

  int moveDirTreeObjects(const Stat *oldDir, const Stat *newDir);

  std::string getQuotaName(void) const;

  int getQuotasXAttr(std::string &quotas);

  int addQuotaObject(const Quota &obj, bool applyRecursively);

  int removeQuotaObject(const Quota &obj, bool applyRecursively);

  QuotaPriv * getQuotaPriv(const Quota &quota) const { return quota.mPriv.get(); }

  Dir *dir;
  Dir *target;
  std::string parentDir;
  std::tr1::shared_ptr<DirCache> dirInfo;
  bool cacheable;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_DIR_PRIV_HH */
