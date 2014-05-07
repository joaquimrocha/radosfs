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

#ifndef RADOS_FS_DIR_IMPL_HH
#define RADOS_FS_DIR_IMPL_HH

#include <tr1/memory>
#include <rados/librados.h>

#include "radosfsdefines.h"
#include "DirCache.hh"
#include "RadosFsFinder.hh"

RADOS_FS_BEGIN_NAMESPACE

class RadosFsDir;

class RadosFsDirPriv
{
public:
  RadosFsDirPriv(RadosFsDir *dirObj);
  RadosFsDirPriv(RadosFsDir *dirObj, bool cacheable);

  virtual ~RadosFsDirPriv();

  int makeDirsRecursively(rados_ioctx_t &ioctx,
                          const char *path,
                          uid_t uid,
                          gid_t gid);

  void updatePath(void);

  bool updateDirInfoPtr(void);

  int updateIoctx(void);

  void updateFsDirCache();

  int find(std::set<std::string> &entries,
           std::set<std::string> &results,
           const std::map<RadosFsFinder::FindOptions, FinderArg> &args);

  RadosFsDir *dir;
  struct stat statBuff;
  std::string parentDir;
  std::tr1::shared_ptr<DirCache> dirInfo;
  rados_ioctx_t ioctx;
  bool cacheable;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_DIR_IMPL_HH */
