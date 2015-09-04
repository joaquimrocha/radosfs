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

#ifndef RADOS_FS_OBJ_PRIV_HH
#define RADOS_FS_OBJ_PRIV_HH

#include <tr1/memory>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

class Filesystem;
class FsObj;

class FsObjPriv
{
public:
  FsObjPriv(FsObj *obj, Filesystem *fs, const std::string &path);
  ~FsObjPriv();

  void setPath(const std::string &path);

  int makeLink(std::string &path);

  int makeRealPath(std::string &path);

  FilesystemPriv * radosFsPriv(void) const { return radosFs->mPriv; }

  std::string path;
  Filesystem *radosFs;
  FsObj *fsObj;
  std::string target;
  Stat stat;
  Stat parentDirStat;
  bool exists;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_OBJ_PRIV_HH */
