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

#ifndef RADOS_FS_FILE_IMPL_HH
#define RADOS_FS_FILE_IMPL_HH

#include <tr1/memory>
#include <rados/librados.h>
#include "radosfsdefines.h"
#include "RadosFsIO.hh"

RADOS_FS_BEGIN_NAMESPACE

class RadosFs;
class RadosFsFile;

class RadosFsFilePriv
{
public:
  RadosFsFilePriv(RadosFsFile *fsFile,
                  RadosFsFile::OpenMode mode);
  ~RadosFsFilePriv();

  void updatePermissions(void);

  int verifyExistanceAndType(void);

  void updatePath(void);

  static std::string sanitizePath(const std::string &path);

  int removeFile(void);

  RadosFsStat *fsStat(void);

  int rename(const std::string &destination);

  void updateDataPool(const std::string &pool);

  RadosFsFile *fsFile;
  RadosFsFile *target;
  RadosFsPoolSP dataPool;
  RadosFsPoolSP mtdPool;
  std::string inode;
  std::string parentDir;
  RadosFsFile::OpenMode permissions;
  RadosFsFile::OpenMode mode;
  std::tr1::shared_ptr<RadosFsIO> radosFsIO;
  std::vector<std::string> asyncOps;
  boost::mutex asyncOpsMutex;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_FILE_IMPL_HH */
