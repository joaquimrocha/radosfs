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

#ifndef RADOS_FS_FILE_PRIV_HH
#define RADOS_FS_FILE_PRIV_HH

#include <tr1/memory>
#include "radosfsdefines.h"
#include "FileIO.hh"
#include "FileInode.hh"
#include "FileInodePriv.hh"

RADOS_FS_BEGIN_NAMESPACE

class Filesystem;
class File;
class FileInode;

class FilePriv
{
public:
  FilePriv(File *fsFile, File::OpenMode mode);
  ~FilePriv();

  void updatePermissions(void);

  int verifyExistanceAndType(void);

  void updatePath(void);

  static std::string sanitizePath(const std::string &path);

  int removeFile(void);

  Stat *fsStat(void);

  int rename(const std::string &destination);

  void updateDataPool(const std::string &pool);

  void setInode(const size_t stripeSize);

  FileIOSP getFileIO(void) const { return inode->mPriv->io; }

  int create(int mode, uid_t uid, gid_t gid, size_t stripe);

  File *fsFile;
  File *target;
  FileInode *inode;
  PoolSP dataPool;
  PoolSP mtdPool;
  std::string parentDir;
  File::OpenMode permissions;
  File::OpenMode mode;
  size_t inlineBufferSize;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_FILE_PRIV_HH */
