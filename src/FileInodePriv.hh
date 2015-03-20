/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2015 CERN, Switzerland
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

#ifndef RADOS_FS_FILE_INODE_PRIV_HH
#define RADOS_FS_FILE_INODE_PRIV_HH

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <vector>

#include "FileInodePriv.hh"
#include "FileIO.hh"
#include "Filesystem.hh"

RADOS_FS_BEGIN_NAMESPACE

class FileInodePriv
{
public:
  FileInodePriv(Filesystem *fs, const std::string &name,
                const std::string &pool, const size_t stripeSize);
  FileInodePriv(Filesystem *fs, PoolSP &pool, const std::string &name,
                const size_t stripeSize);
  FileInodePriv(Filesystem *fs, FileIOSP fileIO);
  ~FileInodePriv(void);

  void setFileIO(FileIOSP fileIO);

  int registerFile(const std::string &path, uid_t uid, gid_t gid, int mode,
                   size_t inlineBufferSize=0);

  int setBackLink(const std::string &backLink);

  Filesystem *fs;
  std::string name;
  FileIOSP io;
  boost::mutex asyncOpsMutex;
  std::vector<std::string> asyncOps;
};

RADOS_FS_END_NAMESPACE

#endif // RADOS_FS_FILE_INODE_PRIV_HH
