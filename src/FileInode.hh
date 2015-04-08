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

#ifndef RADOS_FS_INODE_FILE_HH
#define RADOS_FS_INODE_FILE_HH

#include <boost/scoped_ptr.hpp>

#include "Filesystem.hh"

RADOS_FS_BEGIN_NAMESPACE

class FileInodePriv;
class FilePriv;

class FileInode
{
public:
  FileInode(Filesystem *fs, const std::string &pool);
  FileInode(Filesystem *fs, const std::string &pool, const std::string &name);
  FileInode(Filesystem *fs, const std::string &pool, const std::string &name,
            const size_t stripeSize);
  FileInode(Filesystem *fs, const std::string &pool, const size_t stripeSize);
  virtual ~FileInode(void);

  ssize_t read(char *buff, off_t offset, size_t blen);

  int read(const std::vector<FileReadData> &intervals,
           std::string *asyncOpId = 0, AsyncOpCallback callback = 0,
           void *callbackArg = 0);

  int write(const char *buff, off_t offset, size_t blen, bool copyBuffer = false,
            std::string *asyncOpId = 0, AsyncOpCallback callback = 0,
            void *callbackArg = 0);

  int writeSync(const char *buff, off_t offset, size_t blen);

  int remove(void);

  int truncate(size_t size);

  int sync(const std::string &opId="");

  std::string name(void) const;

  int registerFile(const std::string &path, uid_t uid, gid_t gid, int mode=-1);

  int getBackLink(std::string *backLink);

private:
  FileInode(FileInodePriv *priv);
  FileInodePriv *mPriv;

  friend class FilePriv;
  friend class ::RadosFsTest;
};

RADOS_FS_END_NAMESPACE


#endif // RADOS_FS_INODE_FILE_HH
