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

#ifndef RADOS_FS_FILE_HH
#define RADOS_FS_FILE_HH

#include <boost/scoped_ptr.hpp>
#include <cstdlib>

#include "Filesystem.hh"
#include "FsObj.hh"

class RadosFsTest;

RADOS_FS_BEGIN_NAMESPACE

class FilePriv;

class File : public FsObj
{
public:
  enum OpenMode
  {
    MODE_NONE  = 0,
    MODE_READ  = 1 << 0,
    MODE_WRITE = 1 << 1,
    MODE_READ_WRITE = (MODE_READ | MODE_WRITE)
  };

  File(Filesystem *radosFs, const std::string &path, OpenMode mode = MODE_READ_WRITE);

  virtual ~File();

  File(const File &otherFile);

  File(const File *otherFile);

  File & operator=(const File &otherFile);

  OpenMode mode(void) const;

  ssize_t read(char *buff, off_t offset, size_t blen);

  int read(const std::vector<FileReadData> &intervals,
           std::string *asyncOpId = 0,
           AsyncOpCallback callback = 0, void *callbackArg = 0);

  int write(const char *buff, off_t offset, size_t blen, bool copyBuffer = false,
            std::string *asyncOpId = 0, AsyncOpCallback callback = 0,
            void *callbackArg = 0);

  int writeSync(const char *buff, off_t offset, size_t blen);

  int create(int permissions = -1, const std::string pool = "", size_t stripe=0,
             ssize_t inlineBufferSize=-1);

  int remove(void);

  int truncate(unsigned long long size);

  bool isWritable(void);

  bool isReadable(void);

  void update(void);

  void setPath(const std::string &path);

  int stat(struct stat *buff);

  int chmod(long int permissions);

  int rename(const std::string &newPath);

  int sync(const std::string &opId="");

  size_t inlineBufferSize(void) const;

private:
  boost::scoped_ptr<FilePriv> mPriv;

  friend class ::RadosFsTest;
  friend class FilePriv;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_FILE_HH */
