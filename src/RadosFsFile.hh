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

#include <cstdlib>
#include <memory>

#include "RadosFs.hh"
#include "RadosFsInfo.hh"

class RadosFsTest;

RADOS_FS_BEGIN_NAMESPACE

class RadosFsFilePriv;

class RadosFsFile : public RadosFsInfo
{
public:
  enum OpenMode
  {
    MODE_NONE  = 0,
    MODE_READ  = 1 << 0,
    MODE_WRITE = 1 << 1,
    MODE_READ_WRITE = (MODE_READ | MODE_WRITE)
  };

  RadosFsFile(RadosFs *radosFs, const std::string &path, OpenMode mode);

  virtual ~RadosFsFile();

  RadosFsFile(const RadosFsFile &otherFile);

  RadosFsFile(const RadosFsFile *otherFile);

  RadosFsFile & operator=(const RadosFsFile &otherFile);

  OpenMode mode(void) const;

  ssize_t read(char *buff, off_t offset, size_t blen);

  ssize_t write(const char *buff, off_t offset, size_t blen);

  ssize_t writeSync(const char *buff, off_t offset, size_t blen);

  int create(int permissions = -1);

  int remove(void);

  int truncate(unsigned long long size);

  bool isWritable(void);

  bool isReadable(void);

  void update(void);

  void setPath(const std::string &path);

  int stat(struct stat *buff);

private:
  std::auto_ptr<RadosFsFilePriv> mPriv;

friend class ::RadosFsTest;
friend class RadosFsFilePriv;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_FILE_HH */
