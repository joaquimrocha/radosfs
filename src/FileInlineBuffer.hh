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

#ifndef RADOS_FS_FILE_INLINE_BUFFER_HH
#define RADOS_FS_FILE_INLINE_BUFFER_HH

#include <arpa/inet.h>
#include <boost/chrono.hpp>
#include <boost/thread/mutex.hpp>
#include <cstdlib>
#include <rados/librados.hpp>
#include <string>
#include <vector>
#include <tr1/memory>

#include "Filesystem.hh"
#include "AsyncOp.hh"
#include "radosfscommon.h"

#define FILE_CHUNK_LOCKER "file-chunk-locker"
#define FILE_CHUNK_LOCKER_COOKIE_WRITE "file-chunk-locker-cookie-write"
#define FILE_CHUNK_LOCKER_COOKIE_OTHER "file-chunk-locker-cookie-other"
#define FILE_CHUNK_LOCKER_TAG "file-chunk-locker-tag"
#define FILE_LOCK_DURATION 120 // seconds

RADOS_FS_BEGIN_NAMESPACE

class FileInlineBuffer
{
public:
  FileInlineBuffer(Filesystem *fs,
                   const Stat *parentStat,
                   const std::string &fileBaseName,
                   size_t capacity);
  ~FileInlineBuffer(void);

  int fillRemainingInlineBuffer(void);

  static void readInlineBuffer(librados::bufferlist &buff, timespec *mtime,
                               std::string *contents);

  ssize_t write(const char *buff, off_t offset, size_t blen);

  void read(timespec *mtime, std::string *contents);

  int getInlineBuffer(librados::bufferlist *buff);

  void truncate(size_t size);

  size_t capacity(void) const { return bufferSize; }

  void setMemoryBuffer(std::string *memoryBuffer,
                       boost::mutex *memoryBufferMutex);

  Filesystem *fs;
  Stat parentStat;
  std::string fileBaseName;
  size_t bufferSize;
  std::string *memoryBuffer;
  boost::mutex *memoryBufferMutex;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_FILE_INLINE_BUFFER_HH */
