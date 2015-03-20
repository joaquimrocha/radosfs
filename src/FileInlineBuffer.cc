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

#include <boost/bind.hpp>
#include <cassert>
#include <climits>
#include <cstdio>
#include <errno.h>
#include <rados/librados.hpp>

#include "radosfsdefines.h"
#include "AsyncOpPriv.hh"
#include "FileInlineBuffer.hh"
#include "Logger.hh"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

FileInlineBuffer::FileInlineBuffer(Filesystem *fs,
                                   const Stat *parentStat,
                                   const std::string &fileBaseName,
                                   size_t capacity)
  : fs(fs),
    parentStat(*parentStat),
    fileBaseName(fileBaseName),
    bufferSize(capacity),
    memoryBuffer(0),
    memoryBufferMutex(0)
{}

FileInlineBuffer::~FileInlineBuffer()
{}

int
FileInlineBuffer::getInlineBuffer(librados::bufferlist *buff)
{
  std::string inlineBufferKey = XATTR_FILE_INLINE_BUFFER + fileBaseName;
  Pool *pool = parentStat.pool.get();
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;
  keys.insert(inlineBufferKey);

  int ret = pool->ioctx.omap_get_vals_by_keys(parentStat.translatedPath, keys,
                                              &omap);
  if (omap.count(inlineBufferKey) > 0)
  {
    *buff = omap[inlineBufferKey];
  }

  return ret;
}

void
FileInlineBuffer::readInlineBuffer(librados::bufferlist &buff, timespec *mtime,
                                     std::string *contents)
{
  if (buff.length() == 0)
    return;

  if (mtime)
  {
    u_int32_t timeSec, timeNSec;
    buff.copy(0, sizeof(u_int32_t), (char *) &timeSec);
    buff.copy(sizeof(u_int32_t), sizeof(u_int32_t), (char *) &timeNSec);

    mtime->tv_sec = ntohl(timeSec);
    mtime->tv_nsec = ntohl(timeNSec);
  }

  if (contents)
  {
    const size_t headerSize(XATTR_FILE_INLINE_BUFFER_HEADER_SIZE);
    if (buff.length() > headerSize)
    {
      contents->assign(buff.c_str() + headerSize, buff.length() - headerSize);
    }
  }
}

void
FileInlineBuffer::read(timespec *mtime, std::string *contents)
{
  if (memoryBuffer)
  {
    boost::unique_lock<boost::mutex> lock(*memoryBufferMutex);
    contents->assign(*memoryBuffer);
    return;
  }

  librados::bufferlist buff;
  int ret = getInlineBuffer(&buff);

  if (ret != 0)
    return;

  readInlineBuffer(buff, mtime, contents);
}

static void
setInlineBufflist(librados::bufferlist &buff, timespec *spec,
                  const std::string &contents)
{
  u_int32_t hTimeSec = htonl(spec->tv_sec);
  u_int32_t hTimeNSec = htonl(spec->tv_nsec);

  buff.append((char *) &hTimeSec, sizeof(hTimeSec));
  buff.append((char *) &hTimeNSec, sizeof(hTimeNSec));
  buff.append_zero(XATTR_FILE_INLINE_BUFFER_HEADER_SIZE - 2 * sizeof(hTimeSec));
  buff.append(contents);
}

ssize_t
FileInlineBuffer::write(const char *buff, off_t offset, size_t blen)
{
  int ret = 0;

  if (capacity() > 0 && (size_t) offset < capacity())
  {
    std::string inlineBufferKey = XATTR_FILE_INLINE_BUFFER + fileBaseName;
    std::map<std::string, librados::bufferlist> omap;
    std::set<std::string> keys;
    keys.insert(inlineBufferKey);

    size_t replaceLength(std::min(blen, capacity()));

    if (memoryBuffer)
    {
      boost::unique_lock<boost::mutex> lock(*memoryBufferMutex);
      memoryBuffer->replace(offset, replaceLength, buff, replaceLength);
      return replaceLength;
    }

    boost::chrono::milliseconds sleepDuration(25);
    while (true)
    {
      std::string currentContents, inlineBuffer;
      librados::bufferlist contents;

      ret = getInlineBuffer(&contents);
      readInlineBuffer(contents, 0, &currentContents);

      inlineBuffer = currentContents;
      inlineBuffer.replace(offset, replaceLength, buff, replaceLength);

      omap[inlineBufferKey].clear();

      timespec currentTime;
      clock_gettime(CLOCK_REALTIME, &currentTime);

      setInlineBufflist(omap[inlineBufferKey], &currentTime, inlineBuffer);

      std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
      std::pair<librados::bufferlist, int> cmp(contents,
                                               LIBRADOS_CMPXATTR_OP_EQ);
      omapCmp[inlineBufferKey] = cmp;

      librados::ObjectWriteOperation writeOp;
      writeOp.omap_set(omap);
      writeOp.omap_cmp(omapCmp, 0);

      ret = parentStat.pool->ioctx.operate(parentStat.translatedPath, &writeOp);

      if (ret == -ECANCELED)
      {
        sleepDuration *= 2;
        boost::this_thread::sleep_for(sleepDuration);
      }
      else
      {
        ret = replaceLength;
        break;
      }
    }
  }

  return ret;
}

int
FileInlineBuffer::fillRemainingInlineBuffer()
{
  int ret = 0;
  std::string inlineBufferKey = XATTR_FILE_INLINE_BUFFER + fileBaseName;
  std::map<std::string, librados::bufferlist> omap;
  std::set<std::string> keys;
  keys.insert(inlineBufferKey);

  if (memoryBuffer)
  {
    boost::unique_lock<boost::mutex> lock(*memoryBufferMutex);
    memoryBuffer->append(capacity() - memoryBuffer->length(), '\0');
    return memoryBuffer->length();
  }

  boost::chrono::milliseconds sleepDuration(25);
  while (true)
  {
    std::string currentContents, inlineBuffer;
    librados::bufferlist contents;

    ret = getInlineBuffer(&contents);
    readInlineBuffer(contents, 0, &currentContents);

    inlineBuffer = currentContents;
    inlineBuffer.append(capacity() - currentContents.length(), '\0');

    omap[inlineBufferKey].clear();

    timespec currentTime;
    clock_gettime(CLOCK_REALTIME, &currentTime);

    setInlineBufflist(omap[inlineBufferKey], &currentTime, inlineBuffer);

    std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
    std::pair<librados::bufferlist, int> cmp(contents,
                                             LIBRADOS_CMPXATTR_OP_EQ);
    omapCmp[inlineBufferKey] = cmp;

    librados::ObjectWriteOperation writeOp;
    writeOp.omap_set(omap);
    writeOp.omap_cmp(omapCmp, 0);

    ret = parentStat.pool->ioctx.operate(parentStat.translatedPath, &writeOp);

    if (ret == -ECANCELED)
    {
      sleepDuration *= 2;
      boost::this_thread::sleep_for(sleepDuration);
    }
    else
    {
      ret = inlineBuffer.length();
      break;
    }
  }

  return ret;
}

void
FileInlineBuffer::truncate(size_t size)
{
  if (size >= capacity())
  {
    fillRemainingInlineBuffer();
    return;
  }

  std::string inlineBufferKey = XATTR_FILE_INLINE_BUFFER + fileBaseName;
  std::map<std::string, librados::bufferlist> omap;
  std::set<std::string> keys;
  keys.insert(inlineBufferKey);

  if (memoryBuffer)
  {
    boost::unique_lock<boost::mutex> lock(*memoryBufferMutex);

    if (size < memoryBuffer->length())
    {
      *memoryBuffer = memoryBuffer->substr(0, size);
    }
    else if (size > memoryBuffer->length())
    {
      size_t newSize = std::min(capacity(), size);
      memoryBuffer->append(newSize - memoryBuffer->length(), '\0');
    }

    return;
  }

  boost::chrono::milliseconds sleepDuration(25);
  while (true)
  {
    std::string currentContents, inlineBuffer;
    librados::bufferlist contents;

    int ret = getInlineBuffer(&contents);
    readInlineBuffer(contents, 0, &currentContents);

    if (size < currentContents.length())
    {
      inlineBuffer = currentContents.substr(0, size);
    }
    else if (size > currentContents.length())
    {
      inlineBuffer = currentContents;
      size_t newSize = std::min(capacity(), size);
      inlineBuffer.append(newSize - currentContents.length(), '\0');
    }
    else
    {
      break;
    }

    omap[inlineBufferKey].clear();

    timespec currentTime;
    clock_gettime(CLOCK_REALTIME, &currentTime);

    setInlineBufflist(omap[inlineBufferKey], &currentTime, inlineBuffer);

    std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
    std::pair<librados::bufferlist, int> cmp(contents,
                                             LIBRADOS_CMPXATTR_OP_EQ);
    omapCmp[inlineBufferKey] = cmp;

    librados::ObjectWriteOperation writeOp;
    writeOp.omap_set(omap);
    writeOp.omap_cmp(omapCmp, 0);

    ret = parentStat.pool->ioctx.operate(parentStat.translatedPath, &writeOp);

    if (ret == -ECANCELED)
    {
      sleepDuration *= 2;
      boost::this_thread::sleep_for(sleepDuration);
    }
    else
    {
      break;
    }
  }
}

void
FileInlineBuffer::setMemoryBuffer(std::string *memoryBuffer,
                                  boost::mutex *memoryBufferMutex)
{
  this->memoryBuffer = memoryBuffer;
  this->memoryBufferMutex = memoryBufferMutex;
}

RADOS_FS_END_NAMESPACE
