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

#include <fcntl.h>
#include <iostream>
#include <sstream>

#include "radosfscommon.h"
#include "DirCache.hh"

RADOS_FS_BEGIN_NAMESPACE

#define COMPACT_LOCK_NAME "compact-dir"

DirCache::DirCache(const std::string &dirpath, rados_ioctx_t ioctx)
  : mPath(dirpath),
    mIoctx(ioctx),
    mLastCachedSize(0),
    mLastReadByte(0),
    mLogNrLines(0)
{
  pthread_mutex_init(&mContentsMutex, 0);

  genericStat(mIoctx, mPath.c_str(), &statBuff);
}

DirCache::DirCache()
  : mPath(""),
    mIoctx(0),
    mLastCachedSize(0),
    mLastReadByte(0)
{}

DirCache::~DirCache()
{
  pthread_mutex_destroy(&mContentsMutex);
}

void
DirCache::parseContents(char *buff, int length)
{
  int i = 0;
  std::istringstream iss(buff);
  //  while ((i = contents.tokenize(line, i, '\n')) != -1)
  for (std::string line; getline(iss, line, '\n');)
  {
    // we add the name key's length + 2 because we count
    // the operation char (+ or -) and the "
    int namePos = strlen(INDEX_NAME_KEY) + 2;

    if (line.length() < namePos)
      continue;

    // we avoid including the last character because it is " and \n
    std::string entry(line, namePos, line.length() - namePos - 2);

    size_t index = 0;
    while(true)
    {
      index = entry.find("\\\"", index);

      if (index == std::string::npos)
        break;

      entry.replace(index, 2, "\"");
    }

    pthread_mutex_lock(&mContentsMutex);

    mLogNrLines++;

    if (mContents.count(entry.c_str()) > 0)
    {
      if (line[0] == '-')
      {
        mContents.erase(entry.c_str());
      }
    }
    else
      mContents.insert(entry);

    pthread_mutex_unlock(&mContentsMutex);
  }
}

int
DirCache::update()
{
  int ret =  genericStat(mIoctx, mPath.c_str(), &statBuff);

  if (ret != 0)
    return ret;

  if (statBuff.st_size == mLastCachedSize)
    return 0;

  uint64_t buffLength = statBuff.st_size - mLastCachedSize;
  char buff[buffLength];

  ret = rados_read(mIoctx, mPath.c_str(), buff, buffLength, mLastReadByte);

  if (ret > 0)
  {
    mLastReadByte = ret;
    buff[buffLength - 1] = '\0';
    parseContents(buff, buffLength);
  }
  else
  {
    return ret;
  }

  mLastCachedSize = mLastReadByte = statBuff.st_size;

  return 0;
}

const std::string
DirCache::getEntry(int index)
{
  std::string entry("");

  pthread_mutex_lock(&mContentsMutex);

  const int size = (int) mContents.size();

  if (index < size)
  {
    std::set<std::string>::iterator it = mContents.begin();
    std::advance(it, index);

    entry = *it;
  }

  pthread_mutex_unlock(&mContentsMutex);

  return entry;
}

void
DirCache::compactDirOpLog(void)
{
  update();

  genericStat(mIoctx, mPath.c_str(), &statBuff);

  const char *keys[] = { DIR_LOG_UPDATED };
  const char *values[] = { DIR_LOG_UPDATED_FALSE };
  const size_t lengths[] = { strlen(values[0]) };

  rados_write_op_t writeOp = rados_create_write_op();

  rados_write_op_omap_set(writeOp, keys, values, lengths, 1);

  rados_write_op_operate(writeOp, mIoctx, mPath.c_str(), NULL, 0);

  rados_release_write_op(writeOp);

  writeOp = rados_create_write_op();

  std::set<std::string>::iterator it;
  std::string compactContents;

  for (it = mContents.begin(); it != mContents.end(); it++)
  {
    compactContents += getObjectIndexLine(*it, '+');
  }

  rados_write_op_truncate(writeOp, 0);

  if (compactContents != "")
    rados_write_op_write_full(writeOp,
                              compactContents.c_str(),
                              compactContents.length());

  int cmpRet;
  rados_write_op_omap_cmp(writeOp,
                          DIR_LOG_UPDATED,
                          LIBRADOS_CMPXATTR_OP_EQ,
                          DIR_LOG_UPDATED_FALSE,
                          strlen(DIR_LOG_UPDATED_FALSE),
                          &cmpRet);

  rados_write_op_operate(writeOp, mIoctx, mPath.c_str(), NULL, 0);

  genericStat(mIoctx, mPath.c_str(), &statBuff);

  mLastCachedSize = mLastReadByte = statBuff.st_size;

  mLogNrLines = mContents.size();

  rados_release_write_op(writeOp);
}

float
DirCache::logRatio() const
{
  if (mLogNrLines > 0)
    return mContents.size() / (float) mLogNrLines;

  return -1;
}

RADOS_FS_END_NAMESPACE
