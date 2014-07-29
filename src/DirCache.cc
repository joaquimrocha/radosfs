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

DirCache::DirCache(const std::string &dirpath, RadosFsPoolSP pool)
  : mPath(dirpath),
    mPool(pool),
    mLastCachedSize(0),
    mLastReadByte(0),
    mLogNrLines(0)
{
  pthread_mutex_init(&mContentsMutex, 0);

  genericStat(ioctx(), mPath.c_str(), &statBuff);
}

DirCache::DirCache()
  : mPath(""),
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
  std::istringstream iss(std::string(buff, length - 1));

  for (std::string line; getline(iss, line, '\n');)
  {
    int startPos = 0, lastPos = 0;
    std::string key, name, value;
    bool deleteEntry = false;
    std::map<std::string, std::string> metadataToAdd;
    std::set<std::string> metadataToDelete;
    const int metadataPrefixLength = strlen(INDEX_METADATA_PREFIX) + 1;

    while ((lastPos = splitToken(line, startPos, key, value)) != startPos)
    {
      if (key != "")
      {
        value = unescapeObjName(value);

        if (key.compare(1, std::string::npos, INDEX_NAME_KEY) == 0)
        {
          name = value;

          if (key[0] == '-')
          {
            deleteEntry = true;
            break;
          }
        }
        else if (key.compare(1,
                             metadataPrefixLength,
                             INDEX_METADATA_PREFIX ".") == 0)
        {
          const std::string metadataKey =
              unescapeObjName(key.substr(metadataPrefixLength + 1));

          if (key[0] == '-')
            metadataToDelete.insert(metadataKey);
          else
            metadataToAdd[metadataKey] = value;
        }
      }

      startPos = lastPos;
      key = value = "";
    }

    pthread_mutex_lock(&mContentsMutex);

    mLogNrLines++;

    if (mContents.count(name.c_str()) > 0)
    {
      if (deleteEntry)
      {
        mContents.erase(name.c_str());
        mEntryNames.erase(name);
      }
      else
      {
        std::map<std::string, std::string>::iterator mapIt;
        for (mapIt = metadataToAdd.begin(); mapIt != metadataToAdd.end(); mapIt++)
        {
          const std::string &mdKey = (*mapIt).first;
          const std::string &mdValue = (*mapIt).second;
          mContents[name].metadata[mdKey] = mdValue;
        }

        std::set<std::string>::iterator setIt;
        for (setIt = metadataToDelete.begin(); setIt != metadataToDelete.end(); setIt++)
        {
          const std::string &mdKey = *setIt;

          if (mContents[name].metadata.count(mdKey) > 0)
            mContents[name].metadata.erase(mdKey);
        }
      }
    }
    else
    {
      DirEntry entry;
      entry.name = name;
      mContents[name] = entry;
      mEntryNames.insert(name);
    }

    pthread_mutex_unlock(&mContentsMutex);
  }
}

int
DirCache::update()
{
  int ret =  genericStat(ioctx(), mPath.c_str(), &statBuff);

  if (ret != 0)
    return ret;

  if (statBuff.st_size == mLastCachedSize)
    return 0;

  uint64_t buffLength = statBuff.st_size - mLastCachedSize;
  char buff[buffLength];

  ret = rados_read(ioctx(), mPath.c_str(), buff, buffLength, mLastReadByte);

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

  const int size = (int) mEntryNames.size();

  if (index < size)
  {
    std::set<std::string>::iterator it = mEntryNames.begin();
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

  genericStat(ioctx(), mPath.c_str(), &statBuff);

  const char *keys[] = { DIR_LOG_UPDATED };
  const char *values[] = { DIR_LOG_UPDATED_FALSE };
  const size_t lengths[] = { strlen(values[0]) };

  rados_write_op_t writeOp = rados_create_write_op();

  rados_write_op_omap_set(writeOp, keys, values, lengths, 1);

  rados_write_op_operate(writeOp, ioctx(), mPath.c_str(), NULL, 0);

  rados_release_write_op(writeOp);

  writeOp = rados_create_write_op();

  std::map<std::string, DirEntry>::iterator it;
  std::string compactContents;

  for (it = mContents.begin(); it != mContents.end(); it++)
  {
    const DirEntry &entry = (*it).second;
    compactContents += '+';
    compactContents += INDEX_NAME_KEY "=\"" + escapeObjName(entry.name) + "\" ";

    std::map<std::string, std::string>::const_iterator mdIt;
    for (mdIt = entry.metadata.begin(); mdIt != entry.metadata.end(); mdIt++)
    {
      compactContents += "+" + (*mdIt).first + "=\"" + (*mdIt).second + "\" ";
    }

    compactContents += "\n";
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

  rados_write_op_operate(writeOp, ioctx(), mPath.c_str(), NULL, 0);

  genericStat(ioctx(), mPath.c_str(), &statBuff);

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

bool
DirCache::hasEntry(const std::string &entry)
{
  bool entryExists(false);
  pthread_mutex_lock(&mContentsMutex);

  entryExists = mContents.count(entry) > 0;

  pthread_mutex_unlock(&mContentsMutex);

  return entryExists;
}

int
DirCache::getMetadata(const std::string &entry,
                      const std::string &key,
                      std::string &value)
{
  int ret = -ENOENT;
  pthread_mutex_lock(&mContentsMutex);

  if (mContents.count(entry) > 0)
  {
    std::map<std::string, std::string> &metadata = mContents[entry].metadata;

    if (metadata.count(key) > 0)
    {
      value = metadata[key];
      ret = 0;
    }
  }

  pthread_mutex_unlock(&mContentsMutex);

  return ret;
}

RADOS_FS_END_NAMESPACE
