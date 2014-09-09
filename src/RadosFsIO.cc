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

#include <cassert>
#include <climits>
#include <cstdio>
#include <errno.h>

#include "radosfsdefines.h"
#include "RadosFsIO.hh"
#include "RadosFsLogger.hh"

RADOS_FS_BEGIN_NAMESPACE

typedef struct {
  char *contents;
  size_t lastStripeContentsSize;
  std::string path;
  RadosFsPoolSP pool;
} WriteCompletionData;

RadosFsIO::RadosFsIO(RadosFs *radosFs,
                     const RadosFsPoolSP pool,
                     const std::string &iNode,
                     size_t stripeSize,
                     bool hasAlignment)
  : mRadosFs(radosFs),
    mPool(pool),
    mInode(iNode),
    mStripeSize(stripeSize),
    mLazyRemoval(false),
    mHasAlignment(hasAlignment)
{
  assert(mStripeSize != 0);
}

RadosFsIO::~RadosFsIO()
{
  cleanCompletion();

  if (mLazyRemoval)
    remove();
}

ssize_t
RadosFsIO::read(char *buff, off_t offset, size_t blen)
{
  sync();

  if (blen == 0)
  {
    radosfs_debug("Invalid length for reading. Cannot read 0 bytes.");
    return -EINVAL;
  }

  off_t currentOffset =  offset % mStripeSize;
  size_t bytesToRead = blen;
  size_t bytesRead = 0;

  while (bytesToRead  > 0)
  {
    const std::string &fileStripe = getStripePath(blen - bytesToRead  + offset);
    const size_t length = std::min(mStripeSize - currentOffset, bytesToRead );

    int ret = rados_read(mPool->ioctx,
                         fileStripe.c_str(),
                         buff,
                         length,
                         currentOffset);

    currentOffset = 0;

    if (ret < 0)
      return ret;

    bytesRead += ret;

    if (bytesToRead < mStripeSize)
      break;
    else
      bytesToRead  -= length;

    buff += length;
  }

  return bytesRead;
}

int
RadosFsIO::writeSync(const char *buff, off_t offset, size_t blen)
{
  sync();

  int ret = write(buff, offset, blen);

  sync();

  return ret;
}

void writeCommitCallback(rados_completion_t comp, void *arg)
{
  WriteCompletionData *data = reinterpret_cast<WriteCompletionData *>(arg);

  delete[] data->contents;

  std::stringstream stream;
  stream << data->lastStripeContentsSize;

  const std::string &sizeStr = stream.str();

  int ret = rados_setxattr(data->pool->ioctx, data->path.c_str(),
                           XATTR_LAST_STRIPE_SIZE, sizeStr.c_str(),
                           sizeStr.length());

  if (ret != 0)
    radosfs_debug("Problem setting the stripe-size XAttr (%s=%s) in %s",
                  XATTR_FILE_STRIPE_SIZE, sizeStr.c_str(),
                  data->path.c_str());

  delete data;
}

int
RadosFsIO::write(const char *buff, off_t offset, size_t blen)
{
  int ret;

  if (blen == 0)
  {
    radosfs_debug("Invalid length for writing. Cannot write 0 bytes.");
    return -EINVAL;
  }

  if (((size_t) offset + blen) > mPool->size)
    return -EFBIG;

  const bool lockFiles =  mRadosFs->fileLocking();

  if (lockFiles)
  {
    while ((ret = rados_lock_shared(mPool->ioctx,
                                    inode().c_str(),
                                    FILE_STRIPE_LOCKER,
                                    FILE_STRIPE_LOCKER_COOKIE_WRITE,
                                    FILE_STRIPE_LOCKER_TAG,
                                    "",
                                    0,
                                    0)) == -EBUSY)
    {}
  }

  off_t currentOffset =  offset % mStripeSize;
  size_t bytesToWrite = blen;

  while (bytesToWrite > 0)
  {
    rados_completion_t comp;

    mCompletionList.push_back(comp);

    size_t compIndex = mCompletionList.size() - 1;
    const std::string &fileStripe = getStripePath(blen - bytesToWrite + offset);
    size_t length = std::min(mStripeSize - currentOffset, bytesToWrite);

    char *contents;

    if (length < mStripeSize && mHasAlignment)
    {
      contents = new char[mStripeSize];

      WriteCompletionData *data = new WriteCompletionData;
      data->contents = contents;
      data->lastStripeContentsSize = length;
      data->path = fileStripe;
      data->pool = mPool;

      memcpy(contents, buff + currentOffset, length);
      memset(contents + length, '\0', mStripeSize - length);
      length = mStripeSize;
      currentOffset = 0;

      rados_aio_create_completion(data, 0, writeCommitCallback,
                                  &mCompletionList[compIndex]);
    }
    else
    {
      contents = const_cast<char *>(buff);
      rados_aio_create_completion(0, 0, 0, &mCompletionList[compIndex]);
    }

    ret = rados_aio_write(mPool->ioctx, fileStripe.c_str(),
                          mCompletionList[compIndex],
                          contents,
                          length,
                          currentOffset);

    currentOffset = 0;

    // remove the completion object if something failed
    if (ret != 0)
    {
      std::vector<rados_completion_t>::iterator it = mCompletionList.begin();
      std::advance(it, compIndex);
      mCompletionList.erase(it);

      radosfs_debug("Problem writing to %s: %s",
                    fileStripe.c_str(),
                    strerror(ret));
      break;
    }

    if (bytesToWrite < mStripeSize)
      break;
    else
      bytesToWrite -= length;

    buff += length;
  }

  if (lockFiles)
  {
    rados_unlock(mPool->ioctx,
                 inode().c_str(),
                 FILE_STRIPE_LOCKER,
                 FILE_STRIPE_LOCKER_COOKIE_WRITE);
  }

  return ret;
}

void
RadosFsIO::sync()
{
  cleanCompletion(true);
}

void
RadosFsIO::cleanCompletion(bool sync)
{
  std::vector<rados_completion_t>::iterator it;

  it = mCompletionList.begin();
  while (it != mCompletionList.end())
  {
    if (sync)
      rados_aio_wait_for_complete(*it);

    rados_aio_release(*it);
    it = mCompletionList.erase(it);
  }
}

int
RadosFsIO::remove()
{
  int ret = 0;

  const bool lockFiles =  mRadosFs->fileLocking();

  if (lockFiles)
  {
    while (rados_lock_exclusive(mPool->ioctx,
                                inode().c_str(),
                                FILE_STRIPE_LOCKER,
                                FILE_STRIPE_LOCKER_COOKIE_OTHER,
                                "",
                                0,
                                0) != 0)
    {}
  }

  size_t lastStripe = getLastStripeIndex();

  for (int i = lastStripe; i >= 0; i--)
  {
    const std::string &stripe = makeFileStripeName(mInode, i);
    ret = rados_remove(mPool->ioctx, stripe.c_str());

    if (ret != 0)
    {
      radosfs_debug("Cannot remove file stripe %s: %s",
                    stripe.c_str(),
                    strerror(ret));
      break;
    }
  }

  if (lockFiles)
  {
    rados_unlock(mPool->ioctx,
                 inode().c_str(),
                 FILE_STRIPE_LOCKER,
                 FILE_STRIPE_LOCKER_COOKIE_OTHER);
  }

  return ret;
}

size_t
RadosFsIO::getLastStripeIndex(void) const
{
  int lastStripe = 1;
  int nextIndex = lastStripe + FILE_STRIPE_SEARCH_STEP;
  int lastInexistingIndex = INT_MAX;

  int ret = rados_stat(mPool->ioctx,
                       makeFileStripeName(mInode, lastStripe).c_str(),
                       0,
                       0);

  if (ret != 0)
    return 0;


  while (nextIndex != lastStripe)
  {
    ret = rados_stat(mPool->ioctx,
                     makeFileStripeName(mInode, nextIndex).c_str(),
                     0,
                     0);

    if (ret == 0)
    {
      lastStripe = nextIndex;
      nextIndex = std::min(nextIndex + FILE_STRIPE_SEARCH_STEP,
                           lastInexistingIndex - 1);
    }
    else
    {
      lastInexistingIndex = nextIndex;
      nextIndex -= std::max((nextIndex - lastStripe) / 2, 1);
    }
  }

  return lastStripe;
}

std::string
RadosFsIO::getStripePath(off_t offset) const
{
  return makeFileStripeName(mInode, offset / mStripeSize);
}

RADOS_FS_END_NAMESPACE
