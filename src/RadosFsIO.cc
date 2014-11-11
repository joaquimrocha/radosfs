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
#include <rados/librados.hpp>

#include "radosfsdefines.h"
#include "RadosFsIO.hh"
#include "RadosFsLogger.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsIO::RadosFsIO(RadosFs *radosFs,
                     const RadosFsPoolSP pool,
                     const std::string &iNode,
                     size_t stripeSize)
  : mRadosFs(radosFs),
    mPool(pool),
    mInode(iNode),
    mStripeSize(stripeSize),
    mLazyRemoval(false)
{
  assert(mStripeSize != 0);
}

RadosFsIO::~RadosFsIO()
{
  if (mLazyRemoval)
    remove(false);
}

ssize_t
RadosFsIO::read(char *buff, off_t offset, size_t blen)
{
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
  return write(buff, offset, blen, true);
}

int
RadosFsIO::write(const char *buff, off_t offset, size_t blen)
{
  return write(buff, offset, blen, false);
}

int
RadosFsIO::write(const char *buff, off_t offset, size_t blen, bool sync)
{
  int ret;

  if (blen == 0)
  {
    radosfs_debug("Invalid length for writing. Cannot write 0 bytes.");
    return -EINVAL;
  }

  const size_t totalSize = offset + blen;

  if (totalSize > mPool->size)
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

  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  off_t currentOffset =  offset % mStripeSize;
  size_t bytesToWrite = blen;
  size_t firstStripe = offset / mStripeSize;
  size_t lastStripe = (offset + blen - 1) / mStripeSize;
  size_t totalStripes = lastStripe - firstStripe + 1;
  librados::AioCompletion **compList = 0;

  if (sync)
    compList = new librados::AioCompletion*[totalStripes];

  for (size_t i = 0; i < totalStripes; i++)
  {
    librados::ObjectWriteOperation op;
    librados::bufferlist contents;
    librados::AioCompletion *completion;
    const std::string &fileStripe = makeFileStripeName(inode(), firstStripe + i);
    size_t length = std::min(mStripeSize - currentOffset, bytesToWrite);
    std::string contentsStr(buff + (blen - bytesToWrite), length);

    if (mPool->hasAlignment())
    {
      size_t stripeRemaining = stripeSize() - length;

      if (stripeRemaining > 0)
        contents.append_zero(stripeRemaining);
    }

    contents.append(contentsStr);
    op.write(currentOffset, contents);
    completion = librados::Rados::aio_create_completion();
    ctx.aio_operate(fileStripe, completion, &op);

    if (sync)
      compList[i] = completion;
    else
      completion->release();

    currentOffset = 0;
    bytesToWrite -= length;
  }

  if (sync)
  {
    for (size_t i = 0; i < totalStripes; i++)
    {
      compList[i]->wait_for_complete();
      compList[i]->release();
    }
  }

  delete[] compList;

  if (lockFiles)
  {
    rados_unlock(mPool->ioctx,
                 inode().c_str(),
                 FILE_STRIPE_LOCKER,
                 FILE_STRIPE_LOCKER_COOKIE_WRITE);
  }

  return ret;
}

int
RadosFsIO::remove(bool sync)
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

  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  size_t lastStripe = getLastStripeIndex();
  librados::AioCompletion **compList = 0;

  if (sync)
    compList = new librados::AioCompletion*[lastStripe + 1];

  // We start deleting from the base stripe onward because this will result
  // in other calls to the object eventually seeing the removal sooner
  for (size_t i = 0; i <= lastStripe; i++)
  {
    librados::ObjectWriteOperation op;
    librados::AioCompletion *completion;
    const std::string &fileStripe = makeFileStripeName(inode(), i);

    op.remove();
    completion = librados::Rados::aio_create_completion();
    ctx.aio_operate(fileStripe, completion, &op);

    if (sync)
      compList[i] = completion;
    else
      completion->release();
  }

  if (sync)
  {
    for (size_t i = 0; i <= lastStripe; i++)
    {
      compList[i]->wait_for_complete();
      compList[i]->release();
    }
  }

  delete[] compList;

  if (lockFiles)
  {
    rados_unlock(mPool->ioctx,
                 inode().c_str(),
                 FILE_STRIPE_LOCKER,
                 FILE_STRIPE_LOCKER_COOKIE_OTHER);
  }

  return ret;
}

int
RadosFsIO::truncate(size_t newSize, bool sync)
{
  if (newSize > mPool->size)
  {
    radosfs_debug("The size given for truncating is too big for the pool.");
    return -EFBIG;
  }

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

  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  size_t currentSize;
  size_t lastStripe = getLastStripeIndexAndSize(&currentSize);
  size_t newLastStripe = (newSize == 0) ? 0 : (newSize - 1) / stripeSize();
  librados::AioCompletion **compList = 0;
  bool truncateDown = currentSize > newSize;
  size_t totalStripes = 1;
  size_t newLastStripeSize = newSize % stripeSize();
  bool hasAlignment = mPool->hasAlignment();

  if (newLastStripe == 0 && newSize > stripeSize())
    newLastStripe = stripeSize();

  if (truncateDown)
    totalStripes = lastStripe - newLastStripe + 1;

  if (sync)
    compList = new librados::AioCompletion*[totalStripes];

  for (ssize_t i = totalStripes - 1; i >= 0; i--)
  {
    librados::ObjectWriteOperation op;
    librados::AioCompletion *completion;
    const std::string &fileStripe = makeFileStripeName(inode(),
                                                       newLastStripe + i);

    if (i == 0)
    {
      // The base stripe should never be deleting on when a truncate occurs
      // but rather really truncated -- in the case the pool has no alignment --
      // or have the part out of the truncated range zeroed otherwise.
      if (hasAlignment)
      {
        librados::bufferlist zeroContents;
        zeroContents.append_zero(stripeSize() - newLastStripeSize);
        op.write(newLastStripeSize, zeroContents);
      }
      else
      {
        op.truncate(newLastStripeSize);
      }

      op.assert_exists();
    }
    else
    {
      op.remove();
    }

    completion = librados::Rados::aio_create_completion();
    ctx.aio_operate(fileStripe, completion, &op);

    if (sync)
      compList[i] = completion;
    else
      completion->release();
  }

  if (sync)
  {
    for (size_t i = 0; i < totalStripes; i++)
    {
      compList[i]->wait_for_complete();
      compList[i]->release();
    }
  }

  delete[] compList;

  if (lockFiles)
  {
    rados_unlock(mPool->ioctx,
                 inode().c_str(),
                 FILE_STRIPE_LOCKER,
                 FILE_STRIPE_LOCKER_COOKIE_OTHER);
  }

  return 0;
}

size_t
RadosFsIO::getLastStripeIndex(void) const
{
  return getLastStripeIndexAndSize(0);
}

librados::ObjectReadOperation
makeStripeReadOp(bool hasAlignment, u_int64_t *size, int *statRet,
                 librados::bufferlist *stripeXAttr)
{
  librados::ObjectReadOperation op;

  op.stat(size, 0, statRet);

  if (hasAlignment)
  {
    // Since the alignment is set, the last stripe will be the same size as the
    // other ones so we retrieve the real data size which was set as an XAttr
    op.getxattr(XATTR_LAST_STRIPE_SIZE, stripeXAttr, 0);
    op.set_op_flags(librados::OP_FAILOK);
  }

  return op;
}

ssize_t
getLastValid(int *retValues, size_t valuesSize)
{
  ssize_t i;
  for (i = 0; i < (ssize_t) valuesSize; i++)
  {
    if (retValues[i] != 0)
      break;
  }

  return i - 1;
}

size_t
RadosFsIO::getLastStripeIndexAndSize(uint64_t *size) const
{
  int lastStripe = -1;
  const size_t numOps(FILE_STRIPE_NUM_CHECKS);
  librados::bufferlist stripeSizeXAttrs[numOps];
  int xattrsRets[numOps];
  size_t stripeSizes[numOps];
  rados_completion_t comps[numOps];
  librados::IoCtx ctx;

  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  memset(xattrsRets, 0, numOps);
  ssize_t lastValid(numOps - 1);

  while (lastValid == (ssize_t) numOps - 1)
  {
    for (size_t i = 0; i < numOps; i++)
    {
      librados::ObjectReadOperation op = makeStripeReadOp(mPool->hasAlignment(),
                                                          &stripeSizes[i],
                                                          &xattrsRets[i],
                                                          &stripeSizeXAttrs[i]);

      rados_aio_create_completion(0, 0, 0, &comps[i]);
      librados::AioCompletion completion((librados::AioCompletionImpl *)
                                         comps[i]);

      ctx.aio_operate(makeFileStripeName(mInode, lastStripe + 1 + i),
                      &completion, &op, 0);
    }

    for (size_t i = 0; i < numOps; i++)
    {
      rados_aio_wait_for_complete(comps[i]);
      rados_aio_release(comps[i]);
    }

    lastValid = getLastValid(xattrsRets, numOps);

    if (lastValid < 0)
      break;

    lastStripe += lastValid + 1;
  }

  if (lastStripe < 0)
    lastStripe = 0;

  size_t validIndex = (lastStripe % numOps);

  if (xattrsRets[validIndex] == 0 && size)
  {
    *size = stripeSizes[validIndex];

    librados::bufferlist *stripeXAttr = &stripeSizeXAttrs[validIndex];
    if (mPool->hasAlignment() && stripeXAttr->length() > 0)
    {
      std::string stripeXAttrStr(stripeXAttr->c_str(), stripeXAttr->length());
      *size = atol(stripeXAttrStr.c_str());
    }
  }

  return lastStripe;
}

std::string
RadosFsIO::getStripePath(off_t offset) const
{
  return makeFileStripeName(mInode, offset / mStripeSize);
}

size_t
RadosFsIO::getSize() const
{
  u_int64_t size = 0;
  size_t numStripes = getLastStripeIndexAndSize(&size);

  return numStripes * stripeSize() + size;
}

RADOS_FS_END_NAMESPACE
