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
  mOpManager.sync();

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
  const std::string &opId = generateUuid();

  setSizeIfBigger(totalSize);

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

    mOpManager.addCompletion(opId, completion);
    ctx.aio_operate(fileStripe, completion, &op);

    currentOffset = 0;
    bytesToWrite -= length;
  }

  if (sync)
  {
    mOpManager.sync();
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
  const std::string &opId = generateUuid();

  // We start deleting from the base stripe onward because this will result
  // in other calls to the object eventually seeing the removal sooner
  for (size_t i = 0; i <= lastStripe; i++)
  {
    librados::ObjectWriteOperation op;
    librados::AioCompletion *completion;
    const std::string &fileStripe = makeFileStripeName(inode(), i);

    op.remove();
    completion = librados::Rados::aio_create_completion();

    mOpManager.addCompletion(opId, completion);
    ctx.aio_operate(fileStripe, completion, &op);
  }

  if (sync)
  {
    mOpManager.sync();
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

int
RadosFsIO::truncate(size_t newSize, bool sync)
{
  if (newSize > mPool->size)
  {
    radosfs_debug("The size given for truncating is too big for the pool.");
    return -EFBIG;
  }

  const bool lockFiles =  mRadosFs->fileLocking();
  mOpManager.sync();

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
  bool truncateDown = currentSize > newSize;
  size_t totalStripes = 1;
  size_t newLastStripeSize = newSize % stripeSize();
  bool hasAlignment = mPool->hasAlignment();
  const std::string &opId = generateUuid();

  if (newLastStripe == 0 && newSize > stripeSize())
    newLastStripe = stripeSize();

  if (truncateDown)
    totalStripes = lastStripe - newLastStripe + 1;

  setSize(newSize);

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

    mOpManager.addCompletion(opId, completion);
  }

  if (sync)
  {
    mOpManager.sync();
  }

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
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  librados::ObjectReadOperation op;
  librados::bufferlist sizeXAttr;
  size_t fileSize(0);

  op.getxattr(XATTR_FILE_SIZE, &sizeXAttr, 0);
  op.assert_exists();
  ctx.operate(inode(), &op, 0);

  if (sizeXAttr.length() > 0)
  {
    const std::string sizeStr(sizeXAttr.c_str(), sizeXAttr.length());
    fileSize = atoll(sizeStr.c_str());
  }

  if (size)
    *size = fileSize;

  if (fileSize > 0)
    fileSize = (fileSize - 1) / stripeSize();

  return fileSize;
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
  getLastStripeIndexAndSize(&size);

  return size;
}

int
RadosFsIO::setSizeIfBigger(size_t size)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  librados::ObjectWriteOperation writeOp;
  librados::bufferlist xattrValue;
  std::stringstream stream;

  stream << size;
  xattrValue.append(stream.str());

  // Set the new size only if it's greater than the one already set
  writeOp.setxattr(XATTR_FILE_SIZE, xattrValue);
  writeOp.cmpxattr(XATTR_FILE_SIZE, LIBRADOS_CMPXATTR_OP_GT, size);

  return ctx.operate(inode(), &writeOp);
}

int
RadosFsIO::setSize(size_t size)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(mPool->ioctx, ctx);
  librados::ObjectWriteOperation writeOp;
  librados::bufferlist xattrValue;
  std::stringstream stream;
  stream << size;

  xattrValue.append(stream.str());
  writeOp.create(false);
  writeOp.setxattr(XATTR_FILE_SIZE, xattrValue);

  return ctx.operate(inode(), &writeOp);
}


void
OpsManager::sync(void)
{
  std::map<std::string, CompletionList>::iterator it, oldIt;
  boost::unique_lock<boost::mutex> lock(opsMutex);

  it = mOperations.begin();
  while (it != mOperations.end())
  {
    oldIt = it;
    oldIt++;

    sync((*it).first, false);

    it = oldIt;
  }
}

void
OpsManager::sync(const std::string &opId, bool lock)
{
  boost::unique_lock<boost::mutex> uniqueLock;

  if (lock)
    uniqueLock = boost::unique_lock<boost::mutex>(opsMutex);

  if (mOperations.count(opId) == 0)
    return;

  const CompletionList &compList = mOperations[opId];
  for (size_t i = 0; i < compList.size(); i++)
  {
    compList[i]->wait_for_complete();
    compList[i]->release();
  }

  mOperations.erase(opId);
}

void
OpsManager::addCompletion(const std::string &opId,
                          librados::AioCompletion *comp)
{
  boost::unique_lock<boost::mutex> lock(opsMutex);

  mOperations[opId].push_back(comp);
}

RADOS_FS_END_NAMESPACE
