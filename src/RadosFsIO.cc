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

#include <boost/bind.hpp>
#include <cassert>
#include <climits>
#include <cstdio>
#include <errno.h>
#include <rados/librados.hpp>

#include "radosfsdefines.h"
#include "RadosFsAsyncOpPriv.hh"
#include "RadosFsIO.hh"
#include "RadosFsLogger.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsIO::RadosFsIO(RadosFs *radosFs,
                     const RadosFsPoolSP pool,
                     const std::string &iNode,
                     size_t stripeSize)
  : mRadosFs(radosFs),
    mPool(pool),
    mInode(iNode),
    mStripeSize(stripeSize),
    mLazyRemoval(false),
    mLocker("")
{
  assert(mStripeSize != 0);
}

RadosFsIO::~RadosFsIO()
{
  mOpManager.sync();

  if (mLazyRemoval)
  {
    remove();
    return;
  }

  boost::unique_lock<boost::mutex> lock(mLockMutex);
  unlockShared();
  unlockExclusive();
}

ssize_t
RadosFsIO::read(char *buff, off_t offset, size_t blen)
{
  mOpManager.sync();

  if (blen == 0)
  {
    radosfs_debug("Invalid length for reading. Cannot read 0 bytes.");
    return -EINVAL;
  }

  const size_t fileSize = getSize();

  if ((offset + blen) > fileSize)
  {
    radosfs_debug("Length for reading is greater than the file's current size: "
                  "%lu > %lu", (blen + offset), fileSize);
    return -EOVERFLOW;
  }

  off_t currentOffset =  offset % mStripeSize;
  size_t bytesToRead = blen;
  size_t bytesRead = 0;

  while (bytesToRead  > 0)
  {
    librados::bufferlist readBuff;
    const std::string &fileStripe = getStripePath(blen - bytesToRead  + offset);
    const size_t length = std::min(mStripeSize - currentOffset, bytesToRead );

    int ret = mPool->ioctx.read(fileStripe, readBuff, length, currentOffset);

    if (ret > 0)
    {
      memcpy(buff, readBuff.c_str(), readBuff.length());
    }

    radosfs_debug("Read %lu bytes starting from %lu in stripe %s: "
                  "retcode=%d (%s)", length, currentOffset, fileStripe.c_str(),
                  ret, strerror(abs(ret)));

    currentOffset = 0;

    // If the bytes read were less than expected or the stripe didn't exist,
    // it should assign null characters to the nonexistent length.
    if ((size_t) ret < length || ret == -ENOENT)
    {
      memset(buff + ret, '\0', length - ret);

    }
    else if (ret < 0)
    {
      return ret;
    }

    bytesRead += length;

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
  int ret;

  RadosFsAsyncOpSP asyncOp(new RadosFsAsyncOp(generateUuid()));
  mOpManager.addOperation(asyncOp);

  if ((ret = verifyWriteParams(offset, blen)) != 0)
    return ret;

  return realWrite(const_cast<char *>(buff), offset, blen, false, asyncOp);
}

int
RadosFsIO::write(const char *buff, off_t offset, size_t blen, std::string *opId,
                 bool copyBuffer)
{
  int ret = 0;

  if ((ret = verifyWriteParams(offset, blen)) != 0)
    return ret;

  RadosFsAsyncOpSP asyncOp(new RadosFsAsyncOp(generateUuid()));
  mOpManager.addOperation(asyncOp);

  if (opId)
    opId->assign(asyncOp->id());

  char *bufferToWrite = const_cast<char *>(buff);

  if (copyBuffer)
  {
    bufferToWrite = new char[blen];
    memcpy(bufferToWrite, buff, blen);
  }

  mRadosFs->mPriv->getIoService()->post(boost::bind(&RadosFsIO::realWrite, this,
                                                    bufferToWrite, offset, blen,
                                                    copyBuffer, asyncOp));
  return 0;
}

void
onCompleted(rados_completion_t comp, void *arg)
{
  int ret = rados_aio_get_return_value(comp);
  std::string *msg = reinterpret_cast<std::string *>(arg);

  radosfs_debug("Completed: %s: retcode=%d (%s)", msg->c_str(), ret,
                strerror(abs(ret)));
  delete msg;
}

void
RadosFsIO::setCompletionDebugMsg(librados::AioCompletion *completion,
                                 const std::string &message)
{
  if (mRadosFs->logLevel() == RadosFs::LOG_LEVEL_DEBUG)
  {
    std::string *arg = new std::string(message);
    completion->set_complete_callback(arg, onCompleted);
  }
}

void
RadosFsIO::lockShared(const std::string &uuid)
{
  int ret;

  {
    boost::unique_lock<boost::mutex> lock(mLockMutex);
    boost::chrono::duration<double> seconds;
    seconds = boost::chrono::system_clock::now() - mLockStart;
    if (seconds.count() < FILE_LOCK_DURATION - 1)
    {
      radosfs_debug("Keep shared lock: %s %s", mLocker.c_str(), uuid.c_str());
      if (mLocker == "")
        mLocker = uuid;

      if (mLocker == uuid)
        return;
    }
  }

  timeval tm;
  tm.tv_sec = FILE_LOCK_DURATION;
  tm.tv_usec = 0;
  while ((ret = mPool->ioctx.lock_shared(inode(), FILE_STRIPE_LOCKER,
                                         FILE_STRIPE_LOCKER_COOKIE_WRITE,
                                         FILE_STRIPE_LOCKER_TAG, "", &tm,
                                         0)) == -EBUSY)
  {}

  boost::unique_lock<boost::mutex> lock(mLockMutex);
  mLocker = uuid;
  mLockStart = boost::chrono::system_clock::now();

  radosfs_debug("Set/renew shared lock: %s ", mLocker.c_str());
}

void
RadosFsIO::lockExclusive(const std::string &uuid)
{
  int ret;

  {
    boost::unique_lock<boost::mutex> lock(mLockMutex);
    boost::chrono::duration<double> seconds;

    seconds = boost::chrono::system_clock::now() - mLockStart;
    if (seconds.count() < FILE_LOCK_DURATION - 1)
    {
      radosfs_debug("Keep exclusive lock: %s %s", mLocker.c_str(), uuid.c_str());
      if (mLocker == "")
      {
        mLocker = uuid;
      }

      if (mLocker == uuid)
        return;
    }
  }

  timeval tm;
  tm.tv_sec = FILE_LOCK_DURATION;
  tm.tv_usec = 0;
  while ((ret = mPool->ioctx.lock_exclusive(inode(), FILE_STRIPE_LOCKER,
                                            FILE_STRIPE_LOCKER_COOKIE_OTHER,
                                            "", &tm, 0)) != 0)
  {}

  boost::unique_lock<boost::mutex> lock(mLockMutex);
  mLocker = uuid;
  mLockStart = boost::chrono::system_clock::now();

  radosfs_debug("Set/renew exclusive lock: %s ", mLocker.c_str());
}

void
RadosFsIO::unlockShared()
{
  mPool->ioctx.unlock(inode(), FILE_STRIPE_LOCKER,
                      FILE_STRIPE_LOCKER_COOKIE_WRITE);
  mLocker = "";
  radosfs_debug("Unlocked shared lock.");
}

void
RadosFsIO::unlockExclusive()
{
  mPool->ioctx.unlock(inode(), FILE_STRIPE_LOCKER,
                      FILE_STRIPE_LOCKER_COOKIE_OTHER);
  mLocker = "";
  radosfs_debug("Unlocked exclusive lock.");
}

int
RadosFsIO::verifyWriteParams(off_t offset, size_t length)
{
  int ret = 0;

  if (length == 0)
  {
    radosfs_debug("Invalid length for writing. Cannot write 0 bytes.");
    ret = -EINVAL;
  }

  if (offset + length > mPool->size)
    ret = -EFBIG;

  return ret;
}

int
RadosFsIO::realWrite(char *buff, off_t offset, size_t blen, bool deleteBuffer,
                     RadosFsAsyncOpSP asyncOp)
{
  int ret = 0;

  off_t currentOffset =  offset % mStripeSize;
  size_t bytesToWrite = blen;
  size_t firstStripe = offset / mStripeSize;
  size_t lastStripe = (offset + blen - 1) / mStripeSize;
  size_t totalStripes = lastStripe - firstStripe + 1;
  const std::string &opId = asyncOp->id();
  const size_t totalSize = offset + blen;

  if (totalStripes > 1)
    lockExclusive(opId);
  else
    lockShared(opId);

  setSizeIfBigger(totalSize);

  radosfs_debug("Writing in inode '%s' (op id: '%s') to size %lu affecting "
                "stripes %lu-%lu", inode().c_str(), opId.c_str(), totalSize,
                firstStripe, lastStripe);

  for (size_t i = 0; i < totalStripes; i++)
  {
    if (totalStripes > 1)
      lockExclusive(opId);
    else
      lockShared(opId);

    librados::ObjectWriteOperation op;
    librados::bufferlist contents;
    librados::AioCompletion *completion;
    const std::string &fileStripe = makeFileStripeName(inode(), firstStripe + i);
    size_t length = std::min(mStripeSize - currentOffset, bytesToWrite);
    std::string contentsStr(buff + (blen - bytesToWrite), length);

    contents.append(contentsStr);

    if (mPool->hasAlignment())
    {
      size_t stripeRemaining = stripeSize() - length;

      if (stripeRemaining > 0)
        contents.append_zero(stripeRemaining);
    }

    op.write(currentOffset, contents);

    completion = librados::Rados::aio_create_completion();

    std::stringstream stream;
    stream << "Wrote (od id='" << opId << "') stripe '" << fileStripe << "'";
    setCompletionDebugMsg(completion, stream.str());

    mPool->ioctx.aio_operate(fileStripe, completion, &op);
    asyncOp->mPriv->addCompletion(completion);

    currentOffset = 0;
    bytesToWrite -= length;

    radosfs_debug("Scheduling writing of stripe '%s' in (op id='%s')",
                  fileStripe.c_str(), opId.c_str());
  }

  asyncOp->mPriv->setReady();
  syncAndResetLocker(asyncOp);

  if (deleteBuffer)
    delete buff;

  return ret;
}

int
RadosFsIO::remove()
{
  const std::string &opId = generateUuid();
  mOpManager.sync();

  mLockMutex.lock();
  unlockShared();
  mLockMutex.unlock();

  lockExclusive(opId);

  int ret = 0;
  size_t lastStripe = getLastStripeIndex();

  radosfs_debug("Remove (op id='%s') inode '%s' affecting stripes 0-%lu",
                opId.c_str(), inode().c_str(), 0, lastStripe);

  RadosFsAsyncOpSP asyncOp(new RadosFsAsyncOp(opId));
  mOpManager.addOperation(asyncOp);

  // We start deleting from the base stripe onward because this will result
  // in other calls to the object eventually seeing the removal sooner
  for (size_t i = 0; i <= lastStripe; i++)
  {
    lockExclusive(opId);

    librados::ObjectWriteOperation op;
    librados::AioCompletion *completion;
    const std::string &fileStripe = makeFileStripeName(inode(), i);

    radosfs_debug("Removing stripe '%s' in (op id= '%s')",
                  fileStripe.c_str(), opId.c_str());

    op.remove();
    completion = librados::Rados::aio_create_completion();

    std::stringstream stream;
    stream << "Remove (op id='" << opId << "') stripe '" << fileStripe << "'";
    setCompletionDebugMsg(completion, stream.str());

    mPool->ioctx.aio_operate(fileStripe, completion, &op);
    asyncOp->mPriv->addCompletion(completion);
  }

  asyncOp->mPriv->setReady();
  syncAndResetLocker(asyncOp);

  return ret;
}

int
RadosFsIO::truncate(size_t newSize)
{
  if (newSize > mPool->size)
  {
    radosfs_debug("The size given for truncating is too big for the pool.");
    return -EFBIG;
  }

  mOpManager.sync();

  const std::string &opId = generateUuid();

  mLockMutex.lock();
  unlockShared();
  mLockMutex.unlock();

  lockExclusive(opId);

  size_t currentSize;
  size_t lastStripe = getLastStripeIndexAndSize(&currentSize);
  size_t newLastStripe = (newSize == 0) ? 0 : (newSize - 1) / stripeSize();
  bool truncateDown = currentSize > newSize;
  size_t totalStripes = 1;
  size_t newLastStripeSize = newSize % stripeSize();
  bool hasAlignment = mPool->hasAlignment();

  if (newLastStripe == 0 && newSize > stripeSize())
    newLastStripe = stripeSize();

  if (truncateDown)
    totalStripes = lastStripe - newLastStripe + 1;

  setSize(newSize);

  radosfs_debug("Truncating stripe '%s' (op id='%s').", inode().c_str(),
                opId.c_str());

  RadosFsAsyncOpSP asyncOp(new RadosFsAsyncOp(opId));
  mOpManager.addOperation(asyncOp);

  for (ssize_t i = totalStripes - 1; i >= 0; i--)
  {
    lockExclusive(opId);

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

      radosfs_debug("Truncating stripe '%s' (op id='%s').", fileStripe.c_str(),
                    opId.c_str());

      op.assert_exists();
    }
    else
    {
      op.remove();

      radosfs_debug("Removing stripe '%s' in truncate (op id='%s')",
                    fileStripe.c_str(), opId.c_str());
    }

    completion = librados::Rados::aio_create_completion();

    std::stringstream stream;
    stream << "Truncate (op id='" << opId << "') stripe '" << fileStripe << "'";
    setCompletionDebugMsg(completion, stream.str());

    mPool->ioctx.aio_operate(fileStripe, completion, &op);
    asyncOp->mPriv->addCompletion(completion);
  }

  asyncOp->mPriv->setReady();
  syncAndResetLocker(asyncOp);

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
    std::set<std::string> keys;
    std::map<std::string, librados::bufferlist> omap;

    // Since the alignment is set, the last stripe will be the same size as the
    // other ones so we retrieve the real data size which was set as an XAttr
    keys.insert(XATTR_LAST_STRIPE_SIZE);
    op.omap_get_vals_by_keys(keys, &omap, 0);
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
  librados::ObjectReadOperation op;
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;
  size_t fileSize(0);

  keys.insert(XATTR_FILE_SIZE);
  op.omap_get_vals_by_keys(keys, &omap, 0);
  op.assert_exists();
  mPool->ioctx.operate(inode(), &op, 0);

  if (omap.count(XATTR_FILE_SIZE) > 0)
  {
    librados::bufferlist sizeXAttr = omap[XATTR_FILE_SIZE];
    const std::string sizeStr(sizeXAttr.c_str(), sizeXAttr.length());
    fileSize = strtoul(sizeStr.c_str(), 0, 16);
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
  librados::ObjectWriteOperation writeOp;
  std::map<std::string, librados::bufferlist> omap;
  std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
  librados::bufferlist cmpValue;
  std::string sizeHex = fileSizeToHex(size);

  omap[XATTR_FILE_SIZE].append(sizeHex);
  cmpValue.append(sizeHex);
  std::pair<librados::bufferlist, int> cmp(cmpValue, LIBRADOS_CMPXATTR_OP_LT);
  omapCmp[XATTR_FILE_SIZE] = cmp;

  // Set the new size only if it's greater than the one already set
  int compRet;
  writeOp.omap_set(omap);
  writeOp.omap_cmp(omapCmp, &compRet);

  int ret = mPool->ioctx.operate(inode(), &writeOp);

  radosfs_debug("Set size %d to '%s' if it's greater: retcode=%d (%s)",
                size, inode().c_str(), ret, strerror(abs(ret)));

  return ret;
}

int
RadosFsIO::setSize(size_t size)
{
  librados::ObjectWriteOperation writeOp;
  std::map<std::string, librados::bufferlist> omap;
  std::string sizeHex = fileSizeToHex(size);

  omap[XATTR_FILE_SIZE].append(sizeHex);

  writeOp.create(false);
  writeOp.omap_set(omap);

  int ret = mPool->ioctx.operate(inode(), &writeOp);

  radosfs_debug("Set size %d to '%s': retcode=%d (%s)", size,
                inode().c_str(), ret, strerror(abs(ret)));

  return ret;
}

void
RadosFsIO::manageIdleLock(double idleTimeout)
{
  if (mLockMutex.try_lock())
  {
    if (mLocker == "")
    {
      boost::chrono::duration<double> seconds;
      seconds = boost::chrono::system_clock::now() - mLockStart;
      bool lockIsIdle = seconds.count() >= idleTimeout;
      bool lockTimedOut = seconds.count() > FILE_LOCK_DURATION;

      if (lockIsIdle && !lockTimedOut)
      {
        radosfs_debug("Unlocked idle lock.");

        unlockShared();
        unlockExclusive();
        // Set the lock start to look as if it expired so it does not try to
        // unlock it anymore.
        mLockStart = boost::chrono::system_clock::now() -
                     boost::chrono::seconds(FILE_LOCK_DURATION + 1);
      }
    }

    mLockMutex.unlock();
  }
}

void
RadosFsIO::syncAndResetLocker(RadosFsAsyncOpSP op)
{
  boost::unique_lock<boost::mutex> lock(mLockMutex);
  op->waitForCompletion();
  mLocker = "";
}

bool
RadosFsIO::hasSingleClient(const RadosFsIOSP &io)
{
  // If there is only one client using an instance of the given RadosFsIO, then
  // the use count is 2 because there is a reference hold in RadosFsPriv's map.
  return io.use_count() == 2;
}

int
OpsManager::sync(void)
{
  int ret = 0;
  std::map<std::string, RadosFsAsyncOpSP>::iterator it, oldIt;
  boost::unique_lock<boost::mutex> lock(opsMutex);

  it = mOperations.begin();
  while (it != mOperations.end())
  {
    oldIt = it;
    oldIt++;

    int syncResult = sync((*it).first, false);

    // Assign the first error we eventually find
    if (ret == 0)
      ret = syncResult;

    it = oldIt;
  }

  return ret;
}

int
OpsManager::sync(const std::string &opId, bool lock)
{
  int ret = -ENOENT;
  boost::unique_lock<boost::mutex> uniqueLock;
  RadosFsAsyncOpSP asyncOp;

  if (lock)
    uniqueLock = boost::unique_lock<boost::mutex>(opsMutex);

  if (mOperations.count(opId) == 0)
    return ret;

  ret = mOperations[opId]->waitForCompletion();
  mOperations.erase(opId);

  return ret;
}

void
OpsManager::addOperation(RadosFsAsyncOpSP op)
{
  boost::unique_lock<boost::mutex> lock(opsMutex);

  mOperations[op->id()] = op;
}

RADOS_FS_END_NAMESPACE
