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
#include "AsyncOpPriv.hh"
#include "FileIO.hh"
#include "Logger.hh"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

FileReadDataImp::FileReadDataImp(char *buff, off_t offset, size_t length,
                                 ssize_t *retValue)
  : FileReadData(buff, offset, length, retValue),
    readOpMutex(new boost::shared_mutex),
    buffList(new librados::bufferlist),
    opResult(0)
{}

FileReadDataImp::FileReadDataImp(const FileReadDataImp &otherObj)
  : FileReadData(otherObj.buff, otherObj.offset, otherObj.length,
                 otherObj.retValue),
    readOpMutex(otherObj.readOpMutex),
    buffList(new librados::bufferlist),
    opResult(otherObj.opResult)
{}

FileReadDataImp::FileReadDataImp(const FileReadData &readData)
  : FileReadData(readData),
    readOpMutex(new boost::shared_mutex),
    buffList(new librados::bufferlist),
    opResult(0)
{}

FileReadDataImp::~FileReadDataImp(void)
{
  delete buffList;
}

void
FileReadDataImp::addReturnValue(int value)
{
  boost::unique_lock<boost::shared_mutex> lock(*readOpMutex);
  if (!retValue)
    return;

  if (value >= 0)
    *retValue = *retValue + value;
  else if (*retValue == 0)
    *retValue = value;
}

FileIO::FileIO(Filesystem *radosFs, const PoolSP pool, const std::string &iNode,
               size_t stripeSize)
  : mRadosFs(radosFs),
    mPool(pool),
    mInode(iNode),
    mPath(""),
    mStripeSize(stripeSize),
    mLazyRemoval(false),
    mLocker(""),
    mInlineBuffer(0),
    mHasBackLink(false)
{
  assert(mStripeSize != 0);
}

FileIO::FileIO(Filesystem *radosFs, const PoolSP pool, const std::string &iNode,
               const std::string &path, size_t stripeSize)
  : mRadosFs(radosFs),
    mPool(pool),
    mInode(iNode),
    mPath(path),
    mStripeSize(stripeSize),
    mLazyRemoval(false),
    mLocker(""),
    mInlineBuffer(0),
    // If the path is not set, then we assume the backlink has been set in order
    // to avoid trying to do it when needed
    mHasBackLink(mPath.empty())
{
  assert(mStripeSize != 0);
}

FileIO::~FileIO()
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

void
FileIO::separateReadData(const FileReadDataImpSP &readData,
                         FileReadDataImpSP &inlineData,
                         FileReadDataImpSP &inodeData) const
{
  size_t oldLength = readData->length;

  inlineData.reset(new FileReadDataImp(*readData));
  inlineData->length = mInlineBuffer->capacity() - readData->offset;

  inodeData.reset(new FileReadDataImp(*inlineData));
  inodeData->buff = inlineData->buff + inlineData->length;
  inodeData->offset += inlineData->length;
  inodeData->length = oldLength - inlineData->length;
}

void
FileIO::getInlineAndInodeReadData(const std::vector<FileReadData> &intervals,
                                  std::vector<FileReadDataImpSP> *dataInline,
                                  std::vector<FileReadDataImpSP> *dataInode)
{
  for (size_t i = 0; i < intervals.size(); i++)
  {
    FileReadDataImpSP readData(new FileReadDataImp(intervals[i]));

    if (readData->retValue)
      *readData->retValue = 0;

    if (mInlineBuffer && ((size_t) readData->offset < mInlineBuffer->capacity()))
    {
      if ((readData->offset + readData->length) > mInlineBuffer->capacity())
      {
        FileReadDataImpSP inlineData, inodeData;
        separateReadData(readData, inlineData, inodeData);
        dataInline->push_back(inlineData);
        dataInode->push_back(inodeData);
      }
      else
      {
        dataInline->push_back(readData);
      }
    }
    else
    {
      dataInode->push_back(readData);
    }
  }
}

void
FileIO::getReadDataPerStripe(const std::vector<FileReadDataImpSP> &intervals,
                   std::map<size_t, std::vector<FileReadDataImpSP> > *inodeData)
{
  for (size_t i = 0; i < intervals.size(); i++)
  {
    FileReadDataImpSP readData(intervals[i]);
    size_t stripeIndex =  readData->offset / mStripeSize;
    off_t localOffset =  readData->offset % mStripeSize;
    const size_t originalLength = readData->length;
    size_t remainingLength = originalLength;

    // Separate each FileReadData object that goes beyond one stripe in more
    // objects so they can only fit one stripe
    while (remainingLength > 0)
    {
      FileReadDataImpSP data(new FileReadDataImp(*readData));
      data->opResult = -1;
      data->buff = readData->buff + originalLength - remainingLength;
      data->offset = localOffset;
      data->length = std::min(mStripeSize - localOffset, remainingLength);
      inodeData->operator[](stripeIndex++).push_back(data);
      remainingLength -= data->length;
      localOffset = 0;
    }
  }
}

static size_t
assignInodeSize(ReadOpArgs *args)
{
  boost::unique_lock<boost::shared_mutex> lock(*args->readOpMutex);

  if ((*args->inodeSize) == -1)
  {
    *args->inodeSize = args->fileIO->getSize();
    radosfs_debug("Calculated file size for vector read request: size=%u",
                  *args->inodeSize);
  }

  return *args->inodeSize;
}

static void
assignRemainingReadData(FileReadDataImp *data, const size_t byteOffset,
                        const size_t inodeSize, const size_t currentReadDataSize)
{
  // Sets null chars to the remaining (unset) length of the read data if the
  // inode size covers the read data.
  // This needs to be done for cases where the file is truncated to a greater
  // size than the file's data, meaning that the read data should be null chars.
  if (inodeSize > 0)
  {
    if (byteOffset < inodeSize)
    {
      size_t length = std::min(data->length - currentReadDataSize,
                               inodeSize - byteOffset);
      memset(data->buff + currentReadDataSize, '\0', length);
      data->addReturnValue(length);

      radosfs_debug("Setting %u null chars for vector read request: "
                    "offset=%u; length=%u; size filled with real data: %u;"
                    "filesize=%u", length, data->offset, data->length,
                    currentReadDataSize, inodeSize);
    }
  }
}

void
FileIO::onReadInlineBufferCompleted(rados_completion_t comp, void *arg)
{
  ReadInlineOpArgs *args = reinterpret_cast<ReadInlineOpArgs *>(arg);
  std::map<std::string, librados::bufferlist>::iterator it;

  if ((it = args->omap.find(args->fileBaseName)) != args->omap.end())
  {
    std::string contents;
    librados::bufferlist buff = (*it).second;
    FileInlineBuffer::readInlineBuffer(buff, 0, &contents);

    radosfs_debug("Inline buffer read (size=%u).", contents.size());

    for (size_t i = 0; i < args->readData.size(); i++)
    {
      FileReadDataImpSP data = args->readData[i];
      char *buff = data->buff;
      off_t offset = data->offset;
      size_t length = data->length;

      if (contents != "")
      {
        memcpy(buff, contents.c_str() + offset, length);
        data->addReturnValue(length);

        radosfs_debug("Setting %u bytes from inline buffer for vector read "
                      "request: offset=%u; length=%u;", length, data->offset,
                      data->length);
      }
      else
      {
        size_t inodeSize = assignInodeSize(args);
        const size_t byteOffset = data->offset;

        assignRemainingReadData(data.get(), byteOffset, inodeSize, 0);
      }
    }
  }

  args->asyncOp->mPriv->setPartialReady();

  delete args;
}

void
FileIO::onReadCompleted(rados_completion_t comp, void *arg)
{
  ReadStripeOpArgs *args = reinterpret_cast<ReadStripeOpArgs *>(arg);
  const int ret = rados_aio_get_return_value(comp);

  radosfs_debug("Reading inode's stripe #%u complete with retcode=%d (%s)",
                args->fileStripe, ret, strerror(abs(ret)));

  for (size_t i = 0; i < args->readData.size(); i++)
  {
    FileReadDataImpSP data = args->readData[i].first;
    librados::bufferlist *buff = args->readData[i].second;

    if (buff->length() > 0)
    {
      memcpy(data->buff, buff->c_str(), buff->length());
      data->addReturnValue(buff->length());

      radosfs_debug("Setting %u bytes from stripe #%d for vector read request: "
                    "offset=%u; length=%u;", buff->length(), args->fileStripe,
                    data->offset, data->length);
    }

    if (buff->length() < data->length)
    {
      size_t inodeSize = assignInodeSize(args);
      const size_t byteOffset = args->fileStripe * args->fileIO->mStripeSize +
                                data->offset;

      assignRemainingReadData(data.get(), byteOffset, inodeSize, buff->length());
    }

    delete buff;
  }

  args->asyncOp->mPriv->setPartialReady();

  delete args;
}

void
FileIO::vectorReadInlineBuffer( const std::vector<FileReadDataImpSP> &readData,
                             boost::shared_ptr<boost::shared_mutex> readOpMutex,
                             AsyncOpSP asyncOp,
                             boost::shared_ptr<ssize_t> inodeSize)
{
  ReadInlineOpArgs *args = new ReadInlineOpArgs;
  args->fileBaseName = XATTR_FILE_INLINE_BUFFER + mInlineBuffer->fileBaseName;
  args->readData = readData;
  args->asyncOp = asyncOp;
  args->readOpMutex = readOpMutex;
  args->fileIO = this;
  args->inodeSize = inodeSize;

  std::set<std::string> keys;
  keys.insert(args->fileBaseName);

  librados::ObjectReadOperation readOp;
  readOp.omap_get_vals_by_keys(keys, &args->omap, 0);

  librados::AioCompletion *completion;
  completion = librados::Rados::aio_create_completion();
  completion->set_complete_callback(args, FileIO::onReadInlineBufferCompleted);
  args->asyncOp->mPriv->addCompletion(completion);

  Pool *pool = mInlineBuffer->parentStat.pool.get();
  pool->ioctx.aio_operate(mInlineBuffer->parentStat.translatedPath, completion,
                          &readOp, 0);
}

void
FileIO::vectorReadStripe(size_t fileStripe,
                         const std::vector<FileReadDataImpSP> &readDataVector,
                         boost::shared_ptr<boost::shared_mutex> readOpMutex,
                         AsyncOpSP asyncOp,
                         boost::shared_ptr<ssize_t> inodeSize)
{
  ReadStripeOpArgs *readOp = new ReadStripeOpArgs;
  readOp->fileStripe = fileStripe;
  readOp->readOpMutex = readOpMutex;
  readOp->asyncOp = asyncOp;
  readOp->fileIO = this;
  readOp->inodeSize = inodeSize;
  librados::ObjectReadOperation op;
  const std::string stripeName = makeFileStripeName(mInode, fileStripe);

  for (size_t i = 0; i < readDataVector.size(); i++)
  {
    FileReadDataImpSP readData(readDataVector[i]);
    librados::bufferlist *readBuff = new librados::bufferlist;
    std::pair<FileReadDataImpSP, librados::bufferlist *> dataAndResult(readData,
                                                                       readBuff);
    readOp->readData.push_back(dataAndResult);

    op.read(readData->offset, readData->length, readBuff, &readData->opResult);
    radosfs_debug("Setting read op for the stripe %s . offset=%u; "
                  "length=%u;", stripeName.c_str(), readData->offset,
                  readData->length);
  }

  librados::AioCompletion *completion = librados::Rados::aio_create_completion();
  completion->set_complete_callback(readOp, FileIO::onReadCompleted);
  asyncOp->mPriv->addCompletion(completion);
  mPool->ioctx.aio_operate(stripeName, completion, &op, 0);
}

int
FileIO::read(const std::vector<FileReadData> &intervals, std::string *asyncOpId,
             AsyncOpCallback callback, void *callbackArg)
{
  mOpManager.sync();

  if (intervals.size() == 0)
  {
    radosfs_debug("No FileReadData elements given for reading.");
    return -EINVAL;
  }

  AsyncOpSP asyncOp(new AsyncOp(generateUuid()));

  if (callback)
    asyncOp->setCallback(callback, callbackArg);

  mOpManager.addOperation(asyncOp);

  if (asyncOpId)
    asyncOpId->assign(asyncOp->id());

  std::vector<FileReadDataImpSP> inlineReadData, inodeReadData;
  getInlineAndInodeReadData(intervals, &inlineReadData, &inodeReadData);
  boost::shared_ptr<boost::shared_mutex> readOpMutex(new boost::shared_mutex);
  boost::shared_ptr<ssize_t> inodeSize(new ssize_t);
  *inodeSize = -1;

  if (mInlineBuffer && inlineReadData.size() > 0)
  {
    radosfs_debug("Vector reading inline buffer. opId=%s",
                  asyncOp->id().c_str());
    vectorReadInlineBuffer(inlineReadData, readOpMutex, asyncOp, inodeSize);
  }

  std::map<size_t, std::vector<FileReadDataImpSP> > dataPerStripe;
  getReadDataPerStripe(inodeReadData, &dataPerStripe);

  if (dataPerStripe.size() > 0)
  {
    radosfs_debug("Vector reading stripes. opId=%s", asyncOp->id().c_str());
    std::map<size_t, std::vector<FileReadDataImpSP> >::iterator it;
    for (it = dataPerStripe.begin(); it != dataPerStripe.end(); it++)
    {
      size_t fileStripe = (*it).first;
      const std::vector<FileReadDataImpSP> &readDataVector = (*it).second;

      vectorReadStripe(fileStripe, readDataVector, readOpMutex, asyncOp,
                       inodeSize);
    }
  }

  return 0;
}

ssize_t
FileIO::read(char *buff, off_t offset, size_t blen)
{
  mOpManager.sync();

  if (blen == 0)
  {
    radosfs_debug("Invalid length for reading. Cannot read 0 bytes.");
    return -EINVAL;
  }

  ssize_t ret = 0;
  ssize_t fileSize = -1;

  if (mInlineBuffer)
  {
    if ((size_t) offset < mInlineBuffer->capacity())
    {
      std::string contentsStr;
      mInlineBuffer->read(0, &contentsStr);

      fileSize = contentsStr.length();

      if ((ssize_t) offset < fileSize)
      {
        ret = std::min(blen, contentsStr.length());

        if (ret > 0)
        {
          memcpy(buff, contentsStr.c_str() + offset, ret);
        }

        offset += ret;
        buff += ret;

        assert((size_t) ret <= blen);

        blen -= ret;

        if (blen == 0)
          return ret;
      }
    }
  }

  size_t bytesRead = ret;

  if ((fileSize == -1) || (ret == fileSize))
  {
    size_t currentSize;
    ret = getLastStripeIndexAndSize(&currentSize);
    fileSize = currentSize;
  }

  if (ret < 0)
    return ret;

  if ((ssize_t) offset >= fileSize)
  {
    // Nothing to read
    return 0;
  }

  off_t currentOffset =  offset % mStripeSize;
  size_t bytesToRead = std::min(blen, (size_t) fileSize);

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
    if ((size_t) ret < length)
    {
      if (ret < 0)
      {
        if (ret == -ENOENT)
          ret = 0;
        else
          return ret;
      }

      memset(buff + ret, '\0', length - ret);
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
FileIO::writeSync(const char *buff, off_t offset, size_t blen)
{
  int ret;

  AsyncOpSP asyncOp(new AsyncOp(generateUuid()));
  mOpManager.addOperation(asyncOp);

  if ((ret = verifyWriteParams(offset, blen)) != 0)
    return ret;

  return realWrite(const_cast<char *>(buff), offset, blen, false, asyncOp);
}

int
FileIO::write(const char *buff, off_t offset, size_t blen, std::string *opId,
              bool copyBuffer, AsyncOpCallback callback, void *arg)
{
  int ret = 0;

  if ((ret = verifyWriteParams(offset, blen)) != 0)
    return ret;

  AsyncOpSP asyncOp(new AsyncOp(generateUuid()));

  if (callback)
    asyncOp->setCallback(callback, arg);

  mOpManager.addOperation(asyncOp);

  if (opId)
    opId->assign(asyncOp->id());

  char *bufferToWrite = const_cast<char *>(buff);

  if (copyBuffer)
  {
    bufferToWrite = new char[blen];
    memcpy(bufferToWrite, buff, blen);
  }

  mRadosFs->mPriv->getIoService()->post(boost::bind(&FileIO::realWrite, this,
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
FileIO::setCompletionDebugMsg(librados::AioCompletion *completion,
                              const std::string &message)
{
  if (mRadosFs->logLevel() == Filesystem::LOG_LEVEL_DEBUG)
  {
    std::string *arg = new std::string(message);
    completion->set_complete_callback(arg, onCompleted);
  }
}

void
FileIO::lockShared(const std::string &uuid)
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
FileIO::lockExclusive(const std::string &uuid)
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
FileIO::unlockShared()
{
  mPool->ioctx.unlock(inode(), FILE_STRIPE_LOCKER,
                      FILE_STRIPE_LOCKER_COOKIE_WRITE);
  mLocker = "";
  radosfs_debug("Unlocked shared lock.");
}

void
FileIO::unlockExclusive()
{
  mPool->ioctx.unlock(inode(), FILE_STRIPE_LOCKER,
                      FILE_STRIPE_LOCKER_COOKIE_OTHER);
  mLocker = "";
  radosfs_debug("Unlocked exclusive lock.");
}

int
FileIO::verifyWriteParams(off_t offset, size_t length)
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

void
FileIO::setAlignedStripeWriteOp(librados::ObjectWriteOperation &op,
                                const std::string &fileStripe,
                                const size_t offset,
                                const std::string &newContents)
{
  std::map<std::string, librados::bufferlist> xattrs;
  librados::ObjectReadOperation readOp;
  librados::bufferlist contentsBl;

  readOp.read(0, mStripeSize, &contentsBl, 0);
  readOp.getxattrs(&xattrs, 0);

  mPool->ioctx.operate(fileStripe, &readOp, 0);

  std::string contents;
  contents.reserve(mStripeSize);

  if (contentsBl.length() > 0)
  {
    contents.assign(contentsBl.c_str(), contentsBl.length());
  }
  else if (newContents.length() != mStripeSize)
  {
    contents.assign(mStripeSize, '\0');
  }

  contents.replace(offset, newContents.length(), newContents);

  if (contentsBl.length() == contents.length())
  {
    contentsBl.copy_in(0, contents.length(), contents.c_str());
  }
  else
  {
    contentsBl.clear();
    contentsBl.append(contents);
  }

  op.remove();
  op.set_op_flags(librados::OP_FAILOK);
  op.create(false);

  std::map<std::string, librados::bufferlist>::iterator it;
  for (it = xattrs.begin(); it != xattrs.end(); it++)
  {
    const std::string &xattrName = (*it).first;
    op.setxattr(xattrName.c_str(), (*it).second);
  }

  op.append(contentsBl);
}

int
FileIO::realWrite(char *buff, off_t offset, size_t blen, bool deleteBuffer,
                  AsyncOpSP asyncOp)
{
  int ret = 0;
  char *originalBuff = buff;

  if (mInlineBuffer && mInlineBuffer->capacity() > 0)
  {
    size_t inlineContentsSize = 0;
    ssize_t opResult = 0;

    if ((size_t) offset < mInlineBuffer->capacity())
    {
      opResult = mInlineBuffer->write(buff, offset, blen);
      inlineContentsSize = opResult;
    }
    else
    {
      opResult = mInlineBuffer->fillRemainingInlineBuffer();
    }

    if (opResult < 0)
    {
      asyncOp->mPriv->setReady();
      return ret;
    }

    offset += (off_t) inlineContentsSize;
    buff += inlineContentsSize;
    blen -= (size_t) inlineContentsSize;

    if (blen == 0)
    {
      asyncOp->mPriv->setReady();

      if (deleteBuffer)
        delete[] originalBuff;

      return ret;
    }
  }

  updateTimeAsyncInXAttr(mPool, mInode, XATTR_MTIME);

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
      setAlignedStripeWriteOp(op, fileStripe, currentOffset, contentsStr);
    }
    else
    {
      op.write(currentOffset, contents);
    }

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
    delete[] originalBuff;

  return ret;
}

int
FileIO::remove()
{
  const std::string &opId = generateUuid();
  mOpManager.sync();

  mLockMutex.lock();
  unlockShared();
  mLockMutex.unlock();

  lockExclusive(opId);

  int ret = 0;
  ssize_t lastStripe = getLastStripeIndex();

  if (lastStripe < 0)
  {
    radosfs_debug("Error trying to remove inode '%s' (retcode=%d): %s",
                  inode().c_str(), lastStripe, strerror(abs(lastStripe)));
    return lastStripe;
  }

  radosfs_debug("Remove (op id='%s') inode '%s' affecting stripes 0-%lu",
                opId.c_str(), inode().c_str(), 0, lastStripe);

  AsyncOpSP asyncOp(new AsyncOp(opId));
  mOpManager.addOperation(asyncOp);

  // We start deleting from the base stripe onward because this will result
  // in other calls to the object eventually seeing the removal sooner
  for (size_t i = 0; i <= (size_t) lastStripe; i++)
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
FileIO::truncate(size_t newSize)
{
  if (newSize > mPool->size)
  {
    radosfs_debug("The size given for truncating is too big for the pool.");
    return -EFBIG;
  }

  mOpManager.sync();

  if (mInlineBuffer)
  {
    mInlineBuffer->truncate(newSize);
  }

  updateTimeAsyncInXAttr(mPool, mInode, XATTR_MTIME);

  const std::string &opId = generateUuid();

  mLockMutex.lock();
  unlockShared();
  mLockMutex.unlock();

  lockExclusive(opId);

  size_t currentSize;
  ssize_t lastStripe = getLastStripeIndexAndSize(&currentSize);

  if (lastStripe < 0)
  {
    if (lastStripe == -ENOENT || lastStripe == -ENODATA)
      lastStripe = 0;
    else
      return lastStripe;
  }

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

  AsyncOpSP asyncOp(new AsyncOp(opId));
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
        std::string zeroStr(stripeSize() - newLastStripeSize, '\0');
        setAlignedStripeWriteOp(op, fileStripe, newLastStripeSize, zeroStr);
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

ssize_t
FileIO::getLastStripeIndex(void) const
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

ssize_t
FileIO::getLastStripeIndexAndSize(uint64_t *size) const
{
  librados::ObjectReadOperation op;
  librados::bufferlist sizeXAttr;
  ssize_t fileSize(0);

  op.getxattr(XATTR_FILE_SIZE, &sizeXAttr, 0);
  op.assert_exists();

  int ret = mPool->ioctx.operate(inode(), &op, 0);

  if (ret < 0)
    return ret;

  if (sizeXAttr.length() > 0)
  {
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
FileIO::getStripePath(off_t offset) const
{
  return makeFileStripeName(mInode, offset / mStripeSize);
}

size_t
FileIO::getSize() const
{
  u_int64_t size = 0;
  getLastStripeIndexAndSize(&size);

  return size;
}

void
inodeBackLinkCb(rados_completion_t comp, void *arg)
{
  int ret = rados_aio_get_return_value(comp);
  FileIO *io = reinterpret_cast<FileIO *>(arg);

  // We only assume we have set the back link if it succeeded to do so or if
  // didn't because it had already been set (operation canceled)
  if (ret == 0 || ret == -ECANCELED)
    io->setHasBackLink(true);
}

int
FileIO::setSizeIfBigger(size_t size)
{
  librados::ObjectWriteOperation writeOp;
  librados::bufferlist sizeBl;
  sizeBl.append(fileSizeToHex(size));

  // Set the new size only if it's greater than the one already set
  writeOp.setxattr(XATTR_FILE_SIZE, sizeBl);
  writeOp.cmpxattr(XATTR_FILE_SIZE, LIBRADOS_CMPXATTR_OP_GT, sizeBl);

  int ret = mPool->ioctx.operate(inode(), &writeOp);

  // Set the back link because this op might create the object
  if (ret == 0 && shouldSetBacklink())
  {
    setInodeBacklinkAsync(mPool, mPath, inode(), 0, inodeBackLinkCb, this);
  }

  radosfs_debug("Set size %d to '%s' if it's greater: retcode=%d (%s)",
                size, inode().c_str(), ret, strerror(abs(ret)));

  return ret;
}

int
FileIO::setSize(size_t size)
{
  librados::bufferlist sizeBl;
  sizeBl.append(fileSizeToHex(size));

  librados::ObjectWriteOperation writeOp;
  writeOp.create(false);
  writeOp.setxattr(XATTR_FILE_SIZE, sizeBl);

  int ret = mPool->ioctx.operate(inode(), &writeOp);

  // Set the back link because this op might create the object
  if (ret == 0 && shouldSetBacklink())
  {
    setInodeBacklinkAsync(mPool, mPath, inode(), 0, inodeBackLinkCb, this);
  }

  radosfs_debug("Set size %d to '%s': retcode=%d (%s)", size,
                inode().c_str(), ret, strerror(abs(ret)));

  return ret;
}

void
FileIO::manageIdleLock(double idleTimeout)
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
FileIO::syncAndResetLocker(AsyncOpSP op)
{
  boost::unique_lock<boost::mutex> lock(mLockMutex);
  op->waitForCompletion();
  mLocker = "";
}

bool
FileIO::hasSingleClient(const FileIOSP &io)
{
  // If there is only one client using an instance of the given FileIO, then
  // the use count is 2 because there is a reference hold in FsPriv's map.
  return io.use_count() == 2;
}

void
FileIO::setInlineBuffer(const std::string path, size_t bufferSize)
{

  Stat parentStat;
  std::string parentPath = getParentDir(path, 0);

  if (parentPath == "")
    return;

  if (mInlineBuffer)
  {
    if ((mInlineBuffer->parentStat.path + mInlineBuffer->fileBaseName) == path)
      return;

    mInlineBuffer.reset();
  }

  if (mRadosFs->mPriv->stat(parentPath, &parentStat) != 0)
    return;

  mInlineBuffer.reset(new FileInlineBuffer(mRadosFs, &parentStat,
                                           path.substr(parentPath.length()),
                                           bufferSize));
}

void
FileIO::setLazyRemoval(bool remove)
{
  mLazyRemoval = remove;

  if (mInlineBuffer)
  {
    boost::unique_lock<boost::mutex> lock(mInlineMemBufferMutex);
    mInlineBuffer->setMemoryBuffer(&mInlineMemBuffer, &mInlineMemBufferMutex);
  }
}

void
FileIO::setHasBackLink(bool hasBacklink)
{
  boost::unique_lock<boost::mutex> lock(mHasBackLinkMutex);
  mHasBackLink = hasBacklink;
}

bool
FileIO::hasBackLink(void)
{
  boost::unique_lock<boost::mutex> lock(mHasBackLinkMutex);
  return mHasBackLink;
}

void
FileIO::setPath(const std::string &path)
{
  boost::unique_lock<boost::mutex> lock(mHasBackLinkMutex);
  mPath = path;
  mHasBackLink = false;
}

void
FileIO::updateBackLink(const std::string *oldBackLink)
{
  setInodeBacklinkAsync(mPool, mPath, inode(), oldBackLink, inodeBackLinkCb,
                        this);
}

int
OpsManager::sync(void)
{
  int ret = 0;
  std::map<std::string, AsyncOpSP>::iterator it, oldIt;
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
  AsyncOpSP asyncOp;

  if (lock)
    uniqueLock = boost::unique_lock<boost::mutex>(opsMutex);

  if (mOperations.count(opId) == 0)
    return ret;

  ret = mOperations[opId]->waitForCompletion();
  mOperations.erase(opId);

  return ret;
}

void
OpsManager::addOperation(AsyncOpSP op)
{
  boost::unique_lock<boost::mutex> lock(opsMutex);

  mOperations[op->id()] = op;
}

RADOS_FS_END_NAMESPACE
