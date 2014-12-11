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

#ifndef RADOS_FS_OP_HH
#define RADOS_FS_OP_HH

#include <boost/chrono.hpp>
#include <boost/thread/mutex.hpp>
#include <cstdlib>
#include <rados/librados.hpp>
#include <string>
#include <vector>
#include <tr1/memory>

#include "RadosFs.hh"
#include "RadosFsAsyncOp.hh"
#include "radosfscommon.h"

#define FILE_STRIPE_LOCKER "file-stripe-locker"
#define FILE_STRIPE_LOCKER_COOKIE_WRITE "file-stripe-locker-cookie-write"
#define FILE_STRIPE_LOCKER_COOKIE_OTHER "file-stripe-locker-cookie-other"
#define FILE_STRIPE_LOCKER_TAG "file-stripe-locker-tag"
#define FILE_LOCK_DURATION 120 // seconds

RADOS_FS_BEGIN_NAMESPACE

class RadosFsIO;

typedef std::tr1::shared_ptr<RadosFsAsyncOp> RadosFsAsyncOpSP;
typedef std::tr1::shared_ptr<RadosFsIO> RadosFsIOSP;

struct OpsManager
{
  boost::mutex opsMutex;
  std::map<std::string, RadosFsAsyncOpSP> mOperations;

  int sync(void);
  int sync(const std::string &opId, bool lock=true);

  void addOperation(RadosFsAsyncOpSP op);
};

class RadosFsIO
{
public:
  RadosFsIO(RadosFs *radosFs,
            const RadosFsPoolSP pool,
            const std::string &iNode,
            size_t stripeSize);
  ~RadosFsIO();

  ssize_t read(char *buff, off_t offset, size_t blen);
  int write(const char *buff, off_t offset, size_t blen, std::string *opId = 0,
            bool copyBuffer=false);
  int writeSync(const char *buff, off_t offset, size_t blen);

  std::string inode(void) const { return mInode; }

  void setLazyRemoval(bool remove) { mLazyRemoval = remove; }
  bool lazyRemoval(void) const { return mLazyRemoval; }

  std::string getStripePath(off_t offset) const;

  size_t stripeSize(void) const { return mStripeSize; }

  size_t getLastStripeIndexAndSize(uint64_t *size) const;

  size_t getLastStripeIndex(void) const;

  size_t getSize(void) const;

  int remove(void);

  int truncate(size_t newSize);

  void lockShared(const std::string &uuid);

  void lockExclusive(const std::string &uuid);

  void unlockShared(void);

  void unlockExclusive(void);

  void manageIdleLock(double idleTimeout);

  static bool hasSingleClient(const RadosFsIOSP &io);

  int sync(const std::string &opId) { return mOpManager.sync(opId); }

private:
  RadosFs *mRadosFs;
  const RadosFsPoolSP mPool;
  const std::string mInode;
  size_t mStripeSize;
  bool mLazyRemoval;
  std::vector<rados_completion_t> mCompletionList;
  boost::chrono::system_clock::time_point mLockStart;
  boost::mutex mLockMutex;
  std::string mLocker;
  OpsManager mOpManager;

  int verifyWriteParams(off_t offset, size_t length);
  int realWrite(char *buff, off_t offset, size_t blen, bool deleteBuffer,
                RadosFsAsyncOpSP asyncOp);
  int setSizeIfBigger(size_t size);
  int setSize(size_t size);
  void setCompletionDebugMsg(librados::AioCompletion *completion,
                             const std::string &message);
  void syncAndResetLocker(RadosFsAsyncOpSP op);
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_OP_HH */
