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

#include <sys/stat.h>
#include <sstream>

#include "FileIO.hh"
#include "FileInode.hh"
#include "FileInodePriv.hh"
#include "FilesystemPriv.hh"
#include "radosfscommon.h"

RADOS_FS_BEGIN_NAMESPACE

FileInodePriv::FileInodePriv(Filesystem *fs, const std::string &poolName,
                             const std::string &name, const size_t stripeSize)
  : fs(fs),
    name(name)
{
  PoolSP pool = fs->mPriv->getDataPoolFromName(poolName);
  size_t stripe = alignStripeSize(stripeSize, pool->alignment);

  if (pool)
    io = FileIOSP(new FileIO(fs, pool, name, stripe));
}

FileInodePriv::FileInodePriv(Filesystem *fs, PoolSP &pool,
                             const std::string &name, const size_t stripeSize)
  : fs(fs),
    name(name)
{
  size_t stripe = alignStripeSize(stripeSize, pool->alignment);

  if (pool)
    io = FileIOSP(new FileIO(fs, pool, name, stripe));
}

FileInodePriv::FileInodePriv(Filesystem *fs, FileIOSP fileIO)
  : fs(fs)
{
  setFileIO(fileIO);
}

FileInodePriv::~FileInodePriv()
{}

void
FileInodePriv::setFileIO(FileIOSP fileIO)
{
  io = fileIO;

  if (io)
    name = io->inode();
}

FileInode::FileInode(Filesystem *fs, const std::string &pool)
  : mPriv(new FileInodePriv(fs, pool, generateUuid(), fs->fileStripeSize()))
{}

FileInode::FileInode(Filesystem *fs, const std::string &name,
                     const std::string &pool)
  : mPriv(new FileInodePriv(fs, pool, name, fs->fileStripeSize()))
{}

FileInode::FileInode(Filesystem *fs, const std::string &name,
                     const std::string &pool, const size_t stripeSize)
  : mPriv(new FileInodePriv(fs, pool, name, stripeSize))
{}

FileInode::FileInode(Filesystem *fs, const std::string &pool,
                     const size_t stripeSize)
  : mPriv(new FileInodePriv(fs, pool, generateUuid(), stripeSize))
{}

FileInode::~FileInode()
{
  delete mPriv;
}

ssize_t
FileInode::read(char *buff, off_t offset, size_t blen)
{
  if (!mPriv->io)
    return -ENODEV;

  return mPriv->io->read(buff, offset, blen);
}

int
FileInode::write(const char *buff, off_t offset, size_t blen)
{
  return write(buff, offset, blen, false);
}

int
FileInode::write(const char *buff, off_t offset, size_t blen, bool copyBuffer)
{
  if (!mPriv->io)
    return -ENODEV;

  Stat stat;
  stat.pool = mPriv->io->pool();
  stat.translatedPath = mPriv->io->inode();

  updateTimeAsync(&stat, XATTR_MTIME);

  std::string opId;
  int ret = mPriv->io->write(buff, offset, blen, &opId, copyBuffer);

  {
    boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);
    mPriv->asyncOps.push_back(opId);
  }

  return ret;
}

int
FileInode::writeSync(const char *buff, off_t offset, size_t blen)
{
  if (!mPriv->io)
    return -ENODEV;

  Stat stat;
  stat.pool = mPriv->io->pool();
  stat.translatedPath = mPriv->io->inode();

  updateTimeAsync(&stat, XATTR_MTIME);

  return mPriv->io->writeSync(buff, offset, blen);
}

int
FileInode::remove(void)
{
  if (!mPriv->io)
    return -ENODEV;

  return mPriv->io->remove();
}

int
FileInode::truncate(size_t size)
{
  if (!mPriv->io)
    return -ENODEV;

  Stat stat;
  stat.pool = mPriv->io->pool();
  stat.translatedPath = mPriv->io->inode();

  updateTimeAsync(&stat, XATTR_MTIME);

  return mPriv->io->truncate(size);
}

int
FileInode::sync()
{
  if (!mPriv->io)
    return -ENODEV;

  int ret = 0;
  boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);

  std::vector<std::string>::iterator it;
  for (it = mPriv->asyncOps.begin(); it != mPriv->asyncOps.end(); it++)
  {
    ret = mPriv->io->sync(*it);
  }

  mPriv->asyncOps.clear();

  return ret;
}

std::string
FileInode::name() const
{
  if (!mPriv->io)
    return "";

  return mPriv->io->inode();
}

RADOS_FS_END_NAMESPACE
