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

int
FileInodePriv::registerFile(const std::string &path, uid_t uid, gid_t gid,
                            int mode, size_t inlineBufferSize)
{
  const std::string parentDir = getParentDir(path, 0);
  std::string filePath = path;

  if (parentDir == "")
  {
    radosfs_debug("Error registering inode %s with file path %s . The file "
                  "path needs to be absolute (start with a '/').", name.c_str(),
                  path.c_str());
    return -EINVAL;
  }

  Stat parentStat;

  int ret = fs->mPriv->stat(parentDir, &parentStat);

  if (ret < 0)
  {
    if (ret == -EEXIST)
      radosfs_debug("Cannot register inode %s in path %s: The parent directory "
                    "does not exist. (Verify that the path is an absolute path "
                    "without any links in it)", name.c_str(), filePath.c_str());
    return ret;
  }

  if (S_ISLNK(parentStat.statBuff.st_mode))
  {
    radosfs_debug("Cannot register inode %s in path %s: Be sure to provide an "
                  "existing absolute path containing no links.", name.c_str(),
                  filePath.c_str());
    return -EINVAL;
  }

  if (!S_ISDIR(parentStat.statBuff.st_mode))
  {
    radosfs_debug("Error registering inode %s with file path %s . The parent "
                  "directory is a regular file.", name.c_str(),
                  filePath.c_str());
    return -EINVAL;
  }

  Stat fileStat;
  ret = fs->mPriv->stat(filePath, &fileStat);

  if (ret == 0)
    return -EEXIST;

  long int permOctal = DEFAULT_MODE_FILE;

  if (mode >= 0)
    permOctal = mode | S_IFREG;

  timespec spec;
  clock_gettime(CLOCK_REALTIME, &spec);

  fileStat.path = filePath;
  fileStat.translatedPath = io->inode();
  fileStat.pool = io->pool();
  fileStat.statBuff = parentStat.statBuff;
  fileStat.statBuff.st_uid = uid;
  fileStat.statBuff.st_gid = gid;
  fileStat.statBuff.st_mode = permOctal;
  fileStat.statBuff.st_ctim = spec;
  fileStat.statBuff.st_ctime = spec.tv_sec;

  std::stringstream stream;
  stream << io->stripeSize();

  fileStat.extraData[XATTR_FILE_STRIPE_SIZE] = stream.str();

  stream.str("");

  stream << inlineBufferSize;
  fileStat.extraData[XATTR_FILE_INLINE_BUFFER_SIZE] = stream.str();

  ret = indexObject(&parentStat, &fileStat, '+');

  if (ret == -ECANCELED)
  {
    return -EEXIST;
  }

  return ret;
}

int
FileInodePriv::setBackLink(const std::string &backLink)
{
  librados::bufferlist buff;
  buff.append(backLink);

  return io->pool()->ioctx.setxattr(io->inode(), XATTR_INODE_HARD_LINK, buff);
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

FileInode::FileInode(FileInodePriv *priv)
  : mPriv(priv)
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
FileInode::read(const std::vector<FileReadData> &intervals,
                std::string *asyncOpId)
{
  std::string opId;

  int ret = mPriv->io->read(intervals, &opId);

  {
    boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);
    mPriv->asyncOps.push_back(opId);
  }

  if (asyncOpId)
    asyncOpId->assign(opId);

  return ret;
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

  return mPriv->io->truncate(size);
}

int
FileInode::sync(const std::string &opId)
{
  if (!mPriv->io)
    return -ENODEV;

  int ret = 0;
  boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);

  std::vector<std::string>::iterator it;
  for (it = mPriv->asyncOps.begin(); it != mPriv->asyncOps.end(); it++)
  {
    const std::string &currentOpId = *it;

    // Single op sync
    if (!opId.empty())
    {
      if (currentOpId == opId)
      {
        ret = mPriv->io->sync(currentOpId);
        mPriv->asyncOps.erase(it);
        break;
      }

      continue;
    }

    ret = mPriv->io->sync(currentOpId);
  }

  if (opId.empty())
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

int
FileInode::registerFile(const std::string &path, uid_t uid, gid_t gid, int mode)
{
  if (!mPriv->io)
    return -ENODEV;

  if (path == "")
  {
    radosfs_debug("Error: path for registering inode %s is empty",
                  mPriv->name.c_str());
    return -EINVAL;
  }

  if (isDirPath(path))
  {
    radosfs_debug("Error attempting to register inode %s with directory path "
                  "%s. For registering an inode, it needs to be a file path "
                  "(no '/' in the end of it).", mPriv->name.c_str(),
                  path.c_str());

    return -EISDIR;
  }

  int ret = mPriv->registerFile(path, uid, gid, mode);

  if (ret < 0)
  {
    radosfs_debug("Could register the file '%s' with inode '%s': %s (retcode=%d) ",
                  path.c_str(), mPriv->name.c_str(), strerror(abs(ret)), ret);

    return ret;
  }

  ret = mPriv->setBackLink(path);

  if (ret < 0)
  {
    radosfs_debug("Could not set backlink '%s' on inode '%s': %s (retcode=%d) ",
                  path.c_str(), mPriv->name.c_str(), strerror(abs(ret)), ret);
  }

  return ret;
}

int
FileInode::getBackLink(std::string *backLink)
{
  librados::bufferlist buff;

  int ret = mPriv->io->pool()->ioctx.getxattr(name(), XATTR_INODE_HARD_LINK,
                                              buff);

  if (ret >= 0)
  {
    backLink->assign(buff.c_str(), 0, buff.length());
    return 0;
  }

  return ret;
}

RADOS_FS_END_NAMESPACE
