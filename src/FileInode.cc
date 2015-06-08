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
                             const std::string &name, const size_t chunkSize)
  : fs(fs),
    name(name)
{
  PoolSP pool = fs->mPriv->getDataPoolFromName(poolName);
  size_t chunk = alignChunkSize(chunkSize, pool->alignment);

  if (pool)
    io = FileIOSP(new FileIO(fs, pool, name, chunk));
}

FileInodePriv::FileInodePriv(Filesystem *fs, PoolSP &pool,
                             const std::string &name, const size_t chunkSize)
  : fs(fs),
    name(name)
{
  size_t chunk = alignChunkSize(chunkSize, pool->alignment);

  if (pool)
    io = FileIOSP(new FileIO(fs, pool, name, chunk));
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
  stream << io->chunkSize();

  fileStat.extraData[XATTR_FILE_CHUNK_SIZE] = stream.str();

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

/**
 * @class FileInode
 *
 * Represents a file inode in the filesystem.
 *
 * A file inode is the object in the filesystem where a file's data is
 * effectively stored.
 *
 * This class is used to decouple some operations related to the file data
 * from the File class for more flexibility.
 *
 * A common use case for this class is to create an inode, write its contents,
 * and only after that register a logical file name pointing to it.
 */

/**
 * Creates an new instance of FileInode with an automatically generated name.
 *
 * @param fs a pointer to the Filesystem that contains this file.
 * @param pool the pool where the file inode should be created.
 */
FileInode::FileInode(Filesystem *fs, const std::string &pool)
  : mPriv(new FileInodePriv(fs, pool, generateUuid(), fs->fileChunkSize()))
{}

/**
 * Creates an new instance of FileInode with the given \a name.
 *
 * @param fs a pointer to the Filesystem that contains this file.
 * @param name the name for this file inode.
 * @param pool the pool where the file inode should be created.
 */
FileInode::FileInode(Filesystem *fs, const std::string &name,
                     const std::string &pool)
  : mPriv(new FileInodePriv(fs, pool, name, fs->fileChunkSize()))
{}

/**
 * Creates an new instance of FileInode with the given \a name and \a stripeSize.
 *
 * @param fs a pointer to the Filesystem that contains this file.
 * @param name the name for this file inode.
 * @param pool the pool where the file inode should be created.
 * @param stripeSize the stripe size to be used by this file inode.
 */
FileInode::FileInode(Filesystem *fs, const std::string &name,
                     const std::string &pool, const size_t chunkSize)
  : mPriv(new FileInodePriv(fs, pool, name, chunkSize))
{}

/**
 * Creates an new instance of FileInode with an automatically generated name and
 * the given \a stripeSize.
 *
 * @param fs a pointer to the Filesystem that contains this file.
 * @param pool the pool where the file inode should be created.
 * @param stripeSize the stripe size to be used by this file inode.
 */
FileInode::FileInode(Filesystem *fs, const std::string &pool,
                     const size_t chunkSize)
  : mPriv(new FileInodePriv(fs, pool, generateUuid(), chunkSize))
{}

FileInode::FileInode(FileInodePriv *priv)
  : mPriv(priv)
{}

FileInode::~FileInode()
{
  delete mPriv;
}

/**
 * Reads data from the file inode (synchronously).
 *
 * @param[out] buff the buffer in which to store the data read.
 * @param offset the offset of the inode where the data should start being read.
 * @param blen the number of bytes to read.
 * @return the number of bytes read on success, a negative error code otherwise.
 */
ssize_t
FileInode::read(char *buff, off_t offset, size_t blen)
{
  if (!mPriv->io)
    return -ENODEV;

  return mPriv->io->read(buff, offset, blen);
}

/**
 * Reads data from the file inode asynchronously and in parallel.
 *
 * @param intervals a vector of FileReadData objects indicating what should be
 *        read.
 * @param[out] asyncOpId a string location to return the id of the asynchonous
 *             operation (or a null pointer in case none should be returned).
 * @param callback a function to be called upon the end of the read operation.
 * @param callbackArg a pointer representing user-defined argumetns, to be
 *        passed to the \a callback.
 * @return 0 if the operation was initialized, an error code otherwise.
 */
int
FileInode::read(const std::vector<FileReadData> &intervals,
                std::string *asyncOpId, AsyncOpCallback callback,
                void *callbackArg)
{
  std::string opId;

  int ret = mPriv->io->read(intervals, &opId, callback, callbackArg);

  {
    boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);
    mPriv->asyncOps.push_back(opId);
  }

  if (asyncOpId)
    asyncOpId->assign(opId);

  return ret;
}

/**
 * Write the contens of \a buff to the file inode asynchronously.
 *
 * @param buff a char buffer.
 * @param offset the offset in the file inode to write the new contents.
 * @param blen the number of bytes (from the \a buff) that should be written.
 * @param copyBuffer whether \a buff should be copied or not.
 * @param asyncOpId a string location to return the asynchronous operation's
 *        address or a null pointer if this is not desired.
 * @param callback an AsyncOpCallback to be called after the asynchronous
 *        operation is finished
 * @param callbackArg a pointer to the user arguments that will be passed to the
 *        \a callback .
 * @return 0 if the operation was initialized, an error code otherwise.

 */
int
FileInode::write(const char *buff, off_t offset, size_t blen, bool copyBuffer,
                 std::string *asyncOpId, AsyncOpCallback callback,
                 void *callbackArg)
{
  if (!mPriv->io)
    return -ENODEV;

  Stat stat;
  stat.pool = mPriv->io->pool();
  stat.translatedPath = mPriv->io->inode();

  std::string opId;
  int ret = mPriv->io->write(buff, offset, blen, &opId, copyBuffer, callback,
                             callbackArg);

  if (asyncOpId)
    asyncOpId->assign(opId);

  {
    boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);
    mPriv->asyncOps.push_back(opId);
  }

  return ret;
}

/**
 * Write the contens of \a buff to the file inode synchronously.
 *
 * @param buff a char buffer.
 * @param offset the offset in the file inode to write the new contents.
 * @param blen the number of bytes (from the \a buff) that should be written.
 * @return 0 on success, an error code otherwise.
 */
int
FileInode::writeSync(const char *buff, off_t offset, size_t blen)
{
  if (!mPriv->io)
    return -ENODEV;

  return mPriv->io->writeSync(buff, offset, blen);
}

/**
 * Removes the file inode.
 *
 * @return 0 on success, an error code otherwise.
 */
int
FileInode::remove(void)
{
  if (!mPriv->io)
    return -ENODEV;

  return mPriv->io->remove();
}

/**
 * Truncates the file inode.
 *
 * @param size the new size for the file inode.
 * @return 0 on success, an error code otherwise.
 */
int
FileInode::truncate(size_t size)
{
  if (!mPriv->io)
    return -ENODEV;

  return mPriv->io->truncate(size);
}

/**
 * Waits for the file inode's asynchronous operations to be finished. If \a opId
 * is given, it waits only for the operation with the matching id, otherwise it
 * does it for every single operation.
 *
 * @param opId the id of an asynchronous operation.
 * @return 0 on success, an error code otherwise.
 */
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

/**
 * Returns the name of the file inode.
 *
 * @return the name of the file inode.
 */
std::string
FileInode::name() const
{
  if (!mPriv->io)
    return "";

  return mPriv->io->inode();
}

/**
 * Attaches this file inode with a logical path making the inode visible to the
 * traditional file operations (listing from a directory or accessing the file
 * through a File instance).
 *
 * @param path the logical and absolute path to register with the file inode.
 * @param uid the file owner's uid.
 * @param gid the file owner's gid.
 * @param mode the permissions of the file.
 * @return 0 on success, an error code otherwise.
 */
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

/**
 * Gets the logical path associated with this file inode (if any).
 *
 * @param[out] backLink a string location to store the back link.
 * @return 0 on success, an error code otherwise.
 */
int
FileInode::getBackLink(std::string *backLink)
{
  return getFileInodeBackLink(mPriv->io->pool().get(), name(), backLink);
}

RADOS_FS_END_NAMESPACE
