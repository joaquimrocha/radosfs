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

#include "radosfsdefines.h"
#include "radosfscommon.h"
#include "File.hh"
#include "FileInode.hh"
#include "FileInodePriv.hh"
#include "FilePriv.hh"
#include "Dir.hh"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

FilePriv::FilePriv(File *fsFile, File::OpenMode mode)
  : fsFile(fsFile),
    target(0),
    permissions(File::MODE_NONE),
    mode(mode),
    inlineBufferSize(DEFAULT_FILE_INLINE_BUFFER_SIZE)
{
  FileInodePriv *mPriv = new FileInodePriv(fsFile->filesystem(), FileIOSP());
  inode = new FileInode(mPriv);

  updatePath();
}

FilePriv::~FilePriv()
{
  if (target)
    delete target;

  delete inode;
}

void
FilePriv::updateDataPool(const std::string &pool)
{
  dataPool = fsFile->filesystem()->mPriv->getDataPool(fsFile->path(), pool);
}

void
FilePriv::updatePath()
{
  Stat *stat = fsStat();

  parentDir = getParentDir(fsFile->path(), 0);

  Filesystem *radosFs = fsFile->filesystem();

  mtdPool = radosFs->mPriv->getMetadataPoolFromPath(fsFile->path());

  if (!mtdPool)
    return;

  updatePermissions();

  inode->mPriv->io.reset();

  if (!fsFile->exists())
  {
    updateDataPool("");

    return;
  }

  // Only set the pool if they differ. Besides saving it from wasting time in
  // getting the same assignments, getting the alignment when the data pool
  // differs allows us to use it directly in the unit tests to fake an arbitrary
  // alignment.
  if (stat->pool.get() != dataPool.get())
  {
    dataPool = stat->pool;
  }

  if (stat->extraData.count(XATTR_FILE_INLINE_BUFFER_SIZE) > 0)
  {
    const std::string &bufferSize =
        stat->extraData[XATTR_FILE_INLINE_BUFFER_SIZE];
    inlineBufferSize = strtoul(bufferSize.c_str(), 0, 10);
  }

  if (!dataPool)
    return;

  if (target)
  {
    delete target;
    target = 0;
  }

  if (fsFile->isLink())
  {
    target = new File(fsFile->filesystem(),
                      fsFile->targetPath(),
                      mode);
  }
  else if (stat && stat->translatedPath != "")
  {
    inode->mPriv->setFileIO(radosFs->mPriv->getOrCreateFileIO(stat->translatedPath,
                                                              stat));
    if (inlineBufferSize > 0)
      inode->mPriv->io->setInlineBuffer(fsFile->path(), inlineBufferSize);
  }
}

int
FilePriv::verifyExistanceAndType()
{
  if (fsFile->isLink() && !target->exists())
    return -ENOLINK;

  if (!fsFile->exists())
    return -ENOENT;

  if (!fsFile->isFile())
    return -EISDIR;

  return 0;
}

void
FilePriv::updatePermissions()
{
  permissions = File::MODE_NONE;
  Stat stat, *fileStat;

  if (!mtdPool.get())
    return;

  int ret = fsFile->filesystem()->mPriv->stat(parentDir, &stat);
  parentDir = stat.path;

  if (ret != 0)
    return;

  uid_t uid;
  gid_t gid;

  fsFile->filesystem()->getIds(&uid, &gid);

  bool canWriteParent =
      statBuffHasPermission(stat.statBuff, uid, gid, O_WRONLY);

  bool canReadParent =
      statBuffHasPermission(stat.statBuff, uid, gid, O_RDONLY);

  fileStat = fsStat();

  if (fsFile->exists() && !fsFile->isFile())
    return;

  if (canWriteParent && (mode & File::MODE_WRITE))
  {
    if (!fsFile->exists() ||
        statBuffHasPermission(fileStat->statBuff, uid, gid, O_WRONLY))
      permissions =
          (File::OpenMode) (permissions | File::MODE_WRITE);
  }

  if (canReadParent && (mode & File::MODE_READ) &&
      statBuffHasPermission(fileStat->statBuff, uid, gid, O_RDONLY))
  {
    permissions = (File::OpenMode) (permissions | File::MODE_READ);
  }
}

int
FilePriv::removeFile()
{
  if (!getFileIO())
    return 0;

  if (!FileIO::hasSingleClient(inode->mPriv->io))
    getFileIO()->setLazyRemoval(true);
  else
  {
    int ret = getFileIO()->remove();

    // Ignore the fact that the inode might not exist because we're also dealing
    // with the logical file name
    if (ret == -ENOENT)
      return 0;
  }

  return 0;
}

Stat *
FilePriv::fsStat(void)
{
  return reinterpret_cast<Stat *>(fsFile->fsStat());
}

int
FilePriv::rename(const std::string &destination)
{
  int index;
  int ret;
  Stat stat, parentStat;
  std::string destParent = getParentDir(destination, &index);
  std::string baseName;

  if (destParent != "")
  {
    baseName = destination.substr(index);
  }

  if (destParent == parentDir)
  {
    parentStat = *reinterpret_cast<Stat *>(fsFile->parentFsStat());
  }
  else
  {
    uid_t uid;
    gid_t gid;
    std::string realParentPath;

    ret = fsFile->filesystem()->mPriv->getRealPath(destParent, &parentStat,
                                                   realParentPath);

    if (ret == 0 && S_ISLNK(parentStat.statBuff.st_mode))
    {
      destParent = parentStat.translatedPath;
      ret = fsFile->filesystem()->mPriv->stat(destParent,
                                              &parentStat);
    }
    else
    {
      destParent = realParentPath;
    }

    if (ret != 0)
    {
      radosfs_debug("Problem statting destination's parent when moving %s: %s",
                    destination.c_str(), strerror(-ret));
      return ret;
    }

    fsFile->filesystem()->getIds(&uid, &gid);

    if (!statBuffHasPermission(parentStat.statBuff, uid, gid, O_WRONLY))
    {
      radosfs_debug("No permissions to write in parent dir when moving %s",
                    destination.c_str());
      return -EACCES;
    }
  }

  const std::string &newPath = destParent + baseName;

  ret = fsFile->filesystem()->mPriv->stat(newPath, &stat);

  if (ret == 0)
  {
    if (S_ISDIR(stat.statBuff.st_mode))
      return -EISDIR;

    if (newPath == fsFile->path())
      return -EPERM;
  }
  else if (ret != -ENOENT)
  {
    return ret;
  }

  stat = *fsStat();
  stat.path = newPath;

  Stat *oldParentStat = reinterpret_cast<Stat *>(fsFile->parentFsStat());

  ret = moveLogicalFile(*oldParentStat, parentStat, fsFile->path(), newPath);

  if (ret != 0)
    return ret;

  inode->mPriv->io->setPath(newPath);

  std::string oldPath = fsFile->path();
  inode->mPriv->io->updateBackLink(&oldPath);

  fsFile->setPath(newPath);

  return ret;
}

void
FilePriv::setInode(const size_t stripeSize)
{
  size_t stripe = alignStripeSize(stripeSize, dataPool->alignment);
  FileIOSP fileIO = FileIOSP(new FileIO(fsFile->filesystem(),
                                        dataPool,
                                        generateUuid(),
                                        fsFile->path(),
                                        stripe));
  inode->mPriv->setFileIO(fileIO);

  if (inlineBufferSize > 0)
    inode->mPriv->io->setInlineBuffer(fsFile->path(), inlineBufferSize);
}

int
FilePriv::create(int mode, uid_t uid, gid_t gid, size_t stripe)
{
  setInode(stripe ? stripe : fsFile->filesystem()->fileStripeSize());

  return inode->mPriv->registerFile(fsFile->path(), uid, gid, mode,
                                    inlineBufferSize);
}

File::File(Filesystem *radosFs, const std::string &path, File::OpenMode mode)
  : FsObj(radosFs, getFilePath(path)),
    mPriv(new FilePriv(this, mode))
{}

File::~File()
{}

File::File(const File &otherFile)
  : FsObj(otherFile),
    mPriv(new FilePriv(this, otherFile.mode()))
{}

File::File(const File *otherFile)
  : FsObj(*otherFile),
    mPriv(new FilePriv(this, otherFile->mode()))
{}

File &
File::operator=(const File &otherFile)
{
  if (this != &otherFile)
  {
    this->mPriv->mode = otherFile.mode();
    this->setPath(otherFile.path());
  }

  return *this;
}

File::OpenMode
File::mode() const
{
  return mPriv->mode;
}

ssize_t
File::read(char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  ret = -EACCES;

  if (mPriv->permissions & File::MODE_READ)
  {
    if (isLink())
      return mPriv->target->read(buff, offset, blen);

    ret = mPriv->inode->read(buff, offset, blen);

    // If there is no inode object, we treat the file as having a size of 0
    // instead
    if (ret == -ENOENT)
    {
      ret = 0;
    }
  }

  return ret;
}

int
File::read(const std::vector<FileReadData> &intervals, std::string *asyncOpId,
           AsyncOpCallback callback, void *callbackArg)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  ret = -EACCES;

  if (mPriv->permissions & File::MODE_READ)
  {
    if (isLink())
      return mPriv->target->read(intervals, asyncOpId, callback, callbackArg);

    ret = mPriv->inode->read(intervals, asyncOpId, callback, callbackArg);
  }

  return ret;
}

int
File::write(const char *buff, off_t offset, size_t blen, bool copyBuffer,
            std::string *asyncOpId, AsyncOpCallback callback,
            void *callbackArg)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & File::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->write(buff, offset, blen, copyBuffer, asyncOpId,
                                  callback, callbackArg);

    return mPriv->inode->write(buff, offset, blen, copyBuffer, asyncOpId,
                               callback, callbackArg);
  }

  return -EACCES;
}

int
File::writeSync(const char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & File::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->writeSync(buff, offset, blen);

    return mPriv->inode->writeSync(buff, offset, blen);
  }

  return -EACCES;
}

int
File::create(int mode, const std::string pool, size_t stripe,
             ssize_t inlineBufferSize)
{
  int ret;

  if (pool != "")
    mPriv->updateDataPool(pool);

  if (mPriv->dataPool.get() == 0)
    return -ENODEV;

  // we don't allow object names that end in a path separator
  const std::string filePath = path();
  if ((exists() && !isFile()) ||
      (filePath != "" && isDirPath(filePath)))
    return -EISDIR;

  if (exists())
  {
    if (mPriv->getFileIO() && mPriv->getFileIO()->lazyRemoval())
    {
      mPriv->getFileIO()->setLazyRemoval(false);
    }
    else
    {
      return -EEXIST;
    }
  }

  if ((mPriv->permissions & File::MODE_WRITE) == 0)
    return -EACCES;

  if (inlineBufferSize > MAX_FILE_INLINE_BUFFER_SIZE)
  {
    radosfs_debug("Error: Cannot create a file with an inline size > %u. The "
                  "given size was %u.", MAX_FILE_INLINE_BUFFER_SIZE,
                  inlineBufferSize);
    return -EINVAL;
  }

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  if (inlineBufferSize > -1)
    mPriv->inlineBufferSize = inlineBufferSize;

  ret = mPriv->create(mode, uid, gid, stripe);

  if (ret < 0)
    return ret;

  update();

  return ret;
}

int
File::remove()
{
  int ret;

  FsObj::update();

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  uid_t uid;
  gid_t gid;

  Stat *stat = mPriv->fsStat();

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(stat->statBuff, uid, gid,
                            O_WRONLY | O_RDWR))
  {
    ret = mPriv->removeFile();
    Stat *parentStat = reinterpret_cast<Stat *>(parentFsStat());
    indexObject(parentStat, stat, '-');
  }
  else
    return -EACCES;

  FsObj::update();

  return ret;
}

int
File::truncate(unsigned long long size)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (isLink())
    return mPriv->target->truncate(size);

  uid_t uid;
  gid_t gid;
  Stat *stat = mPriv->fsStat();

  filesystem()->getIds(&uid, &gid);

  if (!statBuffHasPermission(stat->statBuff, uid, gid,
                             O_WRONLY | O_RDWR))
  {
    return -EACCES;
  }

  return mPriv->inode->truncate(size);
}

bool
File::isWritable()
{
  return (mPriv->permissions & File::MODE_WRITE) != 0;
}

bool
File::isReadable()
{
  return (mPriv->permissions & File::MODE_READ) != 0;
}

void
File::update()
{
  FsObj::update();
  mPriv->updatePath();
}

void
File::setPath(const std::string &path)
{
  std::string filePath = getFilePath(path);

  FsObj::setPath(filePath);
}

int
File::stat(struct stat *buff)
{
  Stat *stat;

  FsObj::update();

  if (!exists())
    return -ENOENT;

  stat = mPriv->fsStat();


  if (isLink())
  {
    getTimeFromXAttr(stat, XATTR_MTIME, &stat->statBuff.st_mtim,
                     &stat->statBuff.st_mtime);
  }
  else
  {
    // If there is an inline buffer and it is not fully filled, then it already
    // gives us the size of the object, otherwise, we need to check the size set
    // in the inode

    std::string inlineContents;
    FileInlineBuffer *inlineBuffer = mPriv->getFileIO()->inlineBuffer();

    if (inlineBuffer)
    {
      inlineBuffer->read(&stat->statBuff.st_mtim, &inlineContents);

      stat->statBuff.st_mtime = stat->statBuff.st_mtim.tv_sec;
      stat->statBuff.st_size = inlineContents.length();
    }

    if (!inlineBuffer || inlineContents.length() == inlineBuffer->capacity())
    {
      size_t fileIOSize = mPriv->getFileIO()->getSize();

      if (fileIOSize != 0)
        stat->statBuff.st_size = fileIOSize;

      getTimeFromXAttr(stat, XATTR_MTIME, &stat->statBuff.st_mtim,
                       &stat->statBuff.st_mtime);
    }
  }

  *buff = stat->statBuff;

  return 0;
}

int
File::chmod(long int permissions)
{
  long int mode;

  if (!exists())
    return -ENOENT;

  mode = permissions | S_IFREG;

  Stat fsStat = *mPriv->fsStat();
  Stat *parentStat = reinterpret_cast<Stat *>(parentFsStat());

  uid_t uid = filesystem()->uid();

  if (uid != ROOT_UID && fsStat.statBuff.st_uid != uid)
    return -EPERM;

  fsStat.statBuff.st_mode = mode;
  const std::string &baseName = path().substr(mPriv->parentDir.length());
  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_FILE_PREFIX + baseName].append(getFileXAttrDirRecord(&fsStat));

  return mPriv->mtdPool->ioctx.omap_set(parentStat->translatedPath, omap);
}

int
File::rename(const std::string &newPath)
{
  int ret;

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  if (newPath == "")
    return -EINVAL;

  if (!isWritable())
    return -EACCES;

  std::string dest = newPath;

  if (dest[0] != PATH_SEP)
  {
    dest = getParentDir(path(), 0) + dest;
  }

  if (dest == "/")
    return -EISDIR;

  dest = getFilePath(sanitizePath(dest));

  return mPriv->rename(dest);
}

int
File::sync(const std::string &opId)
{
  int ret = mPriv->inode->sync(opId);

  if (ret == -ENODEV)
    ret = 0;

  return ret;
}

size_t
File::inlineBufferSize(void) const
{
  return mPriv->inlineBufferSize;
}

RADOS_FS_END_NAMESPACE
