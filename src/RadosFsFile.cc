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
#include "RadosFsFile.hh"
#include "RadosFsFilePriv.hh"
#include "RadosFsDir.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

FilePriv::FilePriv(File *fsFile, File::OpenMode mode)
  : fsFile(fsFile),
    target(0),
    permissions(File::MODE_NONE),
    mode(mode)
{
  updatePath();
}

FilePriv::~FilePriv()
{
  if (target)
    delete target;
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

  Fs *radosFs = fsFile->filesystem();

  mtdPool = radosFs->mPriv->getMetadataPoolFromPath(fsFile->path());

  if (!mtdPool)
    return;

  updatePermissions();

  fileIO.reset();

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
    fileIO = radosFs->mPriv->getOrCreateFileIO(stat->translatedPath, stat);
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
  if (!fileIO)
    return 0;

  if (!FileIO::hasSingleClient(fileIO))
    fileIO->setLazyRemoval(true);
  else
    return fileIO->remove();

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

  ret = indexObject(&parentStat, &stat, '+');

  if (ret != 0)
    return ret;

  Stat *oldParentStat =
      reinterpret_cast<Stat *>(fsFile->parentFsStat());

  stat.path = fsFile->path();
  ret = indexObject(oldParentStat, &stat, '-');

  if (ret != 0)
    return ret;

  fsFile->setPath(newPath);

  return ret;
}

File::File(Fs *radosFs, const std::string &path, File::OpenMode mode)
  : Info(radosFs, getFilePath(path)),
    mPriv(new FilePriv(this, mode))
{}

File::~File()
{}

File::File(const File &otherFile)
  : Info(otherFile),
    mPriv(new FilePriv(this, otherFile.mode()))
{}

File::File(const File *otherFile)
  : Info(*otherFile),
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

    ret = mPriv->fileIO->read(buff, offset, blen);
  }

  return ret;
}

int
File::write(const char *buff, off_t offset, size_t blen)
{
  return write(buff, offset, blen, false);
}

int
File::write(const char *buff, off_t offset, size_t blen, bool copyBuffer)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & File::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->write(buff, offset, blen, copyBuffer);

    updateTimeAsync(mPriv->fsStat(), XATTR_MTIME);

    std::string opId;
    ret = mPriv->fileIO->write(buff, offset, blen, &opId, copyBuffer);

    {
      boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);
      mPriv->asyncOps.push_back(opId);
    }

    return ret;
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

    updateTimeAsync(mPriv->fsStat(), XATTR_MTIME);

    return mPriv->fileIO->writeSync(buff, offset, blen);
  }

  return -EACCES;
}

int
File::create(int mode, const std::string pool, size_t stripe)
{
  Stat *stat = reinterpret_cast<Stat *>(fsStat());
  Stat *parentStat = reinterpret_cast<Stat *>(parentFsStat());
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
    if (mPriv->fileIO && mPriv->fileIO->lazyRemoval())
    {
      mPriv->fileIO->setLazyRemoval(false);
    }
    else
    {
      return -EEXIST;
    }
  }

  if ((mPriv->permissions & File::MODE_WRITE) == 0)
    return -EACCES;

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  long int permOctal = DEFAULT_MODE_FILE;

  if (mode >= 0)
    permOctal = mode | S_IFREG;

  mPriv->inode = generateUuid();
  stat->path = path();
  stat->translatedPath = mPriv->inode;
  stat->statBuff.st_mode = permOctal;
  stat->statBuff.st_uid = uid;
  stat->statBuff.st_gid = gid;
  stat->pool = mPriv->dataPool;

  timespec spec;
  clock_gettime(CLOCK_REALTIME, &spec);

  stat->statBuff.st_ctim = spec;
  stat->statBuff.st_ctime = spec.tv_sec;

  std::stringstream stream;
  stream << alignStripeSize((stripe) ? stripe : filesystem()->fileStripeSize(),
                            mPriv->dataPool->alignment);

  stat->extraData[XATTR_FILE_STRIPE_SIZE] = stream.str();

  ret = indexObject(parentStat, stat, '+');

  if (ret == -ECANCELED)
    return -EEXIST;

  update();

  return ret;
}

int
File::remove()
{
  int ret;

  Info::update();

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

  Info::update();

  return ret;
}

int
File::truncate(unsigned long long size)
{
  if (isLink())
    return mPriv->target->truncate(size);

  if (mPriv->fileIO)
  {
    updateTimeAsync(mPriv->fsStat(), XATTR_MTIME);

    return mPriv->fileIO->truncate(size);
  }

  return -ENODEV;
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
  Info::update();
  mPriv->updatePath();
}

void
File::setPath(const std::string &path)
{
  std::string filePath = getFilePath(path);

  Info::setPath(filePath);
}

int
File::stat(struct stat *buff)
{
  Stat *stat;

  Info::update();

  if (!exists())
    return -ENOENT;

  stat = mPriv->fsStat();

  getTimeFromXAttr(stat, XATTR_MTIME, &stat->statBuff.st_mtim,
                   &stat->statBuff.st_mtime);

  *buff = stat->statBuff;

  if (isLink())
    return 0;

  buff->st_size = mPriv->fileIO->getSize();

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
File::sync()
{
  int ret = 0;
  boost::unique_lock<boost::mutex> lock(mPriv->asyncOpsMutex);

  std::vector<std::string>::iterator it;
  for (it = mPriv->asyncOps.begin(); it != mPriv->asyncOps.end(); it++)
  {
    ret = mPriv->fileIO->sync(*it);
  }

  mPriv->asyncOps.clear();

  return ret;
}

RADOS_FS_END_NAMESPACE
