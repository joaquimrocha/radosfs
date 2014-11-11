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

RadosFsFilePriv::RadosFsFilePriv(RadosFsFile *fsFile,
                                 RadosFsFile::OpenMode mode)
  : fsFile(fsFile),
    target(0),
    permissions(RadosFsFile::MODE_NONE),
    mode(mode)
{
  updatePath();
}

RadosFsFilePriv::~RadosFsFilePriv()
{
  if (target)
    delete target;

  if (radosFsIO.get() && radosFsIO.use_count() == 1)
  {
    fsFile->filesystem()->mPriv->removeRadosFsIO(radosFsIO);
    radosFsIO.reset();
  }
}

void
RadosFsFilePriv::updateDataPool(const std::string &pool)
{
  dataPool = fsFile->filesystem()->mPriv->getDataPool(fsFile->path(), pool);
}

void
RadosFsFilePriv::updatePath()
{
  RadosFsStat *stat = fsStat();

  parentDir = getParentDir(fsFile->path(), 0);

  RadosFs *radosFs = fsFile->filesystem();

  mtdPool = radosFs->mPriv->getMetadataPoolFromPath(fsFile->path());

  if (!mtdPool)
    return;

  updatePermissions();

  radosFsIO.reset();

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
    target = new RadosFsFile(fsFile->filesystem(),
                             fsFile->targetPath(),
                             mode);
  }
  else if (stat && stat->translatedPath != "")
  {
    radosFsIO = radosFs->mPriv->getOrCreateFsIO(stat->translatedPath, stat);
  }
}

int
RadosFsFilePriv::verifyExistanceAndType()
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
RadosFsFilePriv::updatePermissions()
{
  permissions = RadosFsFile::MODE_NONE;
  RadosFsStat stat, *fileStat;

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

  if (canWriteParent && (mode & RadosFsFile::MODE_WRITE))
  {
    if (!fsFile->exists() ||
        statBuffHasPermission(fileStat->statBuff, uid, gid, O_WRONLY))
      permissions =
          (RadosFsFile::OpenMode) (permissions | RadosFsFile::MODE_WRITE);
  }

  if (canReadParent && (mode & RadosFsFile::MODE_READ) &&
      statBuffHasPermission(fileStat->statBuff, uid, gid, O_RDONLY))
  {
    permissions = (RadosFsFile::OpenMode) (permissions | RadosFsFile::MODE_READ);
  }
}

int
RadosFsFilePriv::removeFile()
{
  if (!radosFsIO)
    return 0;

  if (radosFsIO.use_count() > 1)
    radosFsIO->setLazyRemoval(true);
  else
    return radosFsIO->remove(true);

  return 0;
}

RadosFsStat *
RadosFsFilePriv::fsStat(void)
{
  return reinterpret_cast<RadosFsStat *>(fsFile->fsStat());
}

int
RadosFsFilePriv::rename(const std::string &destination)
{
  int index;
  int ret;
  RadosFsStat stat, parentStat;
  std::string destParent = getParentDir(destination, &index);
  std::string baseName;

  if (destParent != "")
  {
    baseName = destination.substr(index);
  }

  if (destParent == parentDir)
  {
    parentStat = *reinterpret_cast<RadosFsStat *>(fsFile->parentFsStat());
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

  RadosFsStat *oldParentStat =
      reinterpret_cast<RadosFsStat *>(fsFile->parentFsStat());

  stat.path = fsFile->path();
  ret = indexObject(oldParentStat, &stat, '-');

  if (ret != 0)
    return ret;

  fsFile->setPath(newPath);

  return ret;
}

RadosFsFile::RadosFsFile(RadosFs *radosFs,
                         const std::string &path,
                         RadosFsFile::OpenMode mode)
  : RadosFsInfo(radosFs, getFilePath(path)),
    mPriv(new RadosFsFilePriv(this, mode))
{}

RadosFsFile::~RadosFsFile()
{}

RadosFsFile::RadosFsFile(const RadosFsFile &otherFile)
  : RadosFsInfo(otherFile),
    mPriv(new RadosFsFilePriv(this, otherFile.mode()))
{}

RadosFsFile::RadosFsFile(const RadosFsFile *otherFile)
  : RadosFsInfo(*otherFile),
    mPriv(new RadosFsFilePriv(this, otherFile->mode()))
{}

RadosFsFile &
RadosFsFile::operator=(const RadosFsFile &otherFile)
{
  if (this != &otherFile)
  {
    this->mPriv->mode = otherFile.mode();
    this->setPath(otherFile.path());
  }

  return *this;
}

RadosFsFile::OpenMode
RadosFsFile::mode() const
{
  return mPriv->mode;
}

ssize_t
RadosFsFile::read(char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  ret = -EACCES;

  if (mPriv->permissions & RadosFsFile::MODE_READ)
  {
    if (isLink())
      return mPriv->target->read(buff, offset, blen);

    ret = mPriv->radosFsIO->read(buff, offset, blen);
  }

  // Since it only creates the inode object when a write (or setXAttr) operation
  // is needed, if the file exists but reading it returns -ENOENT, it should
  // rather return 0 (it should act as if the file was empty).
  if (ret == -ENOENT && exists())
    ret = 0;

  return ret;
}

int
RadosFsFile::write(const char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & RadosFsFile::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->write(buff, offset, blen);

    updateTimeAsync(mPriv->fsStat(), XATTR_MTIME);

    return mPriv->radosFsIO->write(buff, offset, blen);
  }

  return -EACCES;
}

int
RadosFsFile::writeSync(const char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & RadosFsFile::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->writeSync(buff, offset, blen);

    updateTimeAsync(mPriv->fsStat(), XATTR_MTIME);

    return mPriv->radosFsIO->writeSync(buff, offset, blen);
  }

  return -EACCES;
}

int
RadosFsFile::create(int mode, const std::string pool, size_t stripe)
{
  RadosFsStat *stat = reinterpret_cast<RadosFsStat *>(fsStat());
  RadosFsStat *parentStat = reinterpret_cast<RadosFsStat *>(parentFsStat());
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

  // if the file exists and is not scheduled for deletion
  // we do not return an error;
  // we should check if this is desired behavior
  if (exists())
  {
    if (mPriv->radosFsIO && mPriv->radosFsIO->lazyRemoval())
    {
      mPriv->radosFsIO->setLazyRemoval(false);
    }
    else
    {
      return 0;
    }
  }

  if ((mPriv->permissions & RadosFsFile::MODE_WRITE) == 0)
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
RadosFsFile::remove()
{
  int ret;

  RadosFsInfo::update();

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  uid_t uid;
  gid_t gid;

  RadosFsStat *stat = mPriv->fsStat();

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(stat->statBuff, uid, gid,
                            O_WRONLY | O_RDWR))
  {
    ret = mPriv->removeFile();
    RadosFsStat *parentStat = reinterpret_cast<RadosFsStat *>(parentFsStat());
    indexObject(parentStat, stat, '-');
  }
  else
    return -EACCES;

  RadosFsInfo::update();

  return ret;
}

int
createStripes(rados_ioctx_t ioctx,
              const std::string &path,
              size_t from,
              size_t to,
              size_t stripeSize)
{
  int ret = 0;

  for (; from < to; from++)
  {
    const std::string &stripe = makeFileStripeName(path, from);
    ret = rados_write(ioctx, stripe.c_str(), "", stripeSize, 0);

    if (ret != 0)
    {
      radosfs_debug("Cannot create stripe %s: %s", stripe.c_str(), strerror(ret));
      break;
    }
  }

  return ret;
}

int
removeStripes(rados_ioctx_t ioctx,
              const std::string &path,
              size_t from,
              size_t to)
{
  int ret = 0;

  for (; from > to; from--)
  {
    const std::string &stripe = makeFileStripeName(path, from);
    ret = rados_remove(ioctx, stripe.c_str());

    if (ret != 0)
    {
      radosfs_debug("Cannot remove stripe %s: %s",
                    stripe.c_str(),
                    strerror(ret));
      break;
    }
  }

  return ret;
}

int
RadosFsFile::truncate(unsigned long long size)
{
  if (isLink())
    return mPriv->target->truncate(size);

  if (mPriv->radosFsIO)
  {
    updateTimeAsync(mPriv->fsStat(), XATTR_MTIME);

    return mPriv->radosFsIO->truncate(size, true);
  }

  return -ENODEV;
}

bool
RadosFsFile::isWritable()
{
  return (mPriv->permissions & RadosFsFile::MODE_WRITE) != 0;
}

bool
RadosFsFile::isReadable()
{
  return (mPriv->permissions & RadosFsFile::MODE_READ) != 0;
}

void
RadosFsFile::update()
{
  RadosFsInfo::update();
  mPriv->updatePath();
}

void
RadosFsFile::setPath(const std::string &path)
{
  std::string filePath = getFilePath(path);

  RadosFsInfo::setPath(filePath);
}

int
RadosFsFile::stat(struct stat *buff)
{
  RadosFsStat *stat;

  RadosFsInfo::update();

  if (!exists())
    return -ENOENT;

  stat = mPriv->fsStat();

  getTimeFromXAttr(stat, XATTR_MTIME, &stat->statBuff.st_mtim,
                   &stat->statBuff.st_mtime);

  *buff = stat->statBuff;

  if (isLink())
    return 0;

  buff->st_size = mPriv->radosFsIO->getSize();

  return 0;
}

int
RadosFsFile::chmod(long int permissions)
{
  int ret = 0;
  long int mode;

  if (!exists())
    return -ENOENT;

  mode = permissions | S_IFREG;

  RadosFsStat fsStat = *mPriv->fsStat();
  RadosFsStat *parentStat = reinterpret_cast<RadosFsStat *>(parentFsStat());

  uid_t uid = filesystem()->uid();

  if (uid != ROOT_UID && fsStat.statBuff.st_uid != uid)
    return -EPERM;

  fsStat.statBuff.st_mode = mode;
  const std::string &baseName = path().substr(mPriv->parentDir.length());
  const std::string &linkXAttr = getFileXAttrDirRecord(&fsStat);

  ret = rados_setxattr(mPriv->mtdPool->ioctx, parentStat->translatedPath.c_str(),
                       (XATTR_FILE_PREFIX + baseName).c_str(),
                       linkXAttr.c_str(), linkXAttr.length());

  return ret;
}

int
RadosFsFile::rename(const std::string &newPath)
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

RADOS_FS_END_NAMESPACE
