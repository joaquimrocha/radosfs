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

#include <stdexcept>
#include <uuid/uuid.h>

#include "radosfsdefines.h"
#include "radosfscommon.h"
#include "RadosFsFile.hh"
#include "RadosFsFilePriv.hh"
#include "RadosFsDir.hh"
#include "RadosFsPriv.hh"

#define UUID_STRING_SIZE 36

RADOS_FS_BEGIN_NAMESPACE

RadosFsFilePriv::RadosFsFilePriv(RadosFsFile *fsFile,
                                 RadosFsFile::OpenMode mode)
  : fsFile(fsFile),
    permissions(RadosFsFile::MODE_NONE),
    mode(mode),
    target(0)
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

  dataPool = stat->pool;

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
    radosFsIO = radosFs->mPriv->getRadosFsIO(stat->translatedPath);

    if (!radosFsIO.get())
    {
      int stripeSize = 0;

      if (fsFile->exists())
      {
        const size_t length = 12;
        char buff[length + 1];

        int ret = rados_getxattr(dataPool->ioctx,
                                 stat->translatedPath.c_str(),
                                 XATTR_FILE_STRIPE_SIZE,
                                 buff,
                                 length);

        if (ret >= 0)
        {
          stripeSize = atoi(buff);
        }
      }

      if (stripeSize == 0)
        stripeSize = radosFs->fileStripeSize();

      RadosFsIO *fsIO = new RadosFsIO(fsFile->filesystem(), dataPool,
                                      stat->translatedPath, stripeSize);

      radosFsIO = std::tr1::shared_ptr<RadosFsIO>(fsIO);
      radosFs->mPriv->setRadosFsIO(radosFsIO);
    }
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

  fsFile->RadosFsInfo::update();

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

std::string
RadosFsFilePriv::sanitizePath(const std::string &path)
{
  if (path.length() == 1 && path[0] == '/')
    throw std::invalid_argument("Cannot use / as a path argument");

  std::string filePath(path);

  if (filePath != "" && filePath[filePath.length() - 1] == '/')
    filePath.erase(filePath.length() - 1, 1);

  return filePath;
}

int
RadosFsFilePriv::removeFile()
{
  if (radosFsIO && radosFsIO.use_count() > 1)
    radosFsIO->setLazyRemoval(true);

  return radosFsIO->remove();
}

RadosFsStat *
RadosFsFilePriv::fsStat(void)
{
  return reinterpret_cast<RadosFsStat *>(fsFile->fsStat());
}

int
RadosFsFilePriv::createInode(rados_ioctx_t ioctx,
                             const std::string &name,
                             const std::string &hardLink,
                             long int mode,
                             uid_t uid,
                             gid_t gid)
{
  rados_write_op_t writeOp = rados_create_write_op();

  rados_write_op_setxattr(writeOp, XATTR_INODE_HARD_LINK, hardLink.c_str(),
                          hardLink.length());

  const std::string &permissions = makePermissionsXAttr(mode, uid, gid);

  rados_write_op_setxattr(writeOp, XATTR_PERMISSIONS, permissions.c_str(),
                          permissions.length() + 1);

  int ret = rados_write_op_operate(writeOp, ioctx, name.c_str(), NULL, 0);

  rados_release_write_op(writeOp);

  return ret;
}

RadosFsFile::RadosFsFile(RadosFs *radosFs,
                         const std::string &path,
                         RadosFsFile::OpenMode mode)
  : RadosFsInfo(radosFs, RadosFsFilePriv::sanitizePath(path)),
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

  if (mPriv->permissions & RadosFsFile::MODE_READ)
  {
    if (isLink())
      return mPriv->target->read(buff, offset, blen);

    return mPriv->radosFsIO->read(buff, offset, blen);
  }

  return -EACCES;
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

    return mPriv->radosFsIO->writeSync(buff, offset, blen);
  }

  return -EACCES;
}

int
RadosFsFile::create(int mode, const std::string pool)
{
  RadosFsStat *stat = reinterpret_cast<RadosFsStat *>(fsStat());
  int ret;

  if (pool != "")
    mPriv->updateDataPool(pool);

  if (mPriv->dataPool.get() == 0)
    return -ENODEV;

  // we don't allow object names that end in a path separator
  const std::string filePath = path();
  if ((exists() && !isFile()) ||
      (filePath != "" && filePath[filePath.length() - 1] == PATH_SEP))
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

  uuid_t inode;
  char inodeStr[UUID_STRING_SIZE + 1];

  uuid_generate(inode);
  uuid_unparse(inode, inodeStr);

  mPriv->inode = inodeStr;
  stat->path = path();
  stat->translatedPath = mPriv->inode;
  stat->statBuff.st_mode = permOctal;
  stat->statBuff.st_uid = uid;
  stat->statBuff.st_gid = gid;
  stat->pool = mPriv->mtdPool;

  ret = mPriv->createInode(mPriv->dataPool->ioctx, inodeStr, filePath,
                           permOctal, uid, gid);

  if (ret != 0)
    return ret;

  indexObject(stat, '+');

  if (ret != 0)
    return ret;

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

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid,
                            O_WRONLY | O_RDWR))
  {
    ret = mPriv->removeFile();
    RadosFsStat *stat = mPriv->fsStat();
    stat->pool = mPriv->mtdPool;
    indexObject(stat, '-');
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
              size_t to)
{
  int ret;

  for (; from < to; from++)
  {
    const char *stripe = makeFileStripeName(path, from).c_str();
    ret = rados_write(ioctx, stripe, "", 0, 0);

    if (ret != 0)
    {
      radosfs_debug("Cannot create stripe %s: %s", stripe, strerror(ret));
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
  int ret;

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
  int ret;
  struct stat statBuff;
  uid_t uid;
  gid_t gid;
  RadosFsStat *fsStat = mPriv->fsStat();
  rados_ioctx_t ioctx = mPriv->dataPool->ioctx;
  const bool fileIsLink = isLink();
  const bool lockFiles = filesystem()->fileLocking() && !fileIsLink;

  if (lockFiles)
  {
    while (rados_lock_exclusive(ioctx,
                                fsStat->translatedPath.c_str(),
                                FILE_STRIPE_LOCKER,
                                FILE_STRIPE_LOCKER_COOKIE_OTHER,
                                "",
                                0,
                                0) != 0)
    {}
  }

  ret = stat(&statBuff);

  if (ret != 0)
  {
    goto bailout;
  }

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
  {
    goto bailout;
  }

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid,
                            O_WRONLY | O_RDWR))
  {
    if (fileIsLink)
      return mPriv->target->truncate(size);

    const size_t stripeSize = mPriv->radosFsIO->stripeSize();
    size_t lastStripe = 0;

    if (statBuff.st_size > 0)
      lastStripe = (statBuff.st_size - 1) / stripeSize;

    size_t newLastStripe = 0;

    if (size > 0)
      newLastStripe = (size - 1) / stripeSize;

    if (lastStripe > newLastStripe)
      removeStripes(ioctx, fsStat->translatedPath, lastStripe, newLastStripe);
    else if (lastStripe < newLastStripe)
      createStripes(ioctx, fsStat->translatedPath, lastStripe + 1, newLastStripe);

    const char *stripe = makeFileStripeName(fsStat->translatedPath,
                                            newLastStripe).c_str();

    // create new last stripe if it didn't exist before
    if (newLastStripe > lastStripe)
      ret = rados_write(ioctx, stripe, "", 0, 0);

    if (ret == 0)
    {
      size_t remainingSize = size - newLastStripe * stripeSize;

      ret = rados_trunc(ioctx, stripe, remainingSize);
    }
  }
  else
    ret = -EACCES;

bailout:
  if (lockFiles)
  {
    rados_unlock(mPriv->dataPool->ioctx,
                 fsStat->translatedPath.c_str(),
                 FILE_STRIPE_LOCKER,
                 FILE_STRIPE_LOCKER_COOKIE_OTHER);
  }

  return ret;
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
  std::string filePath = RadosFsFilePriv::sanitizePath(path);

  RadosFsInfo::setPath(filePath);
}

int
RadosFsFile::stat(struct stat *buff)
{
  int ret;
  RadosFsStat *stat;

  RadosFsInfo::update();

  if (!exists())
    return -ENOENT;

  stat = mPriv->fsStat();

  *buff = stat->statBuff;

  if (isLink())
    return 0;

  size_t numStripes = mPriv->radosFsIO->getLastStripeIndex();
  u_int64_t size;

  ret = rados_stat(mPriv->dataPool->ioctx,
                   makeFileStripeName(stat->translatedPath, numStripes).c_str(),
                   &size,
                   0);

  if (ret != 0)
    return ret;

  buff->st_size = numStripes * mPriv->radosFsIO->stripeSize() + size;

  return ret;
}

RADOS_FS_END_NAMESPACE
