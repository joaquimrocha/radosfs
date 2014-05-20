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
    target(0),
    ioctx(0),
    mtdIoctx(0)
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
RadosFsFilePriv::updatePath()
{
  RadosFsStat *stat = fsStat();
  const RadosFsPool *dataPool, *mtdPool;

  parentDir = getParentDir(fsFile->path(), 0);

  RadosFs *radosFs = fsFile->filesystem();

  dataPool = radosFs->mPriv->getDataPoolFromPath(fsFile->path());
  mtdPool = radosFs->mPriv->getMetadataPoolFromPath(fsFile->path());

  if (!dataPool || !mtdPool)
    return;

  ioctx = dataPool->ioctx;
  mtdIoctx = mtdPool->ioctx;

  updatePermissions();

  radosFsIO.reset();

  if (!fsFile->exists())
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


        int ret = rados_getxattr(ioctx,
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

      RadosFsIO *fsIO = new RadosFsIO(dataPool, stat->translatedPath,
                                      stripeSize);

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

  if (!mtdIoctx)
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
  int ret;

  if (radosFsIO)
  {
    if (radosFsIO.use_count() > 1)
    {
      radosFsIO->setLazyRemoval(true);
      ret = 0;
    }
    else
    {
      ret = rados_remove(ioctx, fsStat()->translatedPath.c_str());
    }
  }

  return ret;
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

ssize_t
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

ssize_t
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
RadosFsFile::create(int mode)
{
  RadosFsStat *stat = reinterpret_cast<RadosFsStat *>(fsStat());
  int ret;

  if (mPriv->ioctx == 0)
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
  stat->ioctx = mPriv->mtdIoctx;

  ret = mPriv->createInode(mPriv->ioctx, inodeStr, filePath, permOctal, uid,
                           gid);

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
    stat->ioctx = mPriv->mtdIoctx;
    indexObject(stat, '-');
  }
  else
    return -EACCES;

  RadosFsInfo::update();

  return ret;
}

int
RadosFsFile::truncate(unsigned long long size)
{
  int ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid,
                            O_WRONLY | O_RDWR))
  {
    if (isLink())
      return mPriv->target->truncate(size);

    ret = rados_trunc(mPriv->ioctx,
                      mPriv->fsStat()->translatedPath.c_str(), size);
  }
  else
    ret = -EACCES;

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

RADOS_FS_END_NAMESPACE
