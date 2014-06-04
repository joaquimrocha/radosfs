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
    permissions(RadosFsFile::MODE_NONE),
    mode(mode),
    target(0),
    ioctx(0)
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
  int ret;

  parentDir = getParentDir(fsFile->path(), 0);

  RadosFs *radosFs = fsFile->filesystem();

  const RadosFsPool *pool = radosFs->mPriv->getDataPoolFromPath(fsFile->path());

  if (!pool)
    return;

  ioctx = pool->ioctx;
  ret = genericStat(ioctx, fsFile->path().c_str(), &statBuff);

  updatePermissions();

  radosFsIO.reset();

  if (target)
  {
    delete target;
    target = 0;
  }

  radosFsIO = radosFs->mPriv->getRadosFsIO(fsFile->path());

  if (!radosFsIO.get())
  {
    RadosFsIO *fsIO = new RadosFsIO(pool, fsFile->path());

    radosFsIO = std::tr1::shared_ptr<RadosFsIO>(fsIO);
    radosFs->mPriv->setRadosFsIO(radosFsIO);
  }

  if (fsFile->isLink())
  {
    target = new RadosFsFile(fsFile->filesystem(),
                             fsFile->targetPath(),
                             mode);
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
  struct stat buff;
  permissions = RadosFsFile::MODE_NONE;

  int ret = genericStat(ioctx, parentDir.c_str(), &buff);

  if (ret != 0)
    return;

  uid_t uid;
  gid_t gid;

  fsFile->filesystem()->getIds(&uid, &gid);

  bool canWriteParent =
      statBuffHasPermission(buff, uid, gid, O_WRONLY);

  bool canReadParent =
      statBuffHasPermission(buff, uid, gid, O_RDONLY);

  fsFile->RadosFsInfo::update();

  if (fsFile->exists() && !fsFile->isFile())
    return;

  if (canWriteParent && (mode & RadosFsFile::MODE_WRITE))
  {
    if (!fsFile->exists() || statBuffHasPermission(statBuff, uid, gid, O_WRONLY))
      permissions =
          (RadosFsFile::OpenMode) (permissions | RadosFsFile::MODE_WRITE);
  }

  if (canReadParent && (mode & RadosFsFile::MODE_READ) &&
      statBuffHasPermission(statBuff, uid, gid, O_RDONLY))
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

  const std::string &filePath = fsFile->path().c_str();

  if (radosFsIO.use_count() > 1)
  {
    radosFsIO->setLazyRemoval(true);

    return 0;
  }

  ret = rados_remove(ioctx, filePath.c_str());

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
    if (mPriv->radosFsIO->lazyRemoval())
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

  ret = rados_write(mPriv->ioctx, filePath.c_str(), 0, 0, 0);

  indexObject(mPriv->ioctx, filePath, '+');

  if (ret != 0)
    return ret;

  long int permOctal = mode;

  if (mode < 0)
    permOctal = DEFAULT_MODE_FILE;

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  ret = setPermissionsXAttr(mPriv->ioctx, filePath.c_str(), permOctal, uid, gid);

  if (ret != 0)
    return ret;

  update();

  return ret;
}

int
RadosFsFile::remove()
{
  int ret;
  rados_ioctx_t ioctx = mPriv->ioctx;
  const char *filePath = path().c_str();

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(mPriv->statBuff, uid, gid, O_WRONLY | O_RDWR))
  {
    ret = mPriv->removeFile();
    indexObject(ioctx, filePath, '-');
  }
  else
    return -EACCES;

  RadosFsInfo::update();

  return ret;
}

int
RadosFsFile::truncate(unsigned long long size)
{
  rados_ioctx_t ioctx = mPriv->ioctx;
  const char *filePath = path().c_str();

  int ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(mPriv->statBuff, uid, gid, O_WRONLY | O_RDWR))
  {
    if (isLink())
      return mPriv->target->truncate(size);

    ret = rados_trunc(ioctx, filePath, size);
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

  mPriv->updatePath();
}

int
RadosFsFile::stat(struct stat *buff)
{
  if (exists())
    *buff = mPriv->statBuff;

  return RadosFsInfo::stat(buff);
}

RADOS_FS_END_NAMESPACE
