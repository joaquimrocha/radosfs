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

#include "radosfsdefines.h"
#include "radosfscommon.h"
#include "RadosFsInfo.hh"
#include "RadosFsInfoPriv.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsInfoPriv::RadosFsInfoPriv(RadosFs *radosFs, const std::string &objPath)
  : radosFs(radosFs),
    fileType(0),
    exists(false)
{
  setPath(objPath);
  radosFs->mPriv->getIoctxFromPath(objPath, &ioctx);
}

RadosFsInfoPriv::~RadosFsInfoPriv()
{}

void
RadosFsInfoPriv::setPath(const std::string &path)
{
  this->path = sanitizePath(path);

  radosFs->mPriv->getIoctxFromPath(path, &ioctx);
}

RadosFsInfo::RadosFsInfo(RadosFs *radosFs, const std::string &path)
  : mPriv(new RadosFsInfoPriv(radosFs, path))
{
  update();
}

RadosFsInfo::~RadosFsInfo()
{}

RadosFsInfo::RadosFsInfo(const RadosFsInfo &otherInfo)
  : mPriv(new RadosFsInfoPriv(otherInfo.filesystem(), otherInfo.path()))
{
  update();
}

std::string
RadosFsInfo::path() const
{
  return mPriv->path;
}

void
RadosFsInfo::setPath(const std::string &path)
{
  mPriv->setPath(path);
  update();
}

RadosFs *
RadosFsInfo::filesystem() const
{
  return mPriv->radosFs;
}

void
RadosFsInfo::setFilesystem(RadosFs *radosFs)
{
  mPriv->radosFs = radosFs;
}

bool
RadosFsInfo::isFile() const
{
  return mPriv->fileType == S_IFREG;
}

bool
RadosFsInfo::isDir() const
{
  return mPriv->fileType == S_IFDIR;
}

bool
RadosFsInfo::exists() const
{
  return mPriv->exists;
}

int
RadosFsInfo::stat(struct stat *buff)
{
  return genericStat(mPriv->ioctx, mPriv->path.c_str(), buff);
}

void
RadosFsInfo::update()
{
  mPriv->exists = checkIfPathExists(mPriv->ioctx,
                                    mPriv->path.c_str(),
                                    &mPriv->fileType);
}

int
RadosFsInfo::setXAttr(const std::string &attrName,
                      const std::string &value)
{
  int ret;
  struct stat buff;

  ret = stat(&buff);

  if (ret != 0)
    return ret;

  return setXAttrFromPath(mPriv->ioctx, buff,
                          mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                          path(), attrName, value);
}

int
RadosFsInfo::getXAttr(const std::string &attrName,
                      std::string &value,
                      size_t length)
{
  int ret;
  struct stat buff;

  ret = stat(&buff);

  if (ret != 0)
    return ret;

  return getXAttrFromPath(mPriv->ioctx, buff,
                          mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                          path(), attrName, value, length);
}

int
RadosFsInfo::removeXAttr(const std::string &attrName)
{
  int ret;
  struct stat buff;

  ret = stat(&buff);

  if (ret != 0)
    return ret;

  return removeXAttrFromPath(mPriv->ioctx, buff,
                             mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                             path(), attrName);
}

int
RadosFsInfo::getXAttrsMap(std::map<std::string, std::string> &map)
{
  int ret;
  struct stat buff;

  ret = stat(&buff);

  if (ret != 0)
    return ret;

  return getMapOfXAttrFromPath(mPriv->ioctx, buff,
                               mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                               path(), map);
}

RADOS_FS_END_NAMESPACE
