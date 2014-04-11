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
#include "RadosFsDir.hh"
#include "RadosFsFile.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsInfoPriv::RadosFsInfoPriv(RadosFs *radosFs, const std::string &objPath)
  : radosFs(radosFs),
    target(0),
    fileType(0),
    exists(false)
{
  setPath(objPath);
}

RadosFsInfoPriv::~RadosFsInfoPriv()
{
  delete target;
}

int
RadosFsInfoPriv::makeRealPath(std::string &path, rados_ioctx_t *ioctxOut)
{
  char *linkTarget = 0;
  std::string parent = getParentDir(path, 0);

  rados_ioctx_t ioctx;
  mode_t fileType;

  if (parent == "" || radosFs->mPriv->getIoctxFromPath(parent, &ioctx) != 0)
    return -ENODEV;

  while (parent != "" &&
         !checkIfPathExists(ioctx, parent.c_str(), &fileType, &linkTarget))
    parent = getParentDir(parent, 0);

  if (ioctxOut != 0)
    *ioctxOut = ioctx;

  if (fileType == S_IFREG)
  {
    radosfs_debug("Problem with part of the path, it is a file: %s",
                  parent.c_str());
    return -ENOTDIR;
  }

  if (fileType == S_IFLNK)
  {
    path.erase(0, parent.length());
    path = linkTarget + path;

    delete[] linkTarget;

    return -EAGAIN;
  }

  return 0;
}

void
RadosFsInfoPriv::setPath(const std::string &path)
{
  int ret;
  this->path = sanitizePath(path);

  while ((ret = makeRealPath(this->path)) == -EAGAIN)
  {}

  radosFs->mPriv->getIoctxFromPath(this->path, &ioctx);
}

int
RadosFsInfoPriv::makeLink(std::string &linkPath)
{
  int ret;
  rados_ioctx_t ioctx;

  while ((ret = makeRealPath(linkPath, &ioctx)) == -EAGAIN)
  {}

  if (ret != 0)
  {
    radosfs_debug("Error getting the real path for link %s",
                  linkPath.c_str());
    return ret;
  }

  struct stat buff;
  std::string linkParent = getParentDir(linkPath, 0);

  if (radosFs->stat(linkParent, &buff) != 0)
  {
    radosfs_debug("Cannot create a link in a directory that doesn't exist");
    return -ENOENT;
  }

  uid_t uid;
  gid_t gid;

  radosFs->getIds(&uid, &gid);

  if (!statBuffHasPermission(buff, uid, gid, O_WRONLY))
  {
    radosfs_debug("No permissions to write in %s", linkParent.c_str());
    return -EACCES;
  }

  std::string alternativeName;

  if (fileType == S_IFDIR)
  {
    linkPath = getDirPath(linkPath.c_str());
    alternativeName = linkPath;
    alternativeName.erase(alternativeName.length() - 1, 1);
  }
  else
  {
    if (linkPath[linkPath.length()] == PATH_SEP)
      linkPath.erase(linkPath.length() - 1, 1);

    alternativeName = linkPath;
    alternativeName += PATH_SEP;
  }

  if (radosFs->stat(alternativeName, &buff) == 0)
  {
    radosfs_debug("That path already exists: %s",
                  alternativeName.c_str());
    return -EEXIST;
  }

  if (radosFs->stat(linkPath, &buff) == 0)
  {
    radosfs_debug("The link's path already exists");
    return -EEXIST;
  }

  if (ret != 0)
  {
    radosfs_debug("Failed to retrieve the ioctx for %s", linkPath.c_str());
    return ret;
  }

  rados_write(ioctx, linkPath.c_str(), "", 0, 0);
  indexObject(ioctx, linkPath.c_str(), '+');

  setPermissionsXAttr(ioctx, linkPath.c_str(), DEFAULT_MODE_LINK, uid, gid);

  return setXAttrFromPath(ioctx, buff,
                          ROOT_UID, ROOT_UID,
                          linkPath, XATTR_LINK, this->path);
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
  if (isLink())
    return mPriv->target->isFile();

  return mPriv->fileType == S_IFREG;
}

bool
RadosFsInfo::isDir() const
{
  if (isLink())
    return mPriv->target->isDir();

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
  char *linkTarget = 0;
  mPriv->exists = checkIfPathExists(mPriv->ioctx,
                                    mPriv->path.c_str(),
                                    &mPriv->fileType,
                                    &linkTarget);

  if (mPriv->target)
  {
    delete mPriv->target;
    mPriv->target = 0;
  }

  if (!mPriv->exists)
  {
    mPriv->fileType = 0;
    return;
  }

  if (isLink() && linkTarget != 0)
  {
    if (linkTarget[strlen(linkTarget) - 1] == PATH_SEP)
      mPriv->target = new RadosFsDir(filesystem(), linkTarget);
    else
      mPriv->target = new RadosFsFile(filesystem(), linkTarget,
                                      RadosFsFile::MODE_READ_WRITE);

    delete[] linkTarget;
  }
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

int
RadosFsInfo::createLink(const std::string &linkName)
{
  std::string absLinkName(linkName);

  if (!exists())
  {
    radosfs_debug("Cannot create a link to a file or directly "
                  "that doesn't exist");
    return -ENOENT;
  }

  if (linkName == "")
  {
    radosfs_debug("The link name cannot be empty");
    return -EINVAL;
  }

  if (linkName[0] != PATH_SEP)
    absLinkName = getParentDir(mPriv->path, 0) + linkName;

  return mPriv->makeLink(absLinkName);
}

bool
RadosFsInfo::isLink() const
{
  return mPriv->fileType == S_IFLNK;
}

std::string
RadosFsInfo::targetPath() const
{
  if (!isLink() || mPriv->target == 0)
    return "";

  return mPriv->target->path();
}

RADOS_FS_END_NAMESPACE
