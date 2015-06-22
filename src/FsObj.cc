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
#include <sys/stat.h>

#include "radosfsdefines.h"
#include "radosfscommon.h"
#include "FsObj.hh"
#include "FsObjPriv.hh"
#include "Dir.hh"
#include "File.hh"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

FsObjPriv::FsObjPriv(FsObj *obj, Filesystem *fs, const std::string &objPath)
  : fsObj(obj),
    radosFs(fs),
    target(""),
    exists(false)
{}

FsObjPriv::~FsObjPriv()
{}

int
FsObjPriv::makeRealPath(std::string &path)
{
  std::string parent = getParentDir(path, 0);
  bool originalPathExists = true;
  Stat stat;

  parentDirStat.reset();

  while (parent != "")
  {
    int ret = radosFs->mPriv->stat(parent, &stat);

    if (ret == -ENOENT)
      parent = getParentDir(parent, 0);
    else if (ret == 0)
      break;
    else
      return ret;

    originalPathExists = false;
  }

  if (parent == "")
    return -ENODEV;

  if (S_ISLNK(stat.statBuff.st_mode))
  {
    path.erase(0, parent.length());
    path = stat.translatedPath + path;

    return -EAGAIN;
  }

  if (S_ISREG(stat.statBuff.st_mode))
  {
    radosfs_debug("Problem with part of the path, it is a file: %s",
                  parent.c_str());
    return -ENOTDIR;
  }

  if (originalPathExists)
    parentDirStat = stat;

  return 0;
}

void
FsObjPriv::setPath(const std::string &path)
{
  int ret;
  this->path = sanitizePath(path);

  stat.reset();
  target.clear();

  if (this->path.length() > MAXIMUM_PATH_LENGTH)
  {
    this->path = PATH_SEP;
    throw std::invalid_argument("Path length is too big.");
  }

  while ((ret = makeRealPath(this->path)) == -EAGAIN)
  {}

  ret = radosFs->mPriv->stat(this->path, &stat);

  exists = ret == 0;

  if (fsObj->isLink())
  {
    const std::string &linkTarget = stat.translatedPath;
    target = linkTarget;
  }
}

int
FsObjPriv::makeLink(std::string &linkPath)
{
  int ret;

  while ((ret = makeRealPath(linkPath)) == -EAGAIN)
  {}

  if (linkPath.length() > MAXIMUM_PATH_LENGTH)
  {
    radosfs_debug("Error: The link path is too big.");
    return -ENAMETOOLONG;
  }

  const PoolSP pool =
      radosFs->mPriv->getMetadataPoolFromPath(linkPath);

  if (!pool)
    return -ENODEV;

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

  if (S_ISDIR(stat.statBuff.st_mode))
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

  Stat linkStat = stat;
  linkStat.path = linkPath;
  linkStat.pool = pool;
  linkStat.translatedPath = this->path;
  linkStat.statBuff.st_uid = uid;
  linkStat.statBuff.st_gid = gid;
  linkStat.statBuff.st_mode = DEFAULT_MODE_LINK;

  return indexObject(&parentDirStat, &linkStat, '+');
}

/**
 * @class FsObj
 *
 * Abstract class that represents an object in the filesystem (a File or Dir).
 *
 * This class is used as a base class for the File and Dir classes as they share
 * some common features.
 */

/**
 * @fn virtual bool FsObj::isWritable(void)
 * Should be implemented to check whether the object is writable.
 *
 * @fn virtual bool FsObj::isReadable(void)
 * Should be implemented to check whether the object is readable.
 */

/**
 * Builds a new instance of FsObj.
 * @param radosFs a pointer to the Filesystem where this object belongs.
 * @param path the absolute path (of a file or directory) that represents this
 *        object.
 */
FsObj::FsObj(Filesystem *radosFs, const std::string &path)
  : mPriv(new FsObjPriv(this, radosFs, path))
{
  mPriv->setPath(path);
}

FsObj::~FsObj()
{}

/**
 * Copy constructor.
 * @param otherFsObj another FsObj instance.
 */
FsObj::FsObj(const FsObj &otherFsObj)
  : mPriv(new FsObjPriv(this, otherFsObj.filesystem(), otherFsObj.path()))
{
  mPriv->setPath(otherFsObj.path());
}

/**
 * Returns the path that represents this object.
 * @return the path that represents this object.
 */
std::string
FsObj::path() const
{
  return mPriv->path;
}

/**
 * Changes the object that this FsObj instance refers to. This works as if
 * instantiating the object using a different path.
 * @param path a path to a file or directory.
 */
void
FsObj::setPath(const std::string &path)
{
  mPriv->setPath(path);
  update();
}

/**
 * Returns the filesystem that contains this object.
 * @return a pointer to the filesystem that contains this object.
 */
Filesystem *
FsObj::filesystem() const
{
  return mPriv->radosFs;
}

void
FsObj::setFilesystem(Filesystem *radosFs)
{
  mPriv->radosFs = radosFs;
}

/**
 * Checks whether this object refers to a file.
 * @return true if this object refers to a file, false otherwise.
 */
bool
FsObj::isFile() const
{
  return !isDir();
}

/**
 * Checks whether this object refers to a directory.
 * @return true if this object refers to a directory, false otherwise.
 */
bool
FsObj::isDir() const
{
  if (!exists())
    return isDirPath(mPriv->path);

  if (isLink())
    return isDirPath(mPriv->target);

  return S_ISDIR(mPriv->stat.statBuff.st_mode);
}

/**
 * Checks whether this object exists in the filesystem.
 * @return true if this object exists, false otherwise.
 */
bool
FsObj::exists() const
{
  return mPriv->exists;
}

/**
 * Stats the object.
 * @param[out] buff a stat struct to fill with the details of the object.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::stat(struct stat *buff)
{
  FsObj::update();

  if (!isReadable())
    return -EPERM;

  *buff = mPriv->stat.statBuff;

  return 0;
}

/**
 * Updates the state of this FsObj instance according to the status of the
 * actual object in the system.
 *
 * Traditionally this could be considered as reopening the file or directory
 * that this object refers to. It is used to get the latest status of the
 * object in the system.
 */
void
FsObj::update()
{
  mPriv->exists = false;

  if (mPriv->target != "")
  {
    mPriv->target = "";
  }

  if (!mPriv->parentDirStat.pool)
  {
    const std::string parentDir = getParentDir(mPriv->path, 0);
    mPriv->radosFsPriv()->stat(parentDir, &mPriv->parentDirStat);
  }

  mPriv->exists = mPriv->radosFsPriv()->stat(mPriv->path, &mPriv->stat) == 0;

  if (!mPriv->exists)
    return;

  if (isLink())
  {
    const std::string &linkTarget = mPriv->stat.translatedPath;
    mPriv->target = linkTarget;
  }
}

/**
 * Sets an xattribute in this object.
 * @param attrName the name of the xattribute to set.
 * @param value the value of the xattribute to set.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::setXAttr(const std::string &attrName, const std::string &value)
{
  // We don't call the similar methods from Filesystem for avoiding extra stat
  // calls (except for links).

  if (isLink())
    return filesystem()->setXAttr(mPriv->target, attrName, value);

  Pool *pool = mPriv->stat.pool.get();

  if (!pool)
    return -ENODEV;

  return setXAttrFromPath(mPriv->stat, mPriv->radosFs->uid(),
                          mPriv->radosFs->gid(), attrName, value);
}

/**
 * Gets the value of an xattribute in this object.
 * @param attrName the name of the xattribute to get.
 * @param[out] value a reference to return the value of the xattribute.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::getXAttr(const std::string &attrName, std::string &value)
{
  // We don't call the similar methods from Filesystem for avoiding extra stat
  // calls (except for links).

  if (isLink())
    return filesystem()->getXAttr(mPriv->target, attrName, value);

  Pool *pool = mPriv->stat.pool.get();

  if (!pool)
    return -ENODEV;

  return getXAttrFromPath(pool->ioctx, mPriv->stat.statBuff,
                          mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                          mPriv->stat.translatedPath, attrName, value);
}

/**
 * Removes an xattribute in this object.
 * @param attrName the name of the xattribute to remove.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::removeXAttr(const std::string &attrName)
{
  // We don't call the similar methods from Filesystem for avoiding extra stat
  // calls (except for links).

  if (isLink())
    return filesystem()->removeXAttr(mPriv->target, attrName);

  Pool *pool = mPriv->stat.pool.get();

  if (!pool)
    return -ENODEV;

  return removeXAttrFromPath(pool->ioctx, mPriv->stat.statBuff,
                             mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                             mPriv->stat.translatedPath, attrName);
}

/**
 * Gets a map with all the xattributes in this object.
 * @param map the map reference in which to set the keys and values of the
 *        xattributes.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::getXAttrsMap(std::map<std::string, std::string> &map)
{
  // We don't call the similar methods from Filesystem for avoiding extra stat
  // calls (except for links).

  if (isLink())
    return filesystem()->getXAttrsMap(mPriv->target, map);

  Pool *pool = mPriv->stat.pool.get();

  if (!pool)
    return -ENOENT;

  return getMapOfXAttrFromPath(pool->ioctx, mPriv->stat.statBuff,
                               mPriv->radosFs->uid(), mPriv->radosFs->gid(),
                               mPriv->stat.translatedPath, map);
}

/**
 * Creates a symbolic link to this object.
 * @param linkName the name or path of the link. If a name is given (instead of
 *        an absolute path), then the link will be created in this object's
 *        parent directory.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::createLink(const std::string &linkName)
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

  if (isLink())
  {
    radosfs_debug("Cannot make a link to a link");
    return -EPERM;
  }

  if (linkName[0] != PATH_SEP)
    absLinkName = getParentDir(mPriv->path, 0) + linkName;

  return mPriv->makeLink(absLinkName);
}

/**
 * Checks whether this object is a symbolic link.
 * @return true if this object is a link, false otherwise.
 */
bool
FsObj::isLink() const
{
  if (!exists())
    return false;

  return S_ISLNK(mPriv->stat.statBuff.st_mode);
}

/**
 * Returns the path that this object points to (in case the object is a link).
 * @return the path that this object points to.
 */
std::string
FsObj::targetPath() const
{
  return mPriv->target;
}

/**
 * This function exists to optimize some operations and should not be used by
 * the library's clients.
 */
void *
FsObj::fsStat(void)
{
  return &mPriv->stat;
}

/**
 * This function exists to optimize some operations and should not be used by
 * the library's clients.
 */
void
FsObj::setFsStat(void *stat)
{
  mPriv->stat = *reinterpret_cast<Stat *>(stat);
}

/**
 * This function exists to optimize some operations and should not be used by
 * the library's clients.
 */
void *
FsObj::parentFsStat()
{
  return &mPriv->parentDirStat;
}

/**
 * Should be implemented (if supported) to change the permissions of the object.
 * The FsObj implementation simply returns -EOPNOTSUPP.
 * @see Dir::chmod and File::chmod.
 * @param permissions the new permissions (mode bits from sys/stat.h).
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::chmod(long int permissions)
{
  return -EOPNOTSUPP;
}

/**
 * Should be implemented (if supported) to change the uid and gid of the object.
 * The FsObj implementation simply returns -EOPNOTSUPP.
 * @see Dir::chown and File::chown.
 * @param uid a user id.
 * @param gid a group id.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::chown(uid_t uid, gid_t gid)
{
  return -EOPNOTSUPP;
}

/**
 * Should be implemented (if supported) to change the uid the object.
 * The FsObj implementation simply returns -EOPNOTSUPP.
 * @see Dir::setUid and File::setGid.
 * @param uid a user id.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::setUid(uid_t uid)
{
  return -EOPNOTSUPP;
}

/**
 * Should be implemented (if supported) to change the gid the object.
 * The FsObj implementation simply returns -EOPNOTSUPP.
 * @see Dir::setGid and File::setGid.
 * @param gid a group id.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::setGid(gid_t gid)
{
  return -EOPNOTSUPP;
}

/**
 * Should be implemented (if supported) to rename or move the object. The FsObj
 * implementation simply returns -EOPNOTSUPP.
 * @see Dir::rename and File::rename.
 * @param newPath the new path or name of the object.
 * @return 0 on success, an error code otherwise.
 */
int
FsObj::rename(const std::string &newPath)
{
  return -EOPNOTSUPP;
}

RADOS_FS_END_NAMESPACE
