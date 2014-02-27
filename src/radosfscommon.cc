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

#include "radosfscommon.h"

int
getPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    mode_t *mode,
                    uid_t *uid,
                    gid_t *gid)
{
  char permXAttr[XATTR_PERMISSIONS_LENGTH];

  int ret = rados_getxattr(ioctx, obj, XATTR_PERMISSIONS,
                           permXAttr, XATTR_PERMISSIONS_LENGTH);

  permXAttr[XATTR_PERMISSIONS_LENGTH - 1] = '\0';

  if (ret < 0)
    return ret;

  std::string permissions(permXAttr);

  std::istringstream iss(permissions);
  while (iss)
  {
    std::string token;
    iss >> token;

    if (token.compare(0, strlen(XATTR_MODE), XATTR_MODE) == 0)
    {
      token.erase(0, strlen(XATTR_MODE));
      *mode = (mode_t) strtoul(token.c_str(), 0, 8);
    }
    else if (token.compare(0, strlen(XATTR_UID), XATTR_UID) == 0)
    {
      token.erase(0, strlen(XATTR_UID));
      *uid = (uid_t) atoi(token.c_str());
    }
    else if (token.compare(0, strlen(XATTR_GID), XATTR_GID) == 0)
    {
      token.erase(0, strlen(XATTR_GID));
      *gid = (gid_t) atoi(token.c_str());
    }
  }

  return 0;
}

int
setPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    long int mode,
                    uid_t uid,
                    gid_t gid)
{
  std::ostringstream convert;

  convert << XATTR_MODE;
  convert << std::oct << mode;

  convert << " " << XATTR_UID;
  convert << std::dec << uid;

  convert << " " << XATTR_GID;
  convert << std::dec << gid;

  return rados_setxattr(ioctx, obj, XATTR_PERMISSIONS,
                        convert.str().c_str(), convert.str().length() + 1);
}

bool
statBuffHasPermission(const struct stat &buff,
                      const uid_t uid,
                      const gid_t gid,
                      const int permission)
{
  if (uid == ROOT_UID)
    return true;

  mode_t usrPerm = S_IRUSR;
  mode_t grpPerm = S_IRGRP;
  mode_t othPerm = S_IROTH;

  if (permission != O_RDONLY)
  {
    usrPerm = S_IWUSR;
    grpPerm = S_IWGRP;
    othPerm = S_IWOTH;
  }

  if (buff.st_uid == uid && (buff.st_mode & usrPerm))
    return true;
  if (buff.st_gid == gid && (buff.st_mode & grpPerm))
    return true;
  if (buff.st_mode & othPerm)
    return true;

  return false;
}

int
genericStat(rados_ioctx_t &ioctx,
            const char* path,
            struct stat* buff)
{
  uint64_t psize;
  time_t pmtime;
  int ret;
  uid_t uid = 0;
  gid_t gid = 0;
  mode_t permissions = DEFAULT_MODE_FILE;
  bool isDir = false;
  std::string realPath(path);

  ret = rados_stat(ioctx, realPath.c_str(), &psize, &pmtime);
  isDir = realPath[realPath.length() - 1] == PATH_SEP;

  if (ret != 0)
  {
    if (isDir)
      return ret;

    realPath += PATH_SEP;

    isDir = rados_stat(ioctx, realPath.c_str(), &psize, &pmtime) == 0;

    if (!isDir)
      return -ENOENT;
  }

  if (isDir)
    permissions = DEFAULT_MODE_DIR;

  ret = getPermissionsXAttr(ioctx, realPath.c_str(), &permissions, &uid, &gid);

  if (ret != 0)
    ret = 0;

  buff->st_dev = 0;
  buff->st_ino = hash(realPath.c_str());
  buff->st_mode = permissions;
  buff->st_nlink = 1;
  buff->st_uid = uid;
  buff->st_gid = gid;
  buff->st_rdev = 0;
  buff->st_size = psize;
  buff->st_blksize = 4;
  buff->st_blocks = buff->st_size / buff->st_blksize;
  buff->st_atime = pmtime;
  buff->st_mtime = pmtime;
  buff->st_ctime = pmtime;

  return ret;
}

bool
checkIfPathExists(rados_ioctx_t &ioctx,
                  const char *path,
                  mode_t *filetype)
{
  const int length = strlen(path);
  bool isDirPath = path[length - 1] == PATH_SEP;

  if (rados_stat(ioctx, path, 0, 0) == 0)
  {
    if (isDirPath)
      *filetype = S_IFDIR;
    else
      *filetype = S_IFREG;
    return true;
  }

  std::string otherPath(path);

  if (isDirPath)
  {
    // delete the last separator
    otherPath.erase(length - 1, 1);
  }
  else
  {
    otherPath += PATH_SEP;
  }

  if (rados_stat(ioctx, otherPath.c_str(), 0, 0) == 0)
  {
    if (isDirPath)
      *filetype = S_IFREG;
    else
      *filetype = S_IFDIR;

    return true;
  }

  return false;
}

std::string
getParentDir(const std::string &path, int *pos)
{
  int length = path.length();
  int index = path.rfind(PATH_SEP, length - 2);

  if (length - 1 < 1 || index == std::string::npos)
    return "";

  index++;

  if (pos)
    *pos = index;

  return path.substr(0, index);
}

std::string
escapeObjName(const std::string &obj)
{
  std::string str(obj);

  size_t index = 0;
  while (true)
  {
    index = str.find("\"", index);

    if (index == std::string::npos)
      break;

    str.replace(index, 1, "\\\"");
  }

  return str.c_str();
}

int
indexObject(rados_ioctx_t ioctx,
            const std::string &obj,
            char op)
{
  int index;
  std::string contents;
  const std::string &dirName = getParentDir(obj, &index);

  if (dirName == "")
    return 0;

  const std::string &baseName = obj.substr(index, std::string::npos);

  contents += op;
  contents += INDEX_NAME_KEY "\"" + escapeObjName(baseName) + "\" ";
  contents += "\n";

  return rados_append(ioctx, dirName.c_str(),
                      contents.c_str(), strlen(contents.c_str()));
}

bool
verifyIsOctal(const char *mode)
{
  const char *ptr = mode;
  while (*ptr != '\0')
  {
    if (*ptr < '0' || *ptr > '7')
      return false;
    ptr++;
  }

  return true;
}

std::string
getDirPath(const char *path)
{
  std::string dir(path);

  if (dir[dir.length() - 1] != PATH_SEP)
    dir += PATH_SEP;

  return dir;
}

std::string
getRealPath(rados_ioctx_t ioctx, const std::string &path)
{
  mode_t fileType;

  if (!checkIfPathExists(ioctx, path.c_str(), &fileType))
    return "";

  std::string realPath(path);

  if (realPath[realPath.length()] == PATH_SEP)
  {
    if (fileType == S_IFREG)
      realPath.erase(realPath.length() - 1, 1);
  }
  else
  {
    if (fileType == S_IFDIR)
      realPath += PATH_SEP;
  }

  return realPath;
}

int
checkPermissionsForXAttr(const struct stat &statBuff,
                         const std::string &attrName,
                         uid_t uid,
                         gid_t gid,
                         int permission)
{
  if (!statBuffHasPermission(statBuff, uid, gid, permission))
    return -EACCES;

  if (attrName.compare(0, strlen(XATTR_SYS_PREFIX), XATTR_SYS_PREFIX) == 0)
  {
    if (uid != ROOT_UID)
      return -EACCES;
  }
  else if (attrName.compare(0, strlen(XATTR_USER_PREFIX), XATTR_USER_PREFIX) != 0)
  {
    return -EINVAL;
  }

  return 0;
}

int
setXAttrFromPath(rados_ioctx_t ioctx,
                 const struct stat &statBuff,
                 uid_t uid,
                 gid_t gid,
                 const std::string &path,
                 const std::string &attrName,
                 const std::string &value)
{
  int ret = checkPermissionsForXAttr(statBuff, attrName, uid, gid, O_WRONLY);

  if (ret != 0)
    return ret;

  return rados_setxattr(ioctx, path.c_str(), attrName.c_str(),
                        value.c_str(), value.length());
}

int
getXAttrFromPath(rados_ioctx_t ioctx,
                 const struct stat &statBuff,
                 uid_t uid,
                 gid_t gid,
                 const std::string &path,
                 const std::string &attrName,
                 std::string &value,
                 size_t length)
{
  int ret = checkPermissionsForXAttr(statBuff, attrName, uid, gid, O_RDONLY);

  if (ret != 0)
    return ret;

  char *buff = new char[length];
  ret = rados_getxattr(ioctx, path.c_str(), attrName.c_str(), buff, length);

  if (ret >= 0)
    value = std::string(buff);

  delete[] buff;

  return ret;
}

int removeXAttrFromPath(rados_ioctx_t ioctx,
                        const struct stat &statBuff,
                        uid_t uid,
                        gid_t gid,
                        const std::string &path,
                        const std::string &attrName)
{
  int ret = checkPermissionsForXAttr(statBuff, attrName, uid, gid, O_WRONLY);

  if (ret != 0)
    return ret;

  return rados_rmxattr(ioctx, path.c_str(), attrName.c_str());
}

int getMapOfXAttrFromPath(rados_ioctx_t ioctx,
                          const struct stat &statBuff,
                          uid_t uid,
                          gid_t gid,
                          const std::string &path,
                          std::map<std::string, std::string> &map)
{
  if (!statBuffHasPermission(statBuff, uid, gid, O_RDONLY))
    return -EACCES;

  rados_xattrs_iter_t iter;

  int ret = rados_getxattrs(ioctx, path.c_str(), &iter);

  if (ret != 0)
    return ret;

  const char *attr = 0;
  const char *value = 0;
  size_t len;
  const size_t sysPrefixSize = strlen(XATTR_SYS_PREFIX);

  while ((ret = rados_getxattrs_next(iter, &attr, &value, &len)) == 0)
  {
    if (attr == 0)
      break;

    if (uid != ROOT_UID && strncmp(attr, XATTR_SYS_PREFIX, sysPrefixSize) == 0)
      continue;

    map[attr] = value;
  }

  rados_getxattrs_end(iter);

  return ret;
}
