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

std::string
makePermissionsXAttr(long int mode,
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

  return convert.str();
}

int
setPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    long int mode,
                    uid_t uid,
                    gid_t gid)
{
  const std::string &permissions = makePermissionsXAttr(mode, uid, gid);

  return rados_setxattr(ioctx, obj, XATTR_PERMISSIONS, permissions.c_str(),
                        permissions.length() + 1);
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

int
statFromXAttr(const std::string &path,
              const std::string &xattrValue,
              struct stat *buff,
              std::string &link)
{
  int ret = 0;
  time_t pmtime;
  uid_t uid = 0;
  gid_t gid = 0;
  mode_t permissions = DEFAULT_MODE_FILE;
  std::string realPath(path);

  int startPos = 0, lastPos = 0;
  std::string key, value;

  while ((lastPos = splitToken(xattrValue, startPos, key, value)) != startPos)
  {
    if (key == "link")
    {
      link = value;
    }
    else if (key == "mode")
    {
      permissions = (mode_t) strtoul(value.c_str(), 0, 8);
    }
    else if (key == "uid")
    {
      uid = (uid_t) atoi(value.c_str());
    }
    else if (key == "gid")
    {
      gid = (gid_t) atoi(value.c_str());
    }
    else if (key == "time")
    {
      pmtime = (time_t) strtoul(value.c_str(), 0, 10);
    }

    startPos = lastPos;
    key = value = "";
  }

  buff->st_dev = 0;
  buff->st_ino = hash(realPath.c_str());
  buff->st_mode = permissions;
  buff->st_nlink = 1;
  buff->st_uid = uid;
  buff->st_gid = gid;
  buff->st_rdev = 0;
  buff->st_size = 0;
  buff->st_blksize = 4;
  buff->st_blocks = 0;
  buff->st_atime = pmtime;
  buff->st_mtime = pmtime;
  buff->st_ctime = pmtime;

  return ret;
}

bool
checkIfPathExists(rados_ioctx_t &ioctx,
                  const char *path,
                  mode_t *filetype,
                  char **linkTarget)
{
  uint64_t size;
  std::string realPath(path);
  const int length = strlen(path);
  bool isDirPath = path[length - 1] == PATH_SEP;

  if (rados_stat(ioctx, realPath.c_str(), &size, 0) != 0)
  {
    if (isDirPath)
    {
      // delete the last separator
      realPath.erase(length - 1, 1);
    }
    else
    {
      realPath += PATH_SEP;
    }

    isDirPath = !isDirPath;

    if (rados_stat(ioctx, realPath.c_str(), &size, 0) != 0)
    {
      return false;
    }
  }

  if (size == 0 && getLinkTarget(ioctx, realPath, linkTarget) > 0)
  {
    *filetype = S_IFLNK;

    return true;
  }

  if (isDirPath)
    *filetype = S_IFDIR;
  else
    *filetype = S_IFREG;

  return true;
}

int
getLinkTarget(rados_ioctx_t ioctx, const std::string &path, char **linkTarget)
{
  char *buff = new char[XATTR_LINK_LENGTH];
  int ret = rados_getxattr(ioctx,
                           path.c_str(),
                           XATTR_LINK,
                           buff,
                           XATTR_LINK_LENGTH);

  if (ret > 0)
  {
    if (ret < XATTR_LINK_LENGTH)
      buff[ret] = '\0';
    else
      buff[XATTR_LINK_LENGTH - 1] = '\0';

    if (linkTarget)
      *linkTarget = buff;
    else
      delete[] buff;

    return ret;
  }

  delete[] buff;

  return ret;
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
  std::string str("");

  for (size_t i = 0; i < obj.length(); i++)
  {
    if (obj[i] == '"')
      str += "\\\"";
    else if (obj[i] == '\n')
      str += '%';
    else if (obj[i] == '%')
      str += "\\%";
    else
      str += obj[i];
  }

  return str;
}

std::string
unescapeObjName(const std::string &obj)
{
  std::string str("");

  if (obj == "")
    return str;

  size_t i;
  const int length = obj.length();

  for (i = 0; i < length - 1; i++)
  {
    if (obj[i] == '\\')
    {
      if (obj[i + 1] == '"')
        str += '"';
      else if (obj[i + 1] == '%')
        str += '%';
      else if (obj[i + 1] == '"')
        str += '"';
      else
        obj[i];

      i++;
    }
    else if (obj[i] == '%')
    {
      str += '\n';
    }
    else
    {
      str += obj[i];
    }
  }

  if (i <= length - 1)
  {
    if (obj[length - 1] == '%')
      str += '\n';
    else
      str += obj[length - 1];
  }

  return str;
}

int
indexObject(const RadosFsStat *stat, char op)
{
  int index;
  std::string contents;
  const std::string &dirName = getParentDir(stat->path, &index);
  std::string xAttrKey("");
  std::ostringstream xAttrValue("");

  if (dirName == "")
    return 0;

  const std::string &baseName = stat->path.substr(index, std::string::npos);

  contents = getObjectIndexLine(baseName, op);

  if ((stat->statBuff.st_mode & S_IFDIR) == 0)
  {
    xAttrKey = XATTR_FILE_PREFIX + baseName;

    if (op == '+')
    {
      xAttrValue << "link=\"" << stat->translatedPath << "\"";

      if (stat->translatedPath[0] == PATH_SEP)
      {
        xAttrValue << " " << XATTR_UID << "\"" << stat->statBuff.st_uid << "\" ";
        xAttrValue << XATTR_GID << "\"" << stat->statBuff.st_gid << "\" ";
        xAttrValue << "time=" << "\""  << stat->statBuff.st_ctime << "\" " ;
        xAttrValue << XATTR_MODE << "\"" << std::oct << stat->statBuff.st_mode << "\" ";
      }
    }
  }

  return writeContentsAtomically(stat->ioctx, dirName.c_str(), contents,
                                 xAttrKey, xAttrValue.str());
}

std::string
getObjectIndexLine(const std::string &obj, char op)
{
  std::string contents;

  contents += op;
  contents += INDEX_NAME_KEY "=\"" + escapeObjName(obj) + "\" ";
  contents += "\n";

  return contents;
}

int
indexObjectMetadata(rados_ioctx_t ioctx,
                    const std::string &obj,
                    std::map<std::string, std::string> &metadata,
                    char op)
{
  int index;
  std::string contents;
  const std::string &dirName = getParentDir(obj, &index);

  if (dirName == "")
    return 0;

  const std::string &baseName = obj.substr(index, std::string::npos);

  contents = "+";
  contents += INDEX_NAME_KEY "=\"" + escapeObjName(baseName) + "\" ";

  std::map<std::string, std::string>::iterator it;
  for (it = metadata.begin(); it != metadata.end(); it++)
  {
    const std::string &key = (*it).first;
    const std::string &value = (*it).second;

    contents += op;
    contents += INDEX_METADATA_PREFIX ".\""  + escapeObjName(key) + "\"";

    if (op == '+')
      contents += "=\"" + escapeObjName(value) + "\"";

    contents += " ";
  }

  contents += "\n";

  return writeContentsAtomically(ioctx, dirName.c_str(), contents);
}

int
writeContentsAtomically(rados_ioctx_t ioctx,
                        const std::string &obj,
                        const std::string &contents,
                        const std::string &xattrKey,
                        const std::string &xattrValue)
{
  const char *keys[] = { DIR_LOG_UPDATED };
  const char *values[] = { DIR_LOG_UPDATED_TRUE };
  const size_t lengths[] = { strlen(values[0]) };

  rados_write_op_t writeOp = rados_create_write_op();

  rados_write_op_omap_set(writeOp, keys, values, lengths, 1);

  rados_write_op_append(writeOp, contents.c_str(), contents.length());

  if (xattrKey != "")
  {
    if (xattrValue != "")
    {
      rados_write_op_setxattr(writeOp, xattrKey.c_str(), xattrValue.c_str(),
                              xattrValue.length());
    }
    else
    {
      rados_write_op_rmxattr(writeOp, xattrKey.c_str());
    }
  }

  int ret = rados_write_op_operate(writeOp, ioctx, obj.c_str(), NULL, 0);

  rados_release_write_op(writeOp);

  return ret;
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
getDirPath(const std::string &path)
{
  std::string dir(path);

  if (dir[dir.length() - 1] != PATH_SEP)
    dir += PATH_SEP;

  return dir;
}

std::string
getFilePath(const std::string &path)
{
  std::string file(path);

  if (file != "" && file[file.length() - 1] == PATH_SEP)
    file.erase(file.length() - 1, 1);

  return file;
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

  if (length == 0)
    return -EINVAL;

  char *buff = new char[length];
  ret = rados_getxattr(ioctx, path.c_str(), attrName.c_str(), buff, length);

  if (ret >= 0)
    value = std::string(buff, ret);

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

    if (value != 0)
      map[attr] = std::string(value, len);
  }

  rados_getxattrs_end(iter);

  return ret;
}

int
splitToken(const std::string &line,
           int startPos,
           std::string &key,
           std::string &value,
           std::string *op)
{
  std::string token("");
  bool gotKey(false);
  bool quoteOpened(false);

  size_t i = startPos;

  for (; i < line.length(); i++)
  {
    if (line[i] == '"' && i > 1 && line[i - 1] != '\\')
    {
      if (quoteOpened)
      {
        i++;
        quoteOpened = false;

        if (gotKey)
          break;
      }
      else
      {
        quoteOpened = true;
        continue;
      }
    }

    if (!quoteOpened)
    {
      if (line[i] == '=')
      {
        key = token;
        token = "";
        gotKey = true;
        quoteOpened = false;

        if (op)
          *op = "=";

        continue;
      }

      if (line[i] == ' ')
        continue;

      if (op != 0)
      {
        if (line[i] == '<' || line[i] == '>')
        {
          key = token;
          token = "";
          gotKey = true;
          quoteOpened = false;
          *op = line[i];

          if (i + 1 < line.length() && line[i + 1] == '=')
          {
            *op += "=";
            i++;
          }

          continue;
        }

        if (line[i] == '!')
        {
          if (i + 1 < line.length() && line[i + 1] == '=')
          {
            key = token;
            token = "";
            gotKey = true;
            quoteOpened = false;
            *op = "!=";
            i++;

            continue;
          }
        }
      }
    }

    token += line[i];
  }

  if (token != "")
  {
    if (gotKey)
      value = token;
    else
      key = token;
  }

  return i;
}

std::string
sanitizePath(const std::string &path)
{
  std::string sanitizedPath("");

  for (size_t i = 0; i < path.length(); i++)
  {
    if (i > 0 && (path[i] == PATH_SEP && path[i - 1] == PATH_SEP))
      continue;

    sanitizedPath += path[i];
  }

  if (sanitizedPath == "" || sanitizedPath[0] != PATH_SEP)
    sanitizedPath = PATH_SEP + sanitizedPath;

  return sanitizedPath;
}
