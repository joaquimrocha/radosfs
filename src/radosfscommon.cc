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
#include <sys/stat.h>
#include <uuid/uuid.h>

int
getPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    mode_t *mode,
                    uid_t *uid,
                    gid_t *gid)
{
  char permXAttr[XATTR_PERMISSIONS_LENGTH];
  *mode = 0;
  *uid = 0;
  *gid = 0;

  int ret = rados_getxattr(ioctx, obj, XATTR_PERMISSIONS,
                           permXAttr, XATTR_PERMISSIONS_LENGTH);

  if (ret < 0)
    return ret;

  permXAttr[ret] = '\0';

  std::map<std::string, std::string> attrs = stringAttrsToMap(permXAttr);

  if (attrs.count(XATTR_MODE) > 0)
  {
    *mode = (mode_t) strtoul(attrs[XATTR_MODE].c_str(), 0, 8);
  }

  if (attrs.count(XATTR_UID) > 0)
  {
    *uid = (uid_t) atoi(attrs[XATTR_UID].c_str());
  }

  if (attrs.count(XATTR_GID) > 0)
  {
    *gid = (gid_t) atoi(attrs[XATTR_GID].c_str());
  }

  return 0;
}

std::string
makePermissionsXAttr(long int mode,
                     uid_t uid,
                     gid_t gid)
{
  std::ostringstream convert;

  convert << XATTR_MODE << "=";
  convert << std::oct << mode;

  convert << " " << XATTR_UID << "=";
  convert << std::dec << uid;

  convert << " " << XATTR_GID << "=";
  convert << std::dec << gid;

  return convert.str();
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
genericStat(rados_ioctx_t ioctx,
            const std::string &object,
            struct stat* buff)
{
  uint64_t psize;
  time_t pmtime;
  int ret;
  uid_t uid = 0;
  gid_t gid = 0;
  mode_t permissions = 0;

  ret = rados_stat(ioctx, object.c_str(), &psize, &pmtime);

  if (ret != 0)
    return ret;

  ret = getPermissionsXAttr(ioctx, object.c_str(), &permissions, &uid, &gid);

  buff->st_dev = 0;
  buff->st_ino = hash(object.c_str());
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
getInodeAndPool(rados_ioctx_t ioctx,
                const std::string &path,
                std::string &inode,
                std::string &pool)
{
  char inodeXAttr[XATTR_FILE_LENGTH + 1];

  int ret = rados_getxattr(ioctx, path.c_str(), XATTR_INODE, inodeXAttr,
                           XATTR_FILE_LENGTH);

  if (ret < 0)
  {
    return ret;
  }

  inodeXAttr[ret] = '\0';

  ret = 0;

  std::map<std::string, std::string> attrs = stringAttrsToMap(inodeXAttr);

  if (attrs.count(XATTR_POOL) == 0)
  {
    return -ENODATA;
  }

  if (attrs.count(XATTR_LINK) == 0)
  {
    return -ENODATA;
  }

  pool = attrs[XATTR_POOL];
  inode = attrs[XATTR_LINK];

  return ret;
}

int
statFromXAttr(const std::string &path,
              const std::string &xattrValue,
              struct stat *buff,
              std::string &link,
              std::string &pool,
              std::map<std::string, std::string> &extraData)
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
    if (key == XATTR_LINK)
    {
      link = value;
    }
    else if (key == XATTR_MODE)
    {
      permissions = (mode_t) strtoul(value.c_str(), 0, 8);
    }
    else if (key == XATTR_UID)
    {
      uid = (uid_t) atoi(value.c_str());
    }
    else if (key == XATTR_GID)
    {
      gid = (gid_t) atoi(value.c_str());
    }
    else if (key == XATTR_TIME)
    {
      pmtime = (time_t) strtoul(value.c_str(), 0, 10);
    }
    else if (key == XATTR_POOL)
    {
      pool = value;
    }
    else if (key != "")
    {
      extraData[key] = value;
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

std::map<std::string, std::string>
stringAttrsToMap(const std::string &attrs)
{
  std::map<std::string, std::string> attrsMap;
  int startPos = 0, lastPos = 0;
  std::string key, value;

  while ((lastPos = splitToken(attrs, startPos, key, value)) != startPos)
  {
    if (key != "")
      attrsMap[key] = value;

    startPos = lastPos;
    key = value = "";
  }

  return attrsMap;
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

int indexObject(const RadosFsStat *parentStat,
                const RadosFsStat *stat,
                char op)
{
  std::string contents;
  std::string xAttrKey(""), xAttrValue("");

  if (parentStat->translatedPath == "")
    return 0;

  const std::string &baseName = stat->path.substr(parentStat->path.length(),
                                                  std::string::npos);

  contents = getObjectIndexLine(baseName, op);

  if ((stat->statBuff.st_mode & S_IFDIR) == 0)
  {
    xAttrKey = XATTR_FILE_PREFIX + baseName;

    if (op == '+')
      xAttrValue = getFileXAttrDirRecord(stat);
  }

  return writeContentsAtomically(parentStat->pool->ioctx,
                                 parentStat->translatedPath, contents,
                                 xAttrKey, xAttrValue);
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

std::string
getFileXAttrDirRecord(const RadosFsStat *stat)
{
  std::ostringstream stream;

  stream << XATTR_LINK "=\"" << stat->translatedPath << "\"";

  if (stat->translatedPath != "" && stat->translatedPath[0] != PATH_SEP)
  {
    stream << XATTR_POOL << "='" << stat->pool->name << "' ";
  }

  stream << " " << XATTR_UID << "=\"" << stat->statBuff.st_uid << "\" ";
  stream << XATTR_GID << "=\"" << stat->statBuff.st_gid << "\" ";
  stream << XATTR_TIME "=" << "\""  << stat->statBuff.st_ctime << "\" " ;
  stream << XATTR_MODE << "=\"" << std::oct << stat->statBuff.st_mode << "\" ";

  std::map<std::string, std::string>::const_iterator it;
  for (it = stat->extraData.begin(); it != stat->extraData.end(); it++)
  {
    stream << (*it).first << "='" << (*it).second << "' ";
  }

  return stream.str();
}

int
indexObjectMetadata(rados_ioctx_t ioctx,
                    const std::string &dirName,
                    const std::string &baseName,
                    std::map<std::string, std::string> &metadata,
                    char op)
{
  std::string contents;

  if (dirName == "")
    return 0;

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

  if (file != "" && isDirPath(file))
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
  const size_t usrPrefixSize = strlen(XATTR_USER_PREFIX);

  while ((ret = rados_getxattrs_next(iter, &attr, &value, &len)) == 0)
  {
    if (attr == 0)
      break;

    bool hasSysPrefix = strncmp(attr, XATTR_SYS_PREFIX, sysPrefixSize) == 0;

    // Only include xattrs that have a usr or sys prefixes (for the latter, only
    // include them if user is root)
    if (hasSysPrefix)
    {
      if (uid != ROOT_UID)
        continue;
    }
    else if (strncmp(attr, XATTR_USER_PREFIX, usrPrefixSize) != 0)
    {
      continue;
    }

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
  char quoteFound('\0');

  size_t i = startPos;

  for (; i < line.length(); i++)
  {
    if ((line[i] == '"' || line[i] == '\'') && i > 1 && line[i - 1] != '\\')
    {
      if (quoteFound == '\0')
      {
        quoteFound = line[i];
        continue;
      }

      if (quoteFound == line[i])
      {
        i++;
        quoteFound = '\0';

        if (gotKey)
          break;
      }
    }

    if (quoteFound == '\0')
    {
      if (line[i] == '=')
      {
        key = token;
        token = "";
        gotKey = true;
        quoteFound = '\0';

        if (op)
          *op = "=";

        continue;
      }

      if (line[i] == ' ')
      {
        if (token != "" && gotKey)
            break;

        continue;
      }

      if (op != 0)
      {
        if (line[i] == '<' || line[i] == '>')
        {
          key = token;
          token = "";
          gotKey = true;
          quoteFound = '\0';
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
            quoteFound = '\0';
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

std::string
makeFileStripeName(const std::string &filePath, size_t stripeIndex)
{
  if (stripeIndex == 0)
    return filePath;

  char stripeNumHex[FILE_STRIPE_LENGTH];
  sprintf(stripeNumHex, "%0*x", FILE_STRIPE_LENGTH, (unsigned int) stripeIndex);

  std::ostringstream stream;
  stream << filePath << "//" << stripeNumHex;

  return stream.str();
}

bool
nameIsStripe(const std::string &name)
{
  const size_t nameLength = name.length();

  // we add 2 because of the // that comes before the stripe index
  if (nameLength < FILE_STRIPE_LENGTH + 2)
    return false;

  return name[nameLength - FILE_STRIPE_LENGTH - 1] == PATH_SEP &&
      name[nameLength - FILE_STRIPE_LENGTH - 2] == PATH_SEP;
}

bool
isDirPath(const std::string &path)
{
  return path[path.length() - 1] == PATH_SEP;
}

std::string
generateInode()
{
  uuid_t inode;
  char inodeStr[UUID_STRING_SIZE + 1];

  uuid_generate(inode);
  uuid_unparse(inode, inodeStr);

  return inodeStr;
}

int
createDirAndInode(const RadosFsStat *stat)
{
  int ret = createDirObject(stat);

  if (ret != 0)
  {
    return ret;
  }

  rados_write_op_t writeOp = rados_create_write_op();

  rados_write_op_setxattr(writeOp, XATTR_INODE_HARD_LINK, stat->path.c_str(),
                          stat->path.length());

  const std::string &permissions = makePermissionsXAttr(stat->statBuff.st_mode,
                                                        stat->statBuff.st_uid,
                                                        stat->statBuff.st_gid);

  rados_write_op_setxattr(writeOp, XATTR_PERMISSIONS, permissions.c_str(),
                          permissions.length());

  ret = rados_write_op_operate(writeOp, stat->pool->ioctx,
                               stat->translatedPath.c_str(), NULL, 0);

  rados_release_write_op(writeOp);

  return ret;
}

int
createDirObject(const RadosFsStat *stat)
{
  std::stringstream stream;
  rados_write_op_t writeOp = rados_create_write_op();

  stream << XATTR_LINK << "='" << stat->translatedPath << "' ";
  stream << XATTR_POOL << "='" << stat->pool->name << "'";

  const std::string &inodeXAttr = stream.str();

  rados_write_op_setxattr(writeOp, XATTR_INODE, inodeXAttr.c_str(),
                          inodeXAttr.length());

  int ret = rados_write_op_operate(writeOp, stat->pool->ioctx,
                                   stat->path.c_str(), NULL, 0);

  rados_release_write_op(writeOp);

  return ret;
}
