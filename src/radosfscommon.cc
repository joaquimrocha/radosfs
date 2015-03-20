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
#include <climits>
#include <sys/stat.h>
#include <uuid/uuid.h>

int
getPermissionsXAttr(const std::string &permXAttr,
                    mode_t *mode,
                    uid_t *uid,
                    gid_t *gid)
{
  *mode = 0;
  *uid = 0;
  *gid = 0;

  std::map<std::string, std::string> attrs = stringAttrsToMap(permXAttr);

  if (attrs.count(MODE_KEY) > 0)
  {
    *mode = (mode_t) strtoul(attrs[MODE_KEY].c_str(), 0, 8);
  }

  if (attrs.count(UID_KEY) > 0)
  {
    *uid = (uid_t) atoi(attrs[UID_KEY].c_str());
  }

  if (attrs.count(GID_KEY) > 0)
  {
    *gid = (gid_t) atoi(attrs[GID_KEY].c_str());
  }

  return 0;
}

std::string
makePermissionsXAttr(long int mode,
                     uid_t uid,
                     gid_t gid)
{
  std::ostringstream convert;

  convert << MODE_KEY << "=";
  convert << std::oct << mode;

  convert << " " << UID_KEY << "=";
  convert << std::dec << uid;

  convert << " " << GID_KEY << "=";
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
genericStat(librados::IoCtx &ctx,
            const std::string &object,
            struct stat* buff)
{
  uint64_t psize;
  time_t pmtime;
  int ret, statRet;
  std:: string ctime, mtime, permissions;
  librados::ObjectReadOperation op;
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;

  keys.insert(XATTR_PERMISSIONS);
  keys.insert(XATTR_MTIME);
  keys.insert(XATTR_CTIME);

  op.stat(&psize, &pmtime, &statRet);
  op.omap_get_vals_by_keys(keys, &omap, 0);
  op.assert_exists();

  ret = ctx.operate(object, &op, 0);

  if (ret != 0)
    return ret;

  if (statRet != 0)
    return statRet;

  if (omap.count(XATTR_PERMISSIONS) > 0)
  {
    librados::bufferlist permXAttr = omap[XATTR_PERMISSIONS];
    permissions = std::string(permXAttr.c_str(), permXAttr.length());
  }
  else
  {
    return -ENODATA;
  }

  if (omap.count(XATTR_CTIME) > 0)
  {
    librados::bufferlist ctimeXAttr = omap[XATTR_CTIME];
    ctime = std::string(ctimeXAttr.c_str(), ctimeXAttr.length());
  }

  if (omap.count(XATTR_MTIME) > 0)
  {
    librados::bufferlist mtimeXAttr = omap[XATTR_MTIME];
    mtime = std::string(mtimeXAttr.c_str(), mtimeXAttr.length());
  }

  genericStatFromAttrs(object, permissions, ctime, mtime, psize, pmtime, buff);

  return statRet;
}

void
genericStatFromAttrs(const std::string &object,
                     const std::string &permXAttr,
                     const std::string &ctimeXAttr,
                     const std::string &mtimeXAttr,
                     u_int64_t psize,
                     time_t pmtime,
                     struct stat* buff)
{
  uid_t uid = 0;
  gid_t gid = 0;
  mode_t permissions = 0;

  getPermissionsXAttr(permXAttr.c_str(), &permissions, &uid, &gid);

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
  buff->st_ctime = pmtime;
  buff->st_mtime = pmtime;

  if (ctimeXAttr != "")
  {
    strToTimespec(ctimeXAttr, &buff->st_ctim);
    buff->st_ctime = buff->st_ctim.tv_sec;
  }

  if (mtimeXAttr != "")
  {
    strToTimespec(mtimeXAttr, &buff->st_mtim);
    buff->st_mtime = buff->st_mtim.tv_sec;
  }

  buff->st_atime = buff->st_mtime;
}

int
getInodeAndPool(librados::IoCtx &ioctx,
                const std::string &path,
                std::string &inode,
                std::string &pool)
{
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;

  keys.insert(XATTR_INODE);
  int ret = ioctx.omap_get_vals_by_keys(path, keys, &omap);

  if (ret < 0)
  {
    return ret;
  }

  ret = 0;

  librados::bufferlist inodeXAttr = omap[XATTR_INODE];
  std::string inodeValue(inodeXAttr.c_str(), inodeXAttr.length());
  std::map<std::string, std::string> attrs = stringAttrsToMap(inodeValue);

  if (attrs.count(POOL_KEY) == 0)
  {
    return -ENODATA;
  }

  if (attrs.count(LINK_KEY) == 0)
  {
    return -ENODATA;
  }

  pool = attrs[POOL_KEY];
  inode = attrs[LINK_KEY];

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
  timespec ctime;
  uid_t uid = 0;
  gid_t gid = 0;
  mode_t permissions = DEFAULT_MODE_FILE;
  std::string realPath(path);

  int startPos = 0, lastPos = 0;
  std::string key, value;

  while ((lastPos = splitToken(xattrValue, startPos, key, value)) != startPos)
  {
    if (key == LINK_KEY)
    {
      link = value;
    }
    else if (key == MODE_KEY)
    {
      permissions = (mode_t) strtoul(value.c_str(), 0, 8);
    }
    else if (key == UID_KEY)
    {
      uid = (uid_t) atoi(value.c_str());
    }
    else if (key == GID_KEY)
    {
      gid = (gid_t) atoi(value.c_str());
    }
    else if (key == TIME_KEY)
    {
      strToTimespec(value, &ctime);
    }
    else if (key == POOL_KEY)
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
  buff->st_ctim = ctime;
  buff->st_mtim = ctime;
  buff->st_ctime = ctime.tv_sec;
  buff->st_mtime = ctime.tv_sec;

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
  size_t length = path.length();
  size_t index = path.rfind(PATH_SEP, length - 2);

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
  const size_t length = obj.length();

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

int indexObject(const Stat *parentStat,
                const Stat *stat,
                char op)
{
  std::string contents;
  std::string xAttrKey(""), xAttrValue("");

  if (parentStat->translatedPath == "")
    return 0;

  const std::string &baseName = stat->path.substr(parentStat->path.length(),
                                                  std::string::npos);

  contents = getObjectIndexLine(baseName, op);

  std::map<std::string, librados::bufferlist> xattrs;

  if ((stat->statBuff.st_mode & S_IFDIR) == 0)
  {
    xAttrKey = XATTR_FILE_PREFIX + baseName;

    if (op == '+')
      xAttrValue = getFileXAttrDirRecord(stat);
    else
      xattrs[XATTR_FILE_INLINE_BUFFER + baseName].append("");

    xattrs[xAttrKey].append(xAttrValue);
  }

  int ret =  writeContentsAtomically(parentStat->pool->ioctx,
                                     parentStat->translatedPath, contents,
                                     &xattrs);

  updateTimeAsync(parentStat, XATTR_MTIME);

  return ret;
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
getFileXAttrDirRecord(const Stat *stat)
{
  std::ostringstream stream;

  stream << LINK_KEY "=\"" << stat->translatedPath << "\" ";

  if (stat->translatedPath != "" && stat->translatedPath[0] != PATH_SEP)
  {
    stream << POOL_KEY << "='" << stat->pool->name << "' ";
  }

  stream << " " << UID_KEY << "=\"" << stat->statBuff.st_uid << "\" ";
  stream << GID_KEY << "=\"" << stat->statBuff.st_gid << "\" ";
  stream << TIME_KEY "=\""  << timespecToStr(&stat->statBuff.st_ctim) << "\" " ;
  stream << MODE_KEY << "=\"" << std::oct << stat->statBuff.st_mode << "\" ";

  std::map<std::string, std::string>::const_iterator it;
  for (it = stat->extraData.begin(); it != stat->extraData.end(); it++)
  {
    stream << (*it).first << "='" << (*it).second << "' ";
  }

  return stream.str();
}

int
indexObjectMetadata(librados::IoCtx &ioctx,
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
writeContentsAtomically(librados::IoCtx &ioctx,
                      const std::string &obj,
                      const std::string &contents,
                      const std::map<std::string, librados::bufferlist> *xattrs)
{
  librados::ObjectWriteOperation writeOp;
  std::map<std::string, librados::bufferlist> omap;
  std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
  librados::bufferlist contentsBuff;

  omap[DIR_LOG_UPDATED].append(DIR_LOG_UPDATED_TRUE);

  contentsBuff.append(contents);
  writeOp.append(contentsBuff);

  std::set<std::string> keysToRemove;

  if (xattrs)
  {
    librados::bufferlist emptyStr;
    emptyStr.append("");

    std::pair<librados::bufferlist, int> cmp(emptyStr,
                                             LIBRADOS_CMPXATTR_OP_EQ);

    std::map<std::string, librados::bufferlist>::const_iterator it;
    for (it = xattrs->begin(); it != xattrs->end(); it++)
    {
      std::string xattrKey = (*it).first;
      librados::bufferlist xattrValue = (*it).second;

      if (xattrValue.length() == 0)
      {
        keysToRemove.insert(xattrKey);
        continue;
      }

      omap[xattrKey] = xattrValue;
      omapCmp[xattrKey] = cmp;
    }
  }

  writeOp.omap_cmp(omapCmp, 0);
  writeOp.omap_set(omap);
  writeOp.omap_rm_keys(keysToRemove);

  int ret = ioctx.operate(obj, &writeOp);

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
setXAttrFromPath(Stat &stat, uid_t uid, gid_t gid, const std::string &attrName,
                 const std::string &value)
{
  int ret = checkPermissionsForXAttr(stat.statBuff, attrName, uid, gid,
                                     O_WRONLY);

  if (ret != 0)
    return ret;

  librados::ObjectWriteOperation writeOp;

  std::map<std::string, librados::bufferlist> omap;
  omap[attrName].append(value);
  writeOp.omap_set(omap);

  ret = stat.pool->ioctx.operate(stat.translatedPath, &writeOp);

  if (S_ISREG(stat.statBuff.st_mode) && ret == 0)
  {
    setInodeBacklinkAsync(stat.pool, stat.path, stat.translatedPath);
  }

  return ret;
}

int
getXAttrFromPath(librados::IoCtx &ioctx,
                 const struct stat &statBuff,
                 uid_t uid,
                 gid_t gid,
                 const std::string &path,
                 const std::string &attrName,
                 std::string &value)
{
  int ret = checkPermissionsForXAttr(statBuff, attrName, uid, gid, O_RDONLY);

  if (ret != 0)
    return ret;

  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;

  keys.insert(attrName);
  ret = ioctx.omap_get_vals_by_keys(path, keys, &omap);

  if (omap.count(attrName) == 0)
    ret = -ENODATA;

  if (ret < 0)
    return ret;

  librados::bufferlist buff = omap[attrName];
  value = std::string(buff.c_str(), buff.length());

  return value.length();
}

int removeXAttrFromPath(librados::IoCtx &ioctx,
                        const struct stat &statBuff,
                        uid_t uid,
                        gid_t gid,
                        const std::string &path,
                        const std::string &attrName)
{
  int ret = checkPermissionsForXAttr(statBuff, attrName, uid, gid, O_WRONLY);

  if (ret != 0)
    return ret;

  std::set<std::string> keys;
  keys.insert(attrName);

  return ioctx.omap_rm_keys(path, keys);
}

int getMapOfXAttrFromPath(librados::IoCtx &ioctx,
                          const struct stat &statBuff,
                          uid_t uid,
                          gid_t gid,
                          const std::string &path,
                          std::map<std::string, std::string> &map)
{
  if (!statBuffHasPermission(statBuff, uid, gid, O_RDONLY))
    return -EACCES;

  std::map<std::string, librados::bufferlist> attrs;

  int ret = ioctx.omap_get_vals(path, "", UINT_MAX, &attrs);

  if (ret != 0)
    return ret;

  const size_t sysPrefixSize = strlen(XATTR_SYS_PREFIX);
  const size_t usrPrefixSize = strlen(XATTR_USER_PREFIX);
  std::map<std::string, librados::bufferlist>::iterator it;

  for (it = attrs.begin(); it != attrs.end(); it++)
  {
    const std::string &name = (*it).first;
    librados::bufferlist value = (*it).second;

    bool hasSysPrefix = strncmp(name.c_str(), XATTR_SYS_PREFIX,
                                sysPrefixSize) == 0;

    // Only include xattrs that have a usr or sys prefixes (for the latter, only
    // include them if user is root)
    if (hasSysPrefix)
    {
      if (uid != ROOT_UID)
        continue;
    }
    else if (strncmp(name.c_str(), XATTR_USER_PREFIX, usrPrefixSize) != 0)
    {
      continue;
    }

    map[name] = std::string(value.c_str(), value.length());
  }

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
generateUuid()
{
  uuid_t inode;
  char inodeStr[UUID_STRING_SIZE + 1];

  uuid_generate(inode);
  uuid_unparse(inode, inodeStr);

  return inodeStr;
}

std::string
timespecToStr(const timespec *spec)
{
  std::stringstream stream;
  stream << spec->tv_sec << "." << spec->tv_nsec;

  return stream.str();
}

void
strToTimespec(const std::string &specStr, timespec *spec)
{
  if (specStr == "")
    return;

  std::string tv_sec, tv_nsec;

  for (size_t i = 0; i < specStr.length(); i++)
  {
    if (specStr[i] == '.')
    {
      tv_nsec = specStr.substr(++i);
      break;
    }

    tv_sec.append(1, specStr[i]);
  }

  spec->tv_sec = (time_t) strtoul(tv_sec.c_str(), 0, 10);
  spec->tv_nsec = (time_t) strtoul(tv_nsec.c_str(), 0, 10);
}

std::string
getCurrentTimeStr()
{
  timespec spec;
  clock_gettime(CLOCK_REALTIME, &spec);

  return timespecToStr(&spec);
}

int
createDirAndInode(const Stat *stat)
{
  int ret = createDirObject(stat);

  if (ret != 0)
  {
    return ret;
  }

  librados::ObjectWriteOperation writeOp;
  const std::string &timeSpec = timespecToStr(&stat->statBuff.st_ctim);
  const std::string &permissions = makePermissionsXAttr(stat->statBuff.st_mode,
                                                        stat->statBuff.st_uid,
                                                        stat->statBuff.st_gid);


  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_PERMISSIONS].append(permissions);
  omap[XATTR_CTIME].append(timeSpec);
  omap[XATTR_MTIME].append(timeSpec);
  omap[XATTR_INODE_HARD_LINK].append(stat->path);

  writeOp.create(true);
  writeOp.omap_set(omap);

  ret = stat->pool->ioctx.operate(stat->translatedPath, &writeOp);

  return ret;
}

int
createDirObject(const Stat *stat)
{
  std::stringstream stream;
  librados::ObjectWriteOperation writeOp;

  stream << LINK_KEY << "='" << stat->translatedPath << "' ";
  stream << POOL_KEY << "='" << stat->pool->name << "'";

  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_INODE].append(stream.str());

  writeOp.create(true);
  writeOp.omap_set(omap);

  int ret = stat->pool->ioctx.operate(stat->path, &writeOp);

  return ret;
}

ino_t
hash(const char *path)
{
  return hash64((ub1 *) path, strlen(path), 0);
}

void
updateTimeAsyncCB(rados_completion_t comp, void *arg)
{
  rados_aio_release(comp);
}

void
updateTimeAsync(const Stat *stat, const char *timeXAttrKey,
                const std::string &time)
{
  updateTimeAsync2(stat->pool, stat->translatedPath, timeXAttrKey, time);
}

void
updateTimeAsync2(const PoolSP &pool, const std::string &inode,
                 const char *timeXAttrKey,
                 const std::string &time)
{
  std::map<std::string, librados::bufferlist> omap;

  if (time == "")
    omap[timeXAttrKey].append(getCurrentTimeStr());
  else
    omap[timeXAttrKey].append(time);

  librados::ObjectWriteOperation op;

  op.omap_set(omap);

  rados_completion_t comp;

  rados_aio_create_completion(0, 0, updateTimeAsyncCB, &comp);
  librados::AioCompletion completion((librados::AioCompletionImpl *)comp);

  pool->ioctx.aio_operate(inode, &completion, &op);
}

int
getTimeFromXAttr(const Stat *stat, const std::string &xattr,
                 timespec *spec, time_t *basicTime)
{
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;

  keys.insert(xattr);
  int ret = stat->pool->ioctx.omap_get_vals_by_keys(stat->translatedPath, keys,
                                                    &omap);

  if (omap.count(xattr) == 0)
    ret = -ENODATA;

  if (ret < 0)
    return ret;

  librados::bufferlist buff = omap[xattr];
  std::string timeXAttr(buff.c_str(), buff.length());

  strToTimespec(timeXAttr, spec);

  if (basicTime)
    *basicTime = spec->tv_sec;

  return 0;
}

bool
hasTMTimeEnabled(mode_t mode)
{
  return (mode & TMTIME_MASK) != 0;
}

size_t alignStripeSize(size_t stripeSize, size_t alignment)
{
  if (alignment == 0 || stripeSize % alignment == 0)
    return stripeSize;

  return alignment * (stripeSize / alignment);
}

int statAndGetXAttrs(librados::IoCtx &ctx, const std::string &obj,
                     u_int64_t *size, time_t *mtime,
                     std::map<std::string, std::string> &xattrs)
{
  int statRet;
  librados::ObjectReadOperation op;
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;
  std::map<std::string, std::string>::iterator it;
  size_t i;

  for (it = xattrs.begin(), i = 0; it != xattrs.end(); it++, i++)
  {
    keys.insert((*it).first);
  }

  op.stat(size, mtime, &statRet);
  op.omap_get_vals_by_keys(keys, &omap, 0);

  ctx.operate(obj, &op, 0);

  for (it = xattrs.begin(); it != xattrs.end(); it++)
  {
    const std::string &key = (*it).first;

    if (omap.count(key) == 0)
      continue;

    librados::bufferlist *xattrValue = &omap[key];
    xattrs[key] = std::string(xattrValue->c_str(), xattrValue->length());
  }

  return statRet;
}

std::string
fileSizeToHex(size_t num)
{
  char stripeNumHex[XATTR_FILE_SIZE_LENGTH];
  sprintf(stripeNumHex, "%0*x", XATTR_FILE_SIZE_LENGTH, (unsigned int) num);

  return std::string(stripeNumHex, XATTR_FILE_SIZE_LENGTH);
}

void
updateInodeBacklinkAsyncCB(rados_completion_t comp, void *arg)
{
  rados_aio_release(comp);
}

void
setInodeBacklinkAsync(PoolSP pool, const std::string &backlink,
                      const std::string &inode, const std::string *compare,
                      rados_callback_t callback, void *arg)
{
  librados::bufferlist backlinkBl, compareBl;
  backlinkBl.append(backlink);

  if (compare)
    compareBl.append(*compare);

  librados::ObjectWriteOperation writeOp;
  writeOp.setxattr(XATTR_INODE_HARD_LINK, backlinkBl);
  writeOp.cmpxattr(XATTR_INODE_HARD_LINK, LIBRADOS_CMPXATTR_OP_EQ, compareBl);

  rados_completion_t comp;

  if (!callback)
    callback = updateInodeBacklinkAsyncCB;

  rados_aio_create_completion(arg, 0, callback, &comp);
  librados::AioCompletion completion((librados::AioCompletionImpl *) comp);

  pool->ioctx.aio_operate(inode, &completion, &writeOp);
}

int
moveLogicalFile(Stat &oldParent, Stat &newParent,
                const std::string &oldFilePath,
                const std::string &newFilePath)
{
  const std::string oldBaseName = oldFilePath.substr(oldParent.path.length());
  const std::string newBaseName = newFilePath.substr(newParent.path.length());
  const std::string oldFileEntry = XATTR_FILE_PREFIX + oldBaseName;
  const std::string oldInlineBufferEntry = XATTR_FILE_INLINE_BUFFER + oldBaseName;
  const std::string newFileEntry = XATTR_FILE_PREFIX + newBaseName;
  const std::string newInlineBufferEntry = XATTR_FILE_INLINE_BUFFER + newBaseName;
  std::set<std::string> omapKeys;
  std::map<std::string, librados::bufferlist> omapValues, newOmapValues;
  std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
  librados::bufferlist oldParentContents, newParentContents;

  omapKeys.insert(oldFileEntry);
  omapKeys.insert(oldInlineBufferEntry);

  int ret = oldParent.pool->ioctx.omap_get_vals_by_keys(oldParent.translatedPath,
                                                        omapKeys, &omapValues);

  if (ret != 0)
    return ret;

  std::map<std::string, librados::bufferlist>::iterator it;
  it = omapValues.find(oldFileEntry);

  // Get the contents of the file entry and the inline buffer into the omap
  // we'll set in the new parent

  if (it != omapValues.end())
  {
    newOmapValues[newFileEntry] = (*it).second;
  }

  it = omapValues.find(oldInlineBufferEntry);

  if (it != omapValues.end())
  {
    newOmapValues[newInlineBufferEntry] = (*it).second;
  }

  // Deindex the old file name in the old parent and index it in the new parent
  oldParentContents.append(getObjectIndexLine(oldBaseName, '-'));
  newParentContents.append(getObjectIndexLine(newBaseName, '+'));

  librados::ObjectWriteOperation newParentWriteOp;
  newParentWriteOp.append(newParentContents);
  newParentWriteOp.omap_set(newOmapValues);

  bool sameParent = oldParent.translatedPath == newParent.translatedPath;

  if (sameParent)
  {
    // If it is the same parent (a move inside the same directory), we only set
    // the new values if the old ones were untouched, for added consistency.
    for (it = omapValues.begin(); it != omapValues.end(); it++)
    {
      const std::string &key = (*it).first;

      librados::bufferlist compareInode((*it).second);
      std::pair<librados::bufferlist, int> cmp(compareInode,
                                               LIBRADOS_CMPXATTR_OP_EQ);
      omapCmp[key] = cmp;
    }

    newParentWriteOp.append(oldParentContents);
    newParentWriteOp.omap_rm_keys(omapKeys);

    if (omapCmp.size() > 0)
      newParentWriteOp.omap_cmp(omapCmp, 0);
  }

  ret = newParent.pool->ioctx.operate(newParent.translatedPath,
                                      &newParentWriteOp);

  if (ret == 0 && !sameParent)
  {
    // If we succeeded in moving the file's logical contents to the new parent
    // then we delete the old ones
    librados::ObjectWriteOperation oldParentWriteOp;
    oldParentWriteOp.append(oldParentContents);
    oldParentWriteOp.omap_rm_keys(omapKeys);

    ret = oldParent.pool->ioctx.operate(oldParent.translatedPath,
                                        &oldParentWriteOp);
  }

  return ret;
}
