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
#include "RadosFsDir.hh"
#include "RadosFsDirPriv.hh"
#include "RadosFsPriv.hh"
#include "RadosFsFinder.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsDirPriv::RadosFsDirPriv(RadosFsDir *dirObj)
  : dir(dirObj),
    target(0),
    cacheable(true)
{
  updatePath();
}

RadosFsDirPriv::RadosFsDirPriv(RadosFsDir *dirObj, bool useCache)
  : dir(dirObj),
    target(0),
    cacheable(useCache)
{
  updatePath();
}

RadosFsDirPriv::~RadosFsDirPriv()
{
  delete target;
}

void
RadosFsDirPriv::updatePath()
{
  const std::string &dirPath = dir->path();

  parentDir = getParentDir(dirPath, 0);

  if (target)
  {
    delete target;
    target = 0;
  }

  if (dir->isLink())
  {
    target = new RadosFsDir(dir->filesystem(), dir->targetPath());
  }
  else
  {
    updateDirInfoPtr();
  }
}

bool
RadosFsDirPriv::updateDirInfoPtr()
{
  if (dir->exists() && !dir->isLink())
  {
    dirInfo = dir->filesystem()->mPriv->getDirInfo(fsStat()->translatedPath,
                                                   fsStat()->pool,
                                                   cacheable);

    return true;
  }

  dirInfo.reset();

  return false;
}

void
RadosFsDirPriv::updateFsDirCache()
{
  if (dir->exists())
    dir->filesystem()->mPriv->updateDirCache(dirInfo);
  else
    dir->filesystem()->mPriv->removeDirCache(dirInfo);
}

const RadosFsPoolSP
RadosFsDirPriv::getPool()
{
  return dir->filesystem()->mPriv->getMetadataPoolFromPath(dir->path());
}

int
RadosFsDirPriv::makeDirsRecursively(RadosFsStat *stat,
                                    const char *path,
                                    uid_t uid,
                                    gid_t gid)
{
  int index;
  int ret = 0;
  const std::string dirPath = getDirPath(path);
  const std::string parentDir = getParentDir(path, &index);
  RadosFsPriv *radosFsPriv = dir->filesystem()->mPriv;
  struct stat *buff;

  if (parentDir == "")
    return -ENODEV;

  if (radosFsPriv->stat(dirPath.c_str(), stat) == 0)
  {
    buff = &stat->statBuff;

    if (buff->st_mode & S_IFDIR)
      return 0;

    if (buff->st_mode & S_IFREG)
      return -ENOTDIR;
  }

  ret = makeDirsRecursively(stat, parentDir.c_str(), uid, gid);

  if (ret == 0)
  {
    RadosFsStat parentStat;
    radosFsPriv->stat(parentDir.c_str(), &parentStat);

    buff = &stat->statBuff;

    if (buff->st_mode & S_IFREG)
      return -ENOTDIR;

    if (ret != 0)
      return ret;

    if (!statBuffHasPermission(*buff, uid, gid, O_WRONLY | O_RDWR))
      return -EACCES;

    std::string dir = getDirPath(path);

    *stat = parentStat;
    stat->path = dir;
    stat->translatedPath = generateInode();

    ret = createDirAndInode(stat);

    if (ret != 0)
      return ret;

    indexObject(&parentStat, stat, '+');
  }

  return ret;
}

int
RadosFsDirPriv::find(std::set<std::string> &entries,
                     std::set<std::string> &results,
                     const std::map<RadosFsFinder::FindOptions, FinderArg> &args)
{
  int ret = 0;
  pthread_mutex_t mutex;
  pthread_cond_t cond;

  pthread_mutex_init(&mutex, 0);
  pthread_cond_init(&cond, 0);

  int numRelatedJobs = 0;
  std::set<std::string>::iterator it;
  std::vector<FinderData *> jobs;

  for (it = entries.begin(); it != entries.end(); it++)
  {
    const std::string &entry = *it;

    if (entry[entry.length() - 1] != PATH_SEP)
      continue;

    FinderData *data = new FinderData;

    pthread_mutex_lock(&mutex);

    data->dir = entry;
    data->mutex = &mutex;
    data->cond = &cond;
    data->args = &args;
    data->retCode = &ret;
    numRelatedJobs++;
    data->numberRelatedJobs = &numRelatedJobs;

    pthread_mutex_unlock(&mutex);

    jobs.push_back(data);

    dir->filesystem()->mPriv->finder.find(data);
  }

  entries.clear();

  if (jobs.size() == 0)
    return 0;

  pthread_mutex_lock(&mutex);

  if (numRelatedJobs > 0)
    pthread_cond_wait(&cond, &mutex);

  pthread_mutex_unlock(&mutex);

  std::vector<FinderData *>::iterator vit;
  for (vit = jobs.begin(); vit != jobs.end(); vit++)
  {
    FinderData *data = *vit;
    entries.insert(data->dirEntries.begin(), data->dirEntries.end());
    results.insert(data->results.begin(), data->results.end());

    delete data;
  }

  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&cond);

  return ret;
}

RadosFsStat *
RadosFsDirPriv::fsStat(void)
{
  return reinterpret_cast<RadosFsStat *>(dir->fsStat());
}

RadosFsPriv *
RadosFsDirPriv::radosFsPriv(void)
{
  return dir->filesystem()->mPriv;
}

RadosFsDir::RadosFsDir(RadosFs *radosFs, const std::string &path)
  : RadosFsInfo(radosFs, getDirPath(path.c_str())),
    mPriv(new RadosFsDirPriv(this))
{}

RadosFsDir::RadosFsDir(const RadosFsDir &otherDir)
  : RadosFsInfo(otherDir),
    mPriv(new RadosFsDirPriv(this))
{}

RadosFsDir::RadosFsDir(const RadosFsDir *otherDir)
  : RadosFsInfo(*otherDir),
    mPriv(new RadosFsDirPriv(this))
{}

RadosFsDir::RadosFsDir(RadosFs *radosFs,
                       const std::string &path,
                       bool cacheable)
  : RadosFsInfo(radosFs, getDirPath(path.c_str())),
    mPriv(new RadosFsDirPriv(this, cacheable))
{}

RadosFsDir::~RadosFsDir()
{}

RadosFsDir &
RadosFsDir::operator=(const RadosFsDir &otherDir)
{
  if (this != &otherDir)
  {
    setPath(otherDir.path());
  }

  return *this;
}

std::string
RadosFsDir::getParent(const std::string &path, int *pos)
{
  return getParentDir(path, pos);
}

int
RadosFsDir::entryList(std::set<std::string> &entries)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->entryList(entries);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  if (!mPriv->dirInfo && !mPriv->updateDirInfoPtr())
    return -ENOENT;

  if (!isReadable())
    return -EACCES;

  const std::set<std::string> &contents = mPriv->dirInfo->contents();
  entries.insert(contents.begin(), contents.end());

  return 0;
}

int
RadosFsDir::create(int mode,
                   bool mkpath,
                   int owner,
                   int group)
{
  int ret;
  const std::string &dir = path();
  RadosFs *radosFs = filesystem();

  if (exists())
  {
    if (isFile())
      return -ENOTDIR;

    if (mkpath)
      return 0;

    return -EEXIST;
  }

  const RadosFsPoolSP pool = mPriv->getPool();

  if (!pool)
    return -ENODEV;

  uid_t uid = radosFs->uid();
  gid_t gid = radosFs->gid();

  if (owner < 0)
    owner = uid;

  if (group < 0)
    group = gid;

  mode_t permOctal = DEFAULT_MODE_DIR;

  if (mode >= 0)
    permOctal = mode | S_IFDIR;

  RadosFsStat stat, parentStat;

  if (mkpath)
  {
    ret = mPriv->makeDirsRecursively(&parentStat, mPriv->parentDir.c_str(), uid,
                                     gid);

    if (ret != 0)
      return ret;
  }
  else
  {
    ret = mPriv->radosFsPriv()->stat(mPriv->parentDir, &parentStat);

    if (ret != 0)
      return ret;
  }

  if (!statBuffHasPermission(parentStat.statBuff, uid, gid, O_WRONLY | O_RDWR))
    return -EACCES;

  stat = parentStat;

  stat.path = dir;
  stat.translatedPath = generateInode();
  stat.statBuff.st_mode = permOctal;
  stat.statBuff.st_uid = owner;
  stat.statBuff.st_gid = group;
  stat.pool = pool;

  ret = createDirAndInode(&stat);

  if (ret != 0)
  {
    radosfs_debug("Problem setting inode in dir %s: %s", stat.path.c_str(),
                  strerror(abs(ret)));
    return ret;
  }

  indexObject(&parentStat, &stat, '+');

  RadosFsInfo::update();
  mPriv->updateDirInfoPtr();

  return 0;
}

int
RadosFsDir::remove()
{
  int ret;
  const std::string &dirPath = path();
  RadosFs *radosFs = filesystem();
  RadosFsStat stat, *statPtr;

  RadosFsInfo::update();

  ret = mPriv->radosFsPriv()->stat(mPriv->parentDir, &stat);

  if (ret != 0)
    return ret;

  if (!statBuffHasPermission(stat.statBuff,
                             radosFs->uid(),
                             radosFs->gid(),
                             O_WRONLY | O_RDWR))
    return -EACCES;

  if (!exists())
    return -ENOENT;

  if (isFile())
    return -ENOTDIR;

  statPtr = reinterpret_cast<RadosFsStat *>(fsStat());

  DirCache *info = 0;

  if (!isLink())
  {
    info = mPriv->dirInfo.get();

    info->update();

    if (info->getEntry(0) != "")
      return -ENOTEMPTY;

    ret = rados_remove(statPtr->pool->ioctx, dirPath.c_str());

    if (ret == 0)
    {
     ret = rados_remove(info->ioctx(), statPtr->translatedPath.c_str());

     mPriv->radosFsPriv()->removeDirInode(path());
    }
  }

  if (ret == 0)
    indexObject(&stat, statPtr, '-');

  RadosFsInfo::update();

  if (info)
    mPriv->updateFsDirCache();

  mPriv->updateDirInfoPtr();

  return ret;
}

void
RadosFsDir::update()
{
  RadosFsInfo::update();

  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->update();

    radosfs_debug("No target for link %s", path().c_str());
    return;
  }

  if (mPriv->dirInfo || mPriv->updateDirInfoPtr())
  {
    mPriv->dirInfo->update();

    const float ratio = mPriv->dirInfo->logRatio();
    if (ratio != -1 && ratio <= filesystem()->dirCompactRatio())
      compact();

    if (mPriv->cacheable)
      mPriv->updateFsDirCache();
  }
}

int
RadosFsDir::entry(int entryIndex, std::string &entry)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->entry(entryIndex, entry);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  if (!mPriv->dirInfo && !mPriv->updateDirInfoPtr())
    return -ENOENT;

  if (!isReadable())
    return -EACCES;

  entry = mPriv->dirInfo->getEntry(entryIndex);

  return 0;
}

void
RadosFsDir::setPath(const std::string &path)
{
  const std::string &dirPath = getDirPath(path.c_str());

  if (dirPath == this->path())
    return;

  mPriv->dirInfo.reset();

  RadosFsInfo::setPath(dirPath);

  mPriv->updatePath();
}

bool
RadosFsDir::isWritable()
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->isWritable();

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  RadosFs *radosFs = filesystem();
  uid_t uid = radosFs->uid();
  gid_t gid = radosFs->gid();

  if (!mPriv->dirInfo)
    return false;

  return statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid, O_WRONLY);
}

bool
RadosFsDir::isReadable()
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->isReadable();

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  RadosFs *radosFs = filesystem();
  uid_t uid = radosFs->uid();
  gid_t gid = radosFs->gid();

  if (!mPriv->dirInfo)
    return false;

  return statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid, O_RDONLY);
}

int
RadosFsDir::stat(struct stat *buff)
{
  return RadosFsInfo::stat(buff);
}

int
RadosFsDir::compact()
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->compact();

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  if (mPriv->dirInfo)
  {
    mPriv->dirInfo->compactDirOpLog();
    return 0;
  }

  return -1;
}

int
RadosFsDir::setMetadata(const std::string &entry,
                        const std::string &key,
                        const std::string &value)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->setMetadata(entry, key, value);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  if (key == "")
    return -EINVAL;

  update();

  if (!isWritable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    if (mPriv->dirInfo->hasEntry(entry))
    {
      std::map<std::string, std::string> metadata;
      metadata[key] = value;

      return indexObjectMetadata(mPriv->dirInfo->ioctx(),
                                 mPriv->dirInfo->inode(), entry, metadata, '+');
    }

    return -ENOENT;
  }

  return -1;
}

int
RadosFsDir::getMetadata(const std::string &entry,
                        const std::string &key,
                        std::string &value)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->getMetadata(entry, key, value);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  if (key == "")
    return -EINVAL;

  update();

  if (!isReadable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    return mPriv->dirInfo->getMetadata(entry, key, value);
  }

  return -1;
}

int
RadosFsDir::removeMetadata(const std::string &entry, const std::string &key)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->removeMetadata(entry, key);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  if (key == "")
    return -EINVAL;

  update();

  if (!isWritable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    std::string value;

    if (mPriv->dirInfo->hasEntry(entry) &&
        mPriv->dirInfo->getMetadata(entry, key, value) == 0)
    {
      std::map<std::string, std::string> metadata;
      metadata[key] = "";

      return indexObjectMetadata(mPriv->dirInfo->ioctx(),
                                 mPriv->dirInfo->inode(), entry, metadata, '-');
    }

    return -ENOENT;
  }

  return -1;
}

int
RadosFsDir::find(std::set<std::string> &results, const std::string args)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->find(results, args);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  int ret = 0;
  std::set<std::string> dirs, files, entries;
  std::map<RadosFsFinder::FindOptions, FinderArg> finderArgs;

  entries.insert(path());

  int startPos = 0, lastPos = 0;
  std::string key, value, op;

  while ((lastPos = splitToken(args, startPos, key, value, &op)) != startPos)
  {
    if (key == "")
      break;

    FinderArg arg;
    arg.valueStr = "";
    RadosFsFinder::FindOptions option;

    bool isIName = key == FINDER_KEY_INAME;
    if (key == FINDER_KEY_NAME || isIName)
    {
      arg.valueStr = value;

      option = RadosFsFinder::FIND_NAME_EQ;

      if (op == FINDER_NE_SYM)
        option = RadosFsFinder::FIND_NAME_NE;

      if (isIName)
        arg.valueInt = 1;
      else
        arg.valueInt = 0;
    }
    else if (key == FINDER_KEY_SIZE)
    {
      arg.valueInt = atoi(value.c_str());

      if (op == FINDER_EQ_SYM)
        option = RadosFsFinder::FIND_SIZE_EQ;
      else if (op == FINDER_NE_SYM)
        option = RadosFsFinder::FIND_SIZE_NE;
      else if (op == FINDER_GE_SYM)
        option = RadosFsFinder::FIND_SIZE_GE;
      else if (op == FINDER_GT_SYM)
        option = RadosFsFinder::FIND_SIZE_GT;
      else if (op == FINDER_LE_SYM)
        option = RadosFsFinder::FIND_SIZE_LE;
      else if (op == FINDER_LT_SYM)
        option = RadosFsFinder::FIND_SIZE_LT;
    }

    finderArgs[option] = arg;

    startPos = lastPos;
    key = value = "";
  }

  if (finderArgs.size() == 0)
    return -EINVAL;

  while (entries.size() != 0 &&
         ((ret = mPriv->find(entries, results, finderArgs)) == 0))
  {}

  return ret;
}

int
RadosFsDir::chmod(long int permissions)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->chmod(permissions);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  int ret = 0;
  long int mode;

  if (!exists())
    return -ENOENT;

  if (!isWritable())
    return -EPERM;

  mode = permissions | S_IFDIR;

  RadosFsStat stat = *reinterpret_cast<RadosFsStat *>(fsStat());
  RadosFsStat *parentStat = reinterpret_cast<RadosFsStat *>(parentFsStat());

  if (isLink())
  {
    stat.statBuff.st_mode = mode;
    const std::string &baseName = path().substr(mPriv->parentDir.length());
    const std::string &linkXAttr = getFileXAttrDirRecord(&stat);

    ret = rados_setxattr(stat.pool->ioctx, parentStat->translatedPath.c_str(),
                         (XATTR_FILE_PREFIX + baseName).c_str(),
                         linkXAttr.c_str(), linkXAttr.length());
  }
  else
  {
    const std::string &permissionsXattr = makePermissionsXAttr(mode,
                                                          stat.statBuff.st_uid,
                                                          stat.statBuff.st_gid);

    ret = rados_setxattr(stat.pool->ioctx, stat.translatedPath.c_str(),
                         XATTR_PERMISSIONS, permissionsXattr.c_str(),
                         permissionsXattr.length());
  }

  return ret;
}

RADOS_FS_END_NAMESPACE
