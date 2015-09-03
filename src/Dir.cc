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

#include <sys/stat.h>

#include "radosfscommon.h"
#include "Dir.hh"
#include "DirPriv.hh"
#include "FilesystemPriv.hh"
#include "Finder.hh"

RADOS_FS_BEGIN_NAMESPACE

DirPriv::DirPriv(Dir *dirObj)
  : dir(dirObj),
    target(0),
    cacheable(true)
{
  updatePath();
}

DirPriv::DirPriv(Dir *dirObj, bool useCache)
  : dir(dirObj),
    target(0),
    cacheable(useCache)
{
  updatePath();
}

DirPriv::~DirPriv()
{
  delete target;
}

void
DirPriv::updatePath()
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
    target = new Dir(dir->filesystem(), dir->targetPath());
  }
  else
  {
    updateDirInfoPtr();
  }
}

bool
DirPriv::updateDirInfoPtr()
{
  if (dir->exists() && !dir->isLink() && !dir->isFile())
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
DirPriv::updateFsDirCache()
{
  if (dir->exists())
    dir->filesystem()->mPriv->updateDirCache(dirInfo);
  else
    dir->filesystem()->mPriv->removeDirCache(dirInfo);
}

const PoolSP
DirPriv::getPool()
{
  return dir->filesystem()->mPriv->getMetadataPoolFromPath(dir->path());
}

int
DirPriv::makeDirsRecursively(Stat *stat, const char *path, uid_t uid, gid_t gid)
{
  int index;
  int ret = 0;
  const std::string dirPath = getDirPath(path);
  const std::string parentDir = getParentDir(path, &index);
  FilesystemPriv *radosFsPriv = dir->filesystem()->mPriv;
  struct stat *buff;

  if (radosFsPriv->stat(dirPath.c_str(), stat) == 0)
  {
    buff = &stat->statBuff;

    if (buff->st_mode & S_IFDIR)
      return 0;

    if (buff->st_mode & S_IFREG)
      return -ENOTDIR;
  }

  if (parentDir == "")
    return -ENODEV;

  ret = makeDirsRecursively(stat, parentDir.c_str(), uid, gid);

  if (ret == 0)
  {
    Stat parentStat;
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
    stat->translatedPath = generateUuid();

    ret = createDirAndInode(stat);

    if (ret != 0)
      return ret;

    indexObject(&parentStat, stat, '+');
  }

  return ret;
}

void
findInThread(Finder *finder, FinderData *data, boost::mutex &mutex,
             boost::condition_variable &cond)
{
  int ret = finder->find(data);
  bool lastJob = false;

  {
    boost::unique_lock<boost::mutex> lock(mutex);
    if (data->retCode == 0 && ret != 0)
    {
      *data->retCode = ret;
    }

    lastJob = (--*data->numberRelatedJobs) == 0;
  }

  if (lastJob)
  {
    cond.notify_all();
  }
}

int
DirPriv::find(std::set<std::string> &entries, std::set<std::string> &results,
              const std::map<Finder::FindOptions, FinderArg> &args)
{
  int ret = 0;
  boost::mutex mutex;
  boost::condition_variable cond;

  int numRelatedJobs = 0;
  std::set<std::string>::iterator it;
  Finder finder(dir->filesystem());
  std::vector<FinderData *> jobs;

  for (it = entries.begin(); it != entries.end(); it++)
  {
    const std::string &entry = *it;

    if (entry[entry.length() - 1] != PATH_SEP)
      continue;

    FinderData *data = new FinderData;

    mutex.lock();

    data->dir = entry;
    data->args = &args;
    data->retCode = &ret;
    numRelatedJobs++;
    data->numberRelatedJobs = &numRelatedJobs;

    mutex.unlock();

    jobs.push_back(data);
    radosFsPriv()->getIoService()->post(boost::bind(&findInThread, &finder,
                                                    data, boost::ref(mutex),
                                                    boost::ref(cond)));
  }

  entries.clear();

  if (jobs.size() == 0)
    return 0;

  boost::unique_lock<boost::mutex> lock(mutex);

  while (numRelatedJobs > 0)
  {
    cond.wait(lock);
  }

  std::vector<FinderData *>::iterator vit;
  for (vit = jobs.begin(); vit != jobs.end(); vit++)
  {
    FinderData *data = *vit;
    entries.insert(data->dirEntries.begin(), data->dirEntries.end());
    results.insert(data->results.begin(), data->results.end());

    delete data;
  }

  return ret;
}

Stat *
DirPriv::fsStat(void) const
{
  return reinterpret_cast<Stat *>(dir->fsStat());
}

FilesystemPriv *
DirPriv::radosFsPriv(void)
{
  return dir->filesystem()->mPriv;
}

int
DirPriv::moveDirTreeObjects(const Stat *oldDir, const Stat *newDir)
{
  if (dirInfo || updateDirInfoPtr())
  {
    dirInfo->update();

    std::set<std::string> entries = dirInfo->contents();

    std::set<std::string>::const_iterator it;
    for (it = entries.begin(); it != entries.end(); it++)
    {
      const std::string &entry = *it;

      if (entry != "" && entry[entry.length() - 1] == PATH_SEP)
      {
        Stat oldSubDir, newSubDir;
        Dir subDir(dir->filesystem(), dir->path() + entry, false);

        oldSubDir = *subDir.mPriv->fsStat();
        newSubDir = oldSubDir;
        newSubDir.path = newDir->path + entry;

        int ret = subDir.mPriv->moveDirTreeObjects(&oldSubDir, &newSubDir);

        if (ret != 0)
          return ret;
      }
    }
  }

  int ret = createDirObject(newDir);

  if (ret == 0)
  {
    radosFsPriv()->updateDirInode(oldDir->path, newDir->path);
    ret = oldDir->pool->ioctx.remove(oldDir->path);
  }

  return ret;
}

int
DirPriv::rename(const std::string &destination)
{
  int index;
  Stat stat, oldStat, parentStat;
  std::string destParent = getParentDir(destination, &index);
  std::string realParentPath;
  std::string baseName;

  if (destParent != "")
  {
    baseName = destination.substr(index);
  }

  uid_t uid;
  gid_t gid;

  int ret = radosFsPriv()->getRealPath(destParent, &parentStat, realParentPath);

  if (ret == 0 && S_ISLNK(parentStat.statBuff.st_mode))
  {
    destParent = parentStat.translatedPath;
    ret = radosFsPriv()->stat(destParent, &parentStat);
  }
  else
  {
    destParent = realParentPath;
  }

  if (ret != 0)
  {
    radosfs_debug("Problem statting destination's parent when moving %s: %s",
                  destination.c_str(), strerror(-ret));
    return ret;
  }

  dir->filesystem()->getIds(&uid, &gid);

  if (!statBuffHasPermission(parentStat.statBuff, uid, gid, O_WRONLY))
  {
    radosfs_debug("No permissions to write in parent dir when moving %s",
                  destination.c_str());
    return -EACCES;
  }

  const std::string &newPath = destParent + baseName;

  ret = radosFsPriv()->stat(newPath, &stat);

  if (ret == 0 || newPath == dir->path())
  {
    radosfs_debug("Error moving directory: the new name already exists %s",
                  destination.c_str());

    return -EPERM;
  }
  else if (ret != -ENOENT)
  {
    return ret;
  }

  if (dir->path().length() <= newPath.length() &&
      dir->path() == newPath.substr(0, dir->path().length()))
  {
    radosfs_debug("Error moving directory. The new name contains the old name "
                  "as a parent: %s -> %s", dir->path().c_str(),
                  destination.c_str());

    return -EPERM;
  }

  oldStat = *fsStat();
  stat = oldStat;

  stat.path = newPath;
  ret = indexObject(&parentStat, &stat, '+');

  if (ret != 0)
    return ret;

  Stat *oldParentStat =
      reinterpret_cast<Stat *>(dir->parentFsStat());

  ret = indexObject(oldParentStat, &oldStat, '-');

  if (ret != 0)
    return ret;

  ret = moveDirTreeObjects(&oldStat, &stat);

  if (ret == 0)
  {
    dir->setPath(newPath);
  }

  radosFsPriv()->updateTMId(&parentStat);
  radosFsPriv()->updateTMId(oldParentStat);

  return ret;
}

/**
 * @class Dir
 *
 * Represents a directory in the filesystem.
 *
 * This class is used to manage the most common directory operations such as
 * creation, removal, listing, etc.
 */

/**
 * Creates an new instance of Dir.
 *
 * @param radosFs a pointer to the Filesystem that contains this directory.
 * @param path an absolute directory path.
 */
Dir::Dir(Filesystem *radosFs, const std::string &path)
  : FsObj(radosFs, getDirPath(path.c_str())),
    mPriv(new DirPriv(this))
{}

/**
 * Copy constructor for creating a directory instance.
 *
 * @param otherDir a reference to a Dir instance.
 */
Dir::Dir(const Dir &otherDir)
  : FsObj(otherDir),
    mPriv(new DirPriv(this))
{}

/**
 * Creates an new instance of Dir with an extra argument to indicate whether the
 * directory should be chached or not.
 *
 * @see The \ref dircache section of the \ref arch page for more information on
 *      directory caching.
 *
 * @param radosFs a pointer to the Filesystem that contains this directory.
 * @param path an absolute directory path.
 * @param cacheable true if the directory should be cacheable, false otherwise.
 */
Dir::Dir(Filesystem *radosFs,
         const std::string &path,
         bool cacheable)
  : FsObj(radosFs, getDirPath(path.c_str())),
    mPriv(new DirPriv(this, cacheable))
{}

Dir::~Dir()
{
  delete mPriv;
}

/**
 * Copy assignment operator.
 *
 * @param otherDir a reference to a Dir instance.
 * @return a reference to a Dir.
 */
Dir &
Dir::operator=(const Dir &otherDir)
{
  if (this != &otherDir)
  {
    setPath(otherDir.path());
  }

  return *this;
}

/**
 * Gets the parent directory of the given \a path.
 *
 * @param path a path to a file or directory (it does not need to be an existing
 *        path).
 * @param[out] pos an int location to store the offset of the parent's path in
 *       the given \a path (where the base name of this file or directory
 *       exists).
 * @return The parent directory's path (or "" if \a path is the root directory).
 */
std::string
Dir::getParent(const std::string &path, int *pos)
{
  return getParentDir(path, pos);
}

/**
 * Gets the list of files and directories in the directory.
 *
 * @note This method returns the entries cached in this instance since the last
 * call to Dir::update. To get an updated list of entries, Dir::update should be
 * called before this method.
 *
 * @param[out] entries a set to store the directory's entries.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::entryList(std::set<std::string> &entries)
{
  if (isFile())
  {
    radosfs_debug("Error: Dir instance has a path file %s ; not listing.",
                  path().c_str());
    return -ENOTDIR;
  }

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

/**
 * Creates the directory object in the system.
 *
 * @param mode the directory mode bits (from sys/stat.h), S_IRWXU, etc. Use -1
 *        for the default (S_IRWXU | S_IRGRP | S_IROTH).
 * @param mkpath creates all intermediate directories in this directory's path.
 * @param owner the user id for the owner of the directory. By default (-1), the
 *        current user's id (see Filesystem::setUid) is used as the owner.
 * @param group the group id for the owner group of the directory. By default
 *        (-1), the current group's id (see Filesystem::setGid) is used as the
 *        owner group.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::create(int mode,
            bool mkpath,
            int owner,
            int group)
{
  int ret;
  const std::string &dir = path();
  Filesystem *radosFs = filesystem();

  if (exists())
  {
    if (isFile())
      return -ENOTDIR;

    if (mkpath)
      return 0;

    return -EEXIST;
  }

  const PoolSP pool = mPriv->getPool();

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

  Stat stat, parentStat;

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
  stat.translatedPath = generateUuid();
  stat.statBuff.st_mode = permOctal;
  stat.statBuff.st_uid = owner;
  stat.statBuff.st_gid = group;
  stat.pool = pool;

  timespec spec;
  clock_gettime(CLOCK_REALTIME, &spec);

  stat.statBuff.st_ctim = spec;
  stat.statBuff.st_ctime = spec.tv_sec;

  ret = createDirAndInode(&stat);

  if (ret != 0)
  {
    radosfs_debug("Problem setting inode in dir %s: %s", stat.path.c_str(),
                  strerror(abs(ret)));
    return ret;
  }

  indexObject(&parentStat, &stat, '+');

  FsObj::refresh();
  mPriv->updateDirInfoPtr();

  mPriv->radosFsPriv()->updateTMId(&stat);

  return 0;
}

/**
 * Removes the directory.
 *
 * @note The directory has to be empty in order to be removed.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::remove()
{
  int ret;
  const std::string &dirPath = path();
  Filesystem *radosFs = filesystem();
  Stat stat, *statPtr;

  FsObj::refresh();

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

  statPtr = reinterpret_cast<Stat *>(fsStat());

  DirCache *info = 0;

  if (!isLink())
  {
    info = mPriv->dirInfo.get();

    info->update();

    if (info->getEntry(0) != "")
      return -ENOTEMPTY;

    ret = statPtr->pool->ioctx.remove(dirPath);

    if (ret == 0)
    {
     ret = info->ioctx().remove(statPtr->translatedPath);

     mPriv->radosFsPriv()->removeDirInode(path());
    }
  }

  if (ret == 0)
    indexObject(&stat, statPtr, '-');

  FsObj::refresh();

  if (info)
    mPriv->updateFsDirCache();

  mPriv->updateDirInfoPtr();

  mPriv->radosFsPriv()->updateTMId(statPtr);

  return ret;
}

/**
 * Updates the state and the entries of this Dir instance according to the data
 * of the actual directory object in the system.
 *
 * Traditionally this could be considered as reopening the directory. It is used
 * to get the latest status of the directory in the system, including the list
 * of entries. E.g. if a directory instance has been used for a long time and
 * the status of the permissions or the existance needs to be checked, or the
 * latest entry list needs to be checked, then update should be called.
 *
 * @note This method should be always called when getting the list of entries in
 *       the directory. Check out the Dir::entryList method.
 *
 * @see The \ref updateobjs and \ref listdir sections of the \ref arch page.
 */
void
Dir::refresh()
{
  FsObj::refresh();

  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->refresh();

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

/**
 * Gets the entry with the index \a entryIndex from the directory.
 *
 * @see Dir::update
 *
 * @param entryIndex the index of the entry to fetch.
 * @param[out] entry a string reference where the entry name will be stored.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::entry(int entryIndex, std::string &entry)
{
  if (isFile())
  {
    radosfs_debug("Error: Dir instance has a path file %s ; not reading the "
                  "entry.", path().c_str());
    return -ENOTDIR;
  }

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

/**
 * Changes the directory object that this Dir instance refers to. This works as
 * if instantiating the directory again using a different path.
 *
 * @param path a path to a directory.
 */
void
Dir::setPath(const std::string &path)
{
  const std::string &dirPath = getDirPath(path.c_str());

  if (dirPath == this->path())
    return;

  mPriv->dirInfo.reset();

  FsObj::setPath(dirPath);

  mPriv->updatePath();
}

/**
 * Checks whether the directory is writable. This takes into account the
 * directory's permissions.
 *
 * @note This operation works over the information that this instance has of the
 *       permissions, it does not get the latest values from the object in the
 *       cluster. For updating those values, Dir::update has to be called.
 *
 * @return true is the directory is writable, false otherwise.
 */
bool
Dir::isWritable()
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->isWritable();

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  Filesystem *radosFs = filesystem();
  uid_t uid = radosFs->uid();
  gid_t gid = radosFs->gid();

  if (!mPriv->dirInfo)
    return false;

  return statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid, O_WRONLY);
}

/**
 * Checks whether the directory is readable. This takes into account the
 * directory's permissions.
 *
 * @note This operation works over the information that this instance has of the
 *       permissions, it does not get the latest values from the object in the
 *       cluster. For updating those values, Dir::update has to be called.
 *
 * @return true is the directory is readable, false otherwise.
 */
bool
Dir::isReadable()
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->isReadable();

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  Filesystem *radosFs = filesystem();
  uid_t uid = radosFs->uid();
  gid_t gid = radosFs->gid();

  if (!mPriv->dirInfo)
    return false;

  return statBuffHasPermission(mPriv->fsStat()->statBuff, uid, gid, O_RDONLY);
}

/**
 * Stats the directory.
 *
 * @param[out] buff a stat struct to fill with the details of the directory.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::stat(struct stat *buff)
{
  return FsObj::stat(buff);
}

/**
 * Compacts the directory object's log.
 *
 * @see The \ref dircompaction section of the \ref arch page for more
 *      info on this operation.
 *
 * @return 0 on success, an error code otherwise.
 */
int
Dir::compact()
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

/**
 * Sets metadata for the directory's entry.
 *
 * @param entry the entry's name in the directory.
 * @param key the metadata's key.
 * @param value the value for the metadata.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::setMetadata(const std::string &entry, const std::string &key,
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

  refresh();

  if (!isWritable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    if (mPriv->dirInfo->hasEntry(entry))
    {
      librados::IoCtx ioctx = mPriv->dirInfo->ioctx();
      std::map<std::string, std::string> metadata;
      metadata[key] = value;

      int ret = indexObjectMetadata(ioctx, mPriv->dirInfo->inode(), entry,
                                    metadata, '+');

      mPriv->radosFsPriv()->updateTMId(mPriv->fsStat());

      return ret;
    }

    return -ENOENT;
  }

  return -1;
}

/**
 * Gets metadata for the directory's entry.
 *
 * @param entry the entry's name in the directory.
 * @param key the metadata's key.
 * @param[out] value a string reference to return the metadata's value.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::getMetadata(const std::string &entry, const std::string &key,
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

  refresh();

  if (!isReadable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    return mPriv->dirInfo->getMetadata(entry, key, value);
  }

  return -1;
}

/**
 * Gets a map with the metadata for the given \a entry.
 *
 * @param entry the entry's name in the directory.
 * @param[out] mtdMap a map reference in which to set the metadata keys and
 *             values.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::getMetadataMap(const std::string &entry,
                    std::map<std::string, std::string> &mtdMap)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->getMetadataMap(entry, mtdMap);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  refresh();

  if (!isReadable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    return mPriv->dirInfo->getMetadataMap(entry, mtdMap);
  }

  return -EOPNOTSUPP;
}

/**
 * Removes the entry's metadata indicated by the given \a key.
 *
 * @param entry the entry's name in the directory.
 * @param key the key of the metadata that should be removed.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::removeMetadata(const std::string &entry, const std::string &key)
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

  refresh();

  if (!isWritable())
    return -EACCES;

  if (mPriv->dirInfo)
  {
    std::string value;

    if (mPriv->dirInfo->hasEntry(entry) &&
        mPriv->dirInfo->getMetadata(entry, key, value) == 0)
    {
      librados::IoCtx ioctx = mPriv->dirInfo->ioctx();
      std::map<std::string, std::string> metadata;
      metadata[key] = "";

      int ret = indexObjectMetadata(ioctx,
                                    mPriv->dirInfo->inode(), entry, metadata,
                                    '-');


      mPriv->radosFsPriv()->updateTMId(mPriv->fsStat());

      return ret;
    }

    return -ENOENT;
  }

  return -1;
}

static Finder::FindOptions
setupMtdFindArg(FinderArg &arg, Finder::FindOptions mtdType,
                const std::string &keyword, bool numeric, const std::string &op,
                const std::string &key, const std::string &value)
{
  arg.valueStr = value;
  arg.valueNum = atof(value.c_str());

  // Strip the keyword (e.g. "mtd") from the key.
  if (key.length() > keyword.length())
  {
    // We strip one extra char because of the dot, e.g. mtd.example
    arg.key = key.substr(keyword.length() + 1);
  }
  else
  {
    arg.key = key.substr(key.length());
  }

  if (numeric)
    arg.options |= FinderArg::FINDER_OPT_CMP_NUM;

  int option = mtdType;

  if (op == FINDER_EQ_SYM)
  {
    option |= Finder::FIND_EQ;
  }
  else if (op == FINDER_NE_SYM)
  {
    option |= Finder::FIND_NE;
  }
  else if (op == FINDER_GT_SYM)
  {
    option |= Finder::FIND_GT;
    arg.options |= FinderArg::FINDER_OPT_CMP_NUM;
  }
  else if (op == FINDER_GE_SYM)
  {
    option |= Finder::FIND_GE;
    arg.options |= FinderArg::FINDER_OPT_CMP_NUM;
  }
  else if (op == FINDER_LT_SYM)
  {
    option |= Finder::FIND_LT;
    arg.options |= FinderArg::FINDER_OPT_CMP_NUM;
  }
  else if (op == FINDER_LE_SYM)
  {
    option |= Finder::FIND_LE;
    arg.options |= FinderArg::FINDER_OPT_CMP_NUM;
  }

  return static_cast<Finder::FindOptions>(option);
}

/**
 * Finds the entries in the directory and subdirectories recursively and in
 * parallel.
 *
 * Check out the \ref usefindindir section for learning how to use this method.
 *
 * @param[out] results a set reference to return the paths found.
 * @param args the arguments for the find operation.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::find(const std::string args, std::set<std::string> &results)
{
  if (isLink())
  {
    if (mPriv->target)
      return mPriv->target->find(args, results);

    radosfs_debug("No target for link %s", path().c_str());
    return -ENOLINK;
  }

  int ret = 0;
  std::set<std::string> dirs, files, entries;
  std::map<Finder::FindOptions, FinderArg> finderArgs;

  entries.insert(path());

  int startPos = 0, lastPos = 0;
  std::string key, value, op;

  while ((lastPos = splitToken(args, startPos, key, value, &op)) != startPos)
  {
    if (key == "")
      break;

    FinderArg arg;
    arg.valueStr = "";
    int option  = Finder::FIND_NAME_EQ;

    bool isIName = key == FINDER_KEY_INAME;
    if (key == FINDER_KEY_NAME || isIName)
    {
      arg.valueStr = value;

      option = Finder::FIND_NAME_EQ;

      if (op == FINDER_NE_SYM)
        option = Finder::FIND_NAME_NE;

      if (isIName)
        arg.options |= FinderArg::FINDER_OPT_ICASE;
    }
    else if (key == FINDER_KEY_UID || key == FINDER_KEY_GID ||
             key == FINDER_KEY_SIZE)
    {
      arg.valueNum = atof(value.c_str());

      if (key == FINDER_KEY_GID)
        option = Finder::FIND_GID;
      else if (key == FINDER_KEY_SIZE)
        option = Finder::FIND_SIZE;
      else
        option = Finder::FIND_UID;

      if (op == FINDER_EQ_SYM)
        option |= Finder::FIND_EQ;
      else if (op == FINDER_NE_SYM)
        option |= Finder::FIND_NE;
      else if (op == FINDER_GE_SYM)
        option |= Finder::FIND_GE;
      else if (op == FINDER_GT_SYM)
        option |= Finder::FIND_GT;
      else if (op == FINDER_LE_SYM)
        option |= Finder::FIND_LE;
      else if (op == FINDER_LT_SYM)
        option |= Finder::FIND_LT;
    }
    else if (key.compare(0, strlen(FINDER_KEY_MTD), FINDER_KEY_MTD) == 0)
    {
      size_t keyLength = strlen(FINDER_KEY_MTD);
      size_t numKeyLength = strlen(FINDER_KEY_MTD_NUM);

      // The numeric version of the keyword only appends a suffix so we
      // just need to compare the strings from the length of the original
      // keyword
      bool isNumeric = key.compare(keyLength, numKeyLength - keyLength,
                                   FINDER_KEY_MTD_NUM + keyLength) == 0;
      option = setupMtdFindArg(arg, Finder::FIND_MTD,
                               isNumeric ? FINDER_KEY_MTD_NUM : FINDER_KEY_MTD,
                               isNumeric, op, key, value);
    }
    else if (key.compare(0, strlen(FINDER_KEY_XATTR), FINDER_KEY_XATTR) == 0)
    {
      size_t keyLength = strlen(FINDER_KEY_XATTR);
      size_t numKeyLength = strlen(FINDER_KEY_XATTR_NUM);

      // The numeric version of the keyword only appends a suffix so we
      // just need to compare the strings from the length of the original
      // keyword
      bool isNumeric = key.compare(keyLength, numKeyLength - keyLength,
                                   FINDER_KEY_XATTR_NUM + keyLength) == 0;
      option = setupMtdFindArg(arg, Finder::FIND_XATTR,
                               isNumeric ? FINDER_KEY_XATTR_NUM : FINDER_KEY_XATTR,
                               isNumeric, op, key, value);
    }
    else if (key.compare(0, strlen(FINDER_KEY_IMTD), FINDER_KEY_IMTD) == 0)
    {
      option = setupMtdFindArg(arg, Finder::FIND_MTD, FINDER_KEY_IMTD, false, op,
                               key, value);
      arg.options = FinderArg::FINDER_OPT_ICASE;
    }
    else if (key.compare(0, strlen(FINDER_KEY_IXATTR), FINDER_KEY_IXATTR) == 0)
    {
      option = setupMtdFindArg(arg, Finder::FIND_XATTR, FINDER_KEY_IXATTR, false,
                               op, key, value);
      arg.options = FinderArg::FINDER_OPT_ICASE;
    }
    else
    {
      radosfs_debug("Invalid keyword found '%s'. Stopping the find operation.",
                    key.c_str());
      finderArgs.clear();
      break;
    }

    finderArgs[static_cast<Finder::FindOptions>(option)] = arg;

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

/**
 * Changes the permissions of the directory.
 *
 * @param permissions the new permissions (mode bits from sys/stat.h).
 * @return 0 on success, an error code otherwise.
 */
int
Dir::chmod(long int permissions)
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

  mode = permissions | S_IFDIR;

  Stat stat = *reinterpret_cast<Stat *>(fsStat());
  Stat *parentStat = reinterpret_cast<Stat *>(parentFsStat());

  if (isLink())
  {
    stat.statBuff.st_mode = mode;
    const std::string &baseName = path().substr(mPriv->parentDir.length());
    std::map<std::string, librados::bufferlist> omap;
    omap[XATTR_FILE_PREFIX + baseName].append(getFileXAttrDirRecord(&stat));

    ret = stat.pool->ioctx.omap_set(parentStat->translatedPath, omap);
  }
  else
  {
    uid_t uid = filesystem()->uid();

    if (uid != ROOT_UID && stat.statBuff.st_uid != uid)
      return -EPERM;

    librados::bufferlist permissionsXAttr;
    permissionsXAttr.append(makePermissionsXAttr(mode, stat.statBuff.st_uid,
                                                 stat.statBuff.st_gid));

    std::map<std::string, librados::bufferlist> omap;
    omap[XATTR_PERMISSIONS] = permissionsXAttr;

    ret = stat.pool->ioctx.omap_set(stat.translatedPath, omap);
  }

  return ret;
}

/**
 * Sets the owner uid and gid of the directory.
 *
 * @note This function can only be used by *root* (the uid of Filesystem needs
 *       to be the root's, see Filesystem::setIds).
 *
 * @param uid a user id.
 * @param gid a group id.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::chown(uid_t uid, gid_t gid)
{
  if (!exists())
    return -ENOENT;

  uid_t currentUid = filesystem()->uid();

  if (currentUid != ROOT_UID)
    return -EPERM;

  Stat fsStat = *mPriv->fsStat();
  std::string permissions = makePermissionsXAttr(fsStat.statBuff.st_mode, uid,
                                                 gid);

  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_PERMISSIONS].append(permissions);

  return fsStat.pool->ioctx.omap_set(fsStat.translatedPath, omap);
}

/**
 * Sets the owner uid of the directory.
 *
 * @note This function can only be used by *root* (the uid of Filesystem needs
 *       to be the root's, see Filesystem::setIds).
 *
 * @param uid a user id.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::setUid(uid_t uid)
{
  if (!exists())
    return -ENOENT;

  Stat fsStat = *mPriv->fsStat();

  return chown(uid, fsStat.statBuff.st_gid);
}

/**
 * Sets the owner gid of the directory.
 *
 * @note This function can only be used by *root* (the uid of Filesystem needs
 *       to be the root's, see Filesystem::setIds).
 *
 * @param gid a group id.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::setGid(gid_t gid)
{
  if (!exists())
    return -ENOENT;

  Stat fsStat = *mPriv->fsStat();

  return chown(fsStat.statBuff.st_uid, gid);
}

/**
 * Renames or moves the directory.
 *
 * @param newName the new path or name of the directory.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::rename(const std::string &newName)
{
  if (!exists())
    return -ENOENT;

  if (newName == "")
    return -EINVAL;

  std::string dest = newName;

  if (dest[0] != PATH_SEP)
  {
    dest = getParentDir(path(), 0) + dest;
  }

  if (dest == "/")
    return -EISDIR;

  dest = getDirPath(sanitizePath(dest));

  return mPriv->rename(dest);
}

/**
 * Sets whether the *Transversal Modification Id* (TMId) should be used in
 * this directory.
 *
 * @see Check out the section \ref usetmid for learning what the \b TMId
 *      means and how it works.
 *
 * @param useTMId true if \b tmid should be used, false otherwise.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::useTMId(bool useTMId)
{
  if (!exists())
    return -ENOENT;

  Stat stat = *reinterpret_cast<Stat *>(fsStat());

  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_USE_TMID].append(useTMId ? 1 : 0);

  return stat.pool->ioctx.omap_set(stat.translatedPath, omap);
}

/**
 * Sets whether the *Transversal Modification Id* (TMId) should be used in
 * this directory.
 *
 * @see Check out the section \ref usetmid for learning what the \b TMId
 *      means and how it works.
 *
 * @return true if the \b tmid is set in this directory, false otherwise.
 */
bool
Dir::usingTMId()
{
  if (!exists())
    return -ENOENT;

  Stat stat = *reinterpret_cast<Stat *>(fsStat());

  std::set<std::string> keys;
  keys.insert(XATTR_USE_TMID);

  std::map<std::string, librados::bufferlist> omap;

  int ret = stat.pool->ioctx.omap_get_vals_by_keys(stat.translatedPath, keys,
                                                   &omap);

  if (ret != 0)
    return false;

  if (omap.count(XATTR_USE_TMID) > 0)
  {
    librados::bufferlist value;
    value.append(1);
    return omap[XATTR_USE_TMID].contents_equal(value);
  }

  return false;
}

/**
 * Gets the *Transversal Modification Id* (TMId) of the directory.
 * @param id a reference to return the TMId.
 * @return 0 on success, an error code otherwise.
 */
int
Dir::getTMId(std::string &id)
{
  if (!exists())
    return -ENOENT;

  id.clear();

  Stat stat = *reinterpret_cast<Stat *>(fsStat());

  std::set<std::string> keys;
  keys.insert(XATTR_USE_TMID);
  keys.insert(XATTR_TMID);

  std::map<std::string, librados::bufferlist> omap;

  librados::ObjectReadOperation op;

  librados::bufferlist expectedValue;
  expectedValue.append(1);

  std::pair<librados::bufferlist, int> cmp(expectedValue,
                                           LIBRADOS_CMPXATTR_OP_EQ);

  std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
  omapCmp[XATTR_USE_TMID] = cmp;

  op.omap_cmp(omapCmp, 0);
  op.omap_get_vals_by_keys(keys, &omap, 0);

  int ret = stat.pool->ioctx.operate(stat.translatedPath, &op, 0);

  if (ret == -ECANCELED)
  {
    // TM id not in use
    ret = -ENODATA;
  }
  else if (ret == 0)
  {
    if (omap.count(XATTR_TMID) > 0)
    {
      librados::bufferlist tmId = omap[XATTR_TMID];
      id.assign(tmId.c_str(), tmId.length());
    }
  }
  else if (ret != -ENODATA)
  {
    radosfs_debug("Could not get TM id from '%s' (%s): %s (retcode=%d)",
                  stat.path.c_str(), stat.translatedPath.c_str(),
                  strerror(abs(ret)), ret);
  }

  return ret;
}

RADOS_FS_END_NAMESPACE
