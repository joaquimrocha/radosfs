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

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/progress.hpp>
#include <rados/librados.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include "RadosFs.hh"
#include "RadosFsFile.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

__thread uid_t RadosFsPriv::uid;
__thread gid_t RadosFsPriv::gid;

RadosFsPriv::RadosFsPriv(RadosFs *radosFs)
  : radosFs(radosFs),
    radosCluster(0),
    dirCompactRatio(DEFAULT_DIR_COMPACT_RATIO),
    fileStripeSize(FILE_STRIPE_SIZE),
    lockFiles(true),
    ioService(new boost::asio::io_service),
    asyncWork(new boost::asio::io_service::work(*ioService)),
    fileOpsIdleChecker(boost::bind(&RadosFsPriv::checkFileLocks, this))
{
  uid = 0;
  gid = 0;
  dirCache.maxCacheSize = DEFAULT_DIR_CACHE_MAX_SIZE;

  pthread_mutex_init(&poolMutex, 0);
  pthread_mutex_init(&mtdPoolMutex, 0);
  pthread_mutex_init(&dirCacheMutex, 0);
  pthread_mutex_init(&dirPathInodeMutex, 0);
}

RadosFsPriv::~RadosFsPriv()
{
  fileOpsIdleChecker.interrupt();
  operationsMutex.lock();
  operations.clear();
  operationsMutex.unlock();

  poolMap.clear();
  mtdPoolMap.clear();
  dirPathInodeMap.clear();

  if (radosCluster)
    rados_shutdown(radosCluster);

  pthread_mutex_lock(&dirCacheMutex);

  dirCache.cleanCache();

  pthread_mutex_unlock(&dirCacheMutex);

  pthread_mutex_destroy(&poolMutex);
  pthread_mutex_destroy(&dirCacheMutex);
}

PriorityCache::PriorityCache()
  : head(0),
    tail(0),
    cacheSize(0),
    maxCacheSize(DEFAULT_DIR_CACHE_MAX_SIZE)
{}

PriorityCache::~PriorityCache()
{}

void
PriorityCache::cleanCache()
{
  std::map<std::string, LinkedList *>::iterator it;
  for (it = cacheMap.begin(); it != cacheMap.end(); it++)
  {
    delete (*it).second;
  }

  cacheMap.clear();
}

void
PriorityCache::removeCache(const std::string &path, bool freeMemory)
{
  if (cacheMap.count(path) == 0)
    return;

  LinkedList *link = cacheMap[path];
  removeCache(link, freeMemory);
}

void
PriorityCache::removeCache(LinkedList *link, bool freeMemory)
{
  if (head == link)
  {
    head = head->previous;

    if (head)
      head->next = 0;
  }

  if (tail == link)
  {
    tail = tail->next;

    if (tail)
      tail->previous = 0;
  }

  if (link->next != 0)
    link->next->previous = link->previous;

  if (link->previous != 0)
    link->previous->next = link->next;

  if (freeMemory)
  {
    cacheSize -= link->lastNumEntries;
    cacheMap.erase(link->cachePtr->inode());
    delete link;
  }
}

void
PriorityCache::moveToFront(LinkedList *link)
{
  if (head == link || link == 0)
    return;

  if (tail == link)
  {
    tail = tail->next;

    if (tail)
      tail->previous = 0;
  }

  if (tail == 0)
    tail = link;

  if (link->next != 0)
    link->next->previous = link->previous;

  if (link->previous != 0)
    link->previous->next = link->next;

  link->previous = head;
  head = link;

  if (link->previous)
    link->previous->next = link;

  link->next = 0;
}

void
PriorityCache::update(std::tr1::shared_ptr<DirCache> cache)
{
  LinkedList *list = 0;
  const size_t dirNumEntries = cache->contents().size();

  // we add 1 to the number of entries because the size of the
  // cache is the number of directories plus their entries
  if (dirNumEntries + 1 > maxCacheSize)
    return;

  if (cacheMap.count(cache->inode()) == 0)
  {
    list = new LinkedList;
    list->cachePtr = cache;
    list->next = 0;
    list->previous = 0;
    list->lastNumEntries = 0;

    cacheMap[cache->inode()] = list;
  }
  else
  {
    list = cacheMap[cache->inode()];
  }

  if (dirNumEntries != list->lastNumEntries)
  {
    cacheSize += dirNumEntries - list->lastNumEntries;
    list->lastNumEntries = dirNumEntries;
  }

  moveToFront(list);

  adjustCache();
}

void
PriorityCache::adjustCache()
{
  if (size() <= maxCacheSize || tail == 0 || size() == 1)
    return;

  size_t minCacheSize = (float) maxCacheSize *
                        DEFAULT_DIR_CACHE_CLEAN_PERCENTAGE;

  if (minCacheSize == 0)
    minCacheSize = 1;

  while (size() > minCacheSize)
  {
    if (tail == 0)
      break;

    removeCache(tail);
  }
}

int
RadosFsPriv::createCluster(const std::string &userName,
                           const std::string &confFile)
{
  int ret;
  const char *user = 0;

  if (userName != "")
    user = userName.c_str();

  ret = rados_create(&radosCluster, user);

  if (ret != 0)
      return ret;

  if (confFile != "")
  {
    ret = rados_conf_read_file(radosCluster, confFile.c_str());

    if (ret != 0)
        return ret;
  }

  ret = rados_connect(radosCluster);

  return ret;
}

int
RadosFsPriv::statLink(RadosFsPoolSP mtdPool,
                      RadosFsStat *stat,
                      std::string &pool)
{
  int ret = 0;
  const std::string &parentDir = getParentDir(stat->path, 0);
  char fileXAttr[XATTR_FILE_LENGTH + 1];

  const std::string &pathXAttr = XATTR_FILE_PREFIX +
                                 stat->path.substr(parentDir.length());

  stat->statBuff.st_size = 0;

  if (mtdPool == 0)
    return -ENODEV;

  RadosFsInode inode;
  ret = getDirInode(parentDir, inode, mtdPool);

  if (ret != 0)
    return ret;

  int length = rados_getxattr(inode.pool->ioctx,
                              inode.inode.c_str(),
                              pathXAttr.c_str(), fileXAttr,
                              XATTR_FILE_LENGTH);

  if (length > 0)
  {
    fileXAttr[length] = '\0';
    ret = statFromXAttr(stat->path, fileXAttr, &stat->statBuff,
                        stat->translatedPath, pool, stat->extraData);

    if (stat->translatedPath == "")
    {
      ret = -ENOLINK;
    }
  }
  else if (length == -ENODATA)
  {
    ret = -ENOENT;
  }
  else
  {
    ret = length;
  }

  return ret;
}

int
RadosFsPriv::getRealPath(const std::string &path,  RadosFsStat *stat,
                         std::string &realPath)
{
  int ret = -ENOENT;
  realPath = path;

  stat->reset();

  while(true)
  {
    ret = this->stat(realPath, stat);

    if (ret == 0)
    {
      realPath = stat->path;
      break;
    }

    std::string parent = getParentDir(realPath, 0);

    while (parent != "")
    {
      int ret = this->stat(parent, stat);

      if (ret == -ENOENT)
      {
        parent = getParentDir(parent, 0);
      }
      else if (ret == 0)
      {
        break;
      }
      else
      {
        stat->reset();
        return ret;
      }
    }

    if (parent == "")
      return -ENODEV;

    if (S_ISLNK(stat->statBuff.st_mode))
    {
      realPath = stat->translatedPath + realPath.substr(parent.length());
      continue;
    }

    break;
  }

  return ret;
}

int
RadosFsPriv::getDirInode(const std::string &path,
                         RadosFsInode &inode,
                         RadosFsPoolSP &mtdPool)
{
  pthread_mutex_lock(&dirPathInodeMutex);

  if (dirPathInodeMap.count(path) > 0)
    inode = dirPathInodeMap[path];

  pthread_mutex_unlock(&dirPathInodeMutex);

  if (inode.inode == "")
  {
    std::string inodeName, poolName;

    int ret = getInodeAndPool(mtdPool->ioctx, path, inodeName, poolName);

    if (ret != 0)
      return ret;

    RadosFsPoolSP pool = getMtdPoolFromName(poolName);

    if (!pool)
      return -ENODEV;

    inode.inode = inodeName;
    inode.pool = pool;
    setDirInode(path, inode);
  }

  return 0;
}

void
RadosFsPriv::setDirInode(const std::string &path, const RadosFsInode &inode)
{
  pthread_mutex_lock(&dirPathInodeMutex);

  dirPathInodeMap[path] = inode;

  pthread_mutex_unlock(&dirPathInodeMutex);
}

void
RadosFsPriv::updateDirInode(const std::string &oldPath,
                            const std::string &newPath)
{
  pthread_mutex_lock(&dirPathInodeMutex);

  if (dirPathInodeMap.count(oldPath) > 0)
  {
    dirPathInodeMap[newPath] = dirPathInodeMap[oldPath];
    dirPathInodeMap.erase(oldPath);
  }

  pthread_mutex_unlock(&dirPathInodeMutex);
}

void
RadosFsPriv::removeDirInode(const std::string &path)
{
  pthread_mutex_lock(&dirPathInodeMutex);

  dirPathInodeMap.erase(path);

  pthread_mutex_unlock(&dirPathInodeMutex);
}

void
RadosFsPriv::launchThreads(void)
{
  size_t currentLaunchedThreads = generalWorkerThreads.size();
  size_t threadsToLaunch = DEFAULT_NUM_WORKER_THREADS - currentLaunchedThreads;
  while (threadsToLaunch-- > 0)
  {
    generalWorkerThreads.create_thread(
          boost::bind(&RadosFsPriv::generalWorkerThread, this, ioService));
  }
}

boost::shared_ptr<boost::asio::io_service>
RadosFsPriv::getIoService()
{
  launchThreads();
  return ioService;
}

void
RadosFsPriv::statXAttrInThread(std::string path, std::string xattr,
                               RadosFsStat *stat, int *ret, boost::mutex *mutex,
                               boost::condition_variable *cond, int *numJobs)
{
  RadosFsPoolSP dataPool;
  std::string pool;
  *ret = statFromXAttr(path, xattr, &stat->statBuff, stat->translatedPath,
                          pool, stat->extraData);

  if (*ret != 0)
  {
    return;
  }

  dataPool = getDataPool(path, pool);
  stat->pool = dataPool;
  RadosFsIOSP radosFsIO = getOrCreateFsIO(stat->translatedPath, stat);
  stat->statBuff.st_size = radosFsIO->getSize();

  bool notify = false;

  {
    boost::unique_lock<boost::mutex> lock(*mutex);

    notify = --*numJobs == 0;
  }

  if (notify)
    cond->notify_all();
}

void
RadosFsPriv::generalWorkerThread(
    boost::shared_ptr<boost::asio::io_service> ioService)
{
  ioService->run();
}

void
RadosFsPriv::statEntries(StatAsyncInfo *info,
                         std::map<std::string, std::string> &xattrs)
{
  RadosFsStat *stats = new RadosFsStat[info->entries->size()];
  int *rets = new int[info->entries->size()];
  boost::mutex mutex;
  boost::condition_variable cond;
  int numJobs = info->entries->size();

  for (size_t i = 0; i < info->entries->size(); i++)
  {
    const std::string &path = info->stat.path + (*info->entries)[i];
    const std::string &xattr = xattrs[XATTR_FILE_PREFIX + (*info->entries)[i]];

    if (xattr == "")
    {
      rets[i] = -ENOENT;
      mutex.lock();
      numJobs--;
      mutex.unlock();
      continue;
    }

    ioService->post(boost::bind(&RadosFsPriv::statXAttrInThread, this, path,
                                xattr, &stats[i], &rets[i], &mutex, &cond,
                                &numJobs));
  }

  boost::unique_lock<boost::mutex> lock(mutex);

  if (numJobs > 0)
    cond.wait(lock);

  for (size_t i = 0; i < info->entries->size(); i++)
  {
    const std::string &path = info->stat.path + (*info->entries)[i];

    if (rets[i] == 0 || info->entryStats.count(path) == 0)
    {
      std::pair<int, struct stat> statValue(rets[i], stats[i].statBuff);
      info->entryStats[path] = statValue;
    }
  }

  delete[] rets;
  delete[] stats;
}

void
RadosFsPriv::statAsync(StatAsyncInfo *info)
{
  std::map<std::string, std::string> xattrs;
  u_int64_t size;
  time_t mtime;
  xattrs[XATTR_PERMISSIONS] = "";
  xattrs[XATTR_MTIME] = "";
  xattrs[XATTR_CTIME] = "";

  for (size_t i = 0; i < info->entries->size(); i++)
  {
    const std::string &entry = XATTR_FILE_PREFIX + (*info->entries)[i];
    xattrs[entry] = "";
  }

  info->statRet = statAndGetXAttrs(info->stat.pool->ioctx,
                                   info->stat.translatedPath, &size,
                                   &mtime, xattrs);

  genericStatFromAttrs(info->stat.path, xattrs[XATTR_PERMISSIONS],
                       xattrs[XATTR_CTIME], xattrs[XATTR_MTIME], size, mtime,
                       &info->stat.statBuff);

  if (info->entries->size() == 0 || info->statRet != 0)
    return;

  statEntries(info, xattrs);
}

int
RadosFsPriv::statAsyncInfoInThread(const std::string path, StatAsyncInfo *info,
                                   boost::mutex *mutex,
                                   boost::condition_variable *cond, int *numJobs)
{
  int ret;
  info->stat.reset();
  info->stat.path = getDirPath(path);
  RadosFsPoolSP mtdPool = getMetadataPoolFromPath(info->stat.path);

  if (!mtdPool.get())
  {
    ret = -ENODEV;
    return ret;
  }

  RadosFsInode inode;
  ret = getDirInode(info->stat.path, inode, mtdPool);

  if (ret == 0)
  {
    info->stat.pool = inode.pool;
    info->stat.translatedPath = inode.inode;

    statAsync(info);
  }
  else
  {
    info->statRet = ret;
  }

  mutex->lock();
  int remaininNumJobs = --*numJobs;
  mutex->unlock();

  if (remaininNumJobs == 0)
    cond->notify_all();

  return ret;
}

void
RadosFsPriv::parallelStat(
    const std::map<std::string, std::vector<std::string> > &paths,
    std::map<std::string, std::pair<int, struct stat> > *stats)
{
  StatAsyncInfo *statInfoList = new StatAsyncInfo[paths.size()];
  boost::mutex mutex;
  boost::condition_variable cond;
  int numJobs = paths.size();
  std::map<std::string, std::vector<std::string> >::const_iterator it;
  size_t i;

  for (it = paths.begin(), i = 0; it != paths.end(); it++, i++)
  {
    std::string dir = (*it).first;
    StatAsyncInfo *info = &statInfoList[i];
    info->entries = &(*it).second;

    getIoService()->post(boost::bind(&RadosFsPriv::statAsyncInfoInThread, this,
                                     dir, info, &mutex, &cond, &numJobs));
  }

  boost::unique_lock<boost::mutex> lock(mutex);

  if (numJobs > 0)
    cond.wait(lock);

  // Assign the result of statting all the paths to the stats output parameter
  for (size_t i = 0; i < paths.size(); i++)
  {
    StatAsyncInfo *statInfo = &statInfoList[i];
    std::map<std::string, std::pair<int, struct stat> >::const_iterator it;

    if (statInfo->statRet == 0)
    {
      std::pair<int, struct stat> statResult(statInfo->statRet,
                                             statInfo->stat.statBuff);
      (*stats)[statInfo->stat.path] = statResult;
    }

    for (it = statInfo->entryStats.begin(); it != statInfo->entryStats.end();
         it++)
    {
      const std::string &path = (*it).first;
      const std::pair<int, struct stat> &statResult = (*it).second;

      if (stats->count(path) > 0 && stats->at(path).first == 0)
        continue;

      (*stats)[path] = statResult;
    }
  }

  delete[] statInfoList;
}

int
RadosFsPriv::stat(const std::string &path,
                  RadosFsStat *stat)
{
  RadosFsPoolSP mtdPool;
  int ret = -ENODEV;
  stat->reset();
  stat->path = getDirPath(path);
  mtdPool = getMetadataPoolFromPath(stat->path);

  if (!mtdPool.get())
    return -ENODEV;

  if (isDirPath(path))
  {
    ret = statDir(mtdPool, stat);

    if (ret == 0 || ret == -ENODEV)
      return ret;
  }

  ret = statFile(mtdPool, stat);

  if (ret == 0 || ret == -ENODEV)
    return ret;

  ret = statDir(mtdPool, stat);

  return ret;
}

int
RadosFsPriv::statDir(RadosFsPoolSP mtdPool, RadosFsStat *stat)
{
  RadosFsInode inode;
  int ret = getDirInode(stat->path, inode, mtdPool);

  if (ret == 0)
  {
    stat->pool = inode.pool;
    stat->translatedPath = inode.inode;

    ret = genericStat(stat->pool->ioctx, stat->translatedPath, &stat->statBuff);
  }

  return ret;
}

int
RadosFsPriv::statFile(RadosFsPoolSP mtdPool, RadosFsStat *stat)
{
  int ret;
  RadosFsPoolSP dataPool;
  std::string poolName("");
  stat->path = getFilePath(stat->path);
  ret = statLink(mtdPool, stat, poolName);

  if (ret == -ENOENT)
  {
    stat->path = getDirPath(stat->path);
    ret = statLink(mtdPool, stat, poolName);

    if (ret == 0)
      stat->pool = getMtdPoolFromName(poolName);
  }
  else
  {
    const RadosFsPoolList &pools = getDataPools(stat->path);
    if (poolName != "")
    {
      const RadosFsPoolList &pools = getDataPools(stat->path);
      RadosFsPoolList::const_iterator it;

      for (it = pools.begin(); it != pools.end(); it++)
      {
        if ((*it)->name == poolName)
        {
          dataPool = *it;
          break;
        }
      }
    }
    else
    {
      dataPool = pools.front();
    }

    stat->pool = dataPool;
  }

  return ret;
}

int
RadosFsPriv::createPrefixDir(RadosFsPoolSP pool, const std::string &prefix)
{
  int ret = 0;

  if (rados_stat(pool->ioctx, prefix.c_str(), 0, 0) != 0)
  {
    RadosFsStat stat;
    stat.path = prefix;
    stat.translatedPath = generateUuid();
    stat.pool = pool;
    stat.statBuff.st_uid = ROOT_UID;
    stat.statBuff.st_gid = ROOT_UID;
    stat.statBuff.st_mode = DEFAULT_MODE_DIR;

    ret = createDirAndInode(&stat);
  }

  return ret;
}

int
RadosFsPriv::addPool(const std::string &name,
                     const std::string &prefix,
                     RadosFsPoolMap *map,
                     pthread_mutex_t *mutex,
                     size_t size)
{
  int ret = -EPERM;
  const std::string &cleanPrefix = sanitizePath(prefix  + "/");

  if (radosCluster == 0)
    return ret;

  if (prefix == "")
  {
    radosfs_debug("The pool's prefix cannot be an empty string");
    return -EINVAL;
  }

  if (map->count(cleanPrefix) > 0)
  {
    radosfs_debug("There is already a pool with the prefix %s. "
                  "Not adding.", cleanPrefix.c_str());
    return -EEXIST;
  }

  rados_ioctx_t ioctx;

  ret = rados_ioctx_create(radosCluster, name.c_str(), &ioctx);

  if (ret != 0)
    return ret;

  RadosFsPool *pool = new RadosFsPool(name.c_str(), size * MEGABYTE_CONVERSION, ioctx);
  RadosFsPoolSP poolSP(pool);

  if (size == 0)
  {
    ret = createPrefixDir(poolSP, cleanPrefix);

    if (ret < 0)
      return ret;
  }

  pthread_mutex_lock(mutex);

  std::pair<std::string, RadosFsPoolSP > entry(cleanPrefix, poolSP);
  map->insert(entry);

  pthread_mutex_unlock(mutex);

  return ret;
}

RadosFsPoolSP
RadosFsPriv::getDataPool(const std::string &path, const std::string &poolName)
{
  RadosFsPoolSP pool;
  size_t maxLength = 0;

  pthread_mutex_lock(&poolMutex);

  RadosFsPoolListMap::const_iterator it;
  for (it = poolMap.begin(); it != poolMap.end(); it++)
  {
    const std::string &prefix = (*it).first;
    const size_t prefixLength = prefix.length();

    if (prefixLength < maxLength)
      continue;

    if (path.compare(0, prefixLength, prefix) == 0)
    {
      const RadosFsPoolList &pools = (*it).second;

      if (poolName != "")
      {
        RadosFsPoolList::const_iterator poolIt;
        for (poolIt = pools.begin(); poolIt != pools.end(); poolIt++)
        {
          if ((*poolIt)->name == poolName)
          {
            pool = *poolIt;
            maxLength = prefixLength;

            break;
          }
        }
      }
      else
      {
        pool = pools.front();
        maxLength = prefixLength;
      }
    }

    if (pool.get())
      break;
  }

  pthread_mutex_unlock(&poolMutex);

  return pool;
}

RadosFsPoolSP
RadosFsPriv::getMtdPoolFromName(const std::string &name)
{
  RadosFsPoolSP pool;

  pthread_mutex_lock(&mtdPoolMutex);

  RadosFsPoolMap::const_iterator it;
  for (it = mtdPoolMap.begin(); it != mtdPoolMap.end(); it++)
  {
    pool = (*it).second;

    if (pool->name == name)
      break;
  }

  pthread_mutex_unlock(&mtdPoolMutex);

  return pool;
}

RadosFsPoolSP
RadosFsPriv::getMetadataPoolFromPath(const std::string &path)
{
  return getPool(path, &mtdPoolMap, &mtdPoolMutex);
}

RadosFsPoolSP
RadosFsPriv::getPool(const std::string &path,
                     RadosFsPoolMap *map,
                     pthread_mutex_t *mutex)
{
  RadosFsPoolSP pool;
  size_t maxLength = 0;

  pthread_mutex_lock(mutex);

  RadosFsPoolMap::const_iterator it;
  for (it = map->begin(); it != map->end(); it++)
  {
    const std::string &prefix = (*it).first;
    const size_t prefixLength = prefix.length();

    if (prefixLength < maxLength)
      continue;

    if (path.compare(0, prefixLength, prefix) == 0)
    {
      pool = map->at(prefix);
      maxLength = prefixLength;
    }
  }

  pthread_mutex_unlock(mutex);

  return pool;
}

std::string
RadosFsPriv::poolPrefix(const std::string &pool,
                        RadosFsPoolMap *map,
                        pthread_mutex_t *mutex) const
{
  std::string prefix("");

  pthread_mutex_lock(mutex);

  RadosFsPoolMap::iterator it;
  for (it = map->begin(); it != map->end(); it++)
  {
    if ((*it).second->name == pool)
    {
      prefix = (*it).first;
      break;
    }
  }

  pthread_mutex_unlock(mutex);

  return prefix;
}

int
RadosFsPriv::removePool(const std::string &name,
                        RadosFsPoolMap *map,
                        pthread_mutex_t *mutex)
{
  int ret = -ENOENT;
  const std::string &prefix = poolPrefix(name, map, mutex);

  pthread_mutex_lock(mutex);

  if (map->count(prefix) > 0)
  {
    map->erase(prefix);
    ret = 0;
  }

  pthread_mutex_unlock(mutex);

  return ret;
}

std::string
RadosFsPriv::poolFromPrefix(const std::string &prefix,
                            RadosFsPoolMap *map,
                            pthread_mutex_t *mutex) const
{
  std::string pool("");

  pthread_mutex_lock(mutex);

  if (map->count(prefix) > 0)
    pool = map->at(prefix)->name;

  pthread_mutex_unlock(mutex);

  return pool;
}

std::vector<std::string>
RadosFsPriv::pools(RadosFsPoolMap *map,
                   pthread_mutex_t *mutex) const
{
  pthread_mutex_lock(mutex);

  std::vector<std::string> pools;

  RadosFsPoolMap::iterator it;
  for (it = map->begin(); it != map->end(); it++)
  {
    pools.push_back((*it).second->name);
  }

  pthread_mutex_unlock(mutex);

  return pools;
}

RadosFsPoolList
RadosFsPriv::getDataPools(const std::string &path)
{
  size_t maxLength = 0;
  std::string prefixFound("");
  RadosFsPoolList pools;

  pthread_mutex_lock(&poolMutex);

  RadosFsPoolListMap::const_iterator it;
  for (it = poolMap.begin(); it != poolMap.end(); it++)
  {
    const std::string &prefix = (*it).first;
    const size_t prefixLength = prefix.length();

    if (prefixLength < maxLength)
      continue;

    if (path.compare(0, prefixLength, prefix) == 0)
    {
      prefixFound = prefix;
      maxLength = prefixLength;
    }
  }

  if (prefixFound != "")
    pools = poolMap[prefixFound];

  pthread_mutex_unlock(&poolMutex);

  return pools;
}

const std::string
RadosFsPriv::getParentDir(const std::string &obj, int *pos)
{
  size_t length = obj.length();
  size_t index = obj.rfind(PATH_SEP, length - 2);

  if (length - 1 < 1 || index == std::string::npos)
    return "";

  index++;

  if (pos)
    *pos = index;

  return obj.substr(0, index);
}

void
RadosFsPriv::updateDirCache(std::tr1::shared_ptr<DirCache> &cache)
{
  pthread_mutex_lock(&dirCacheMutex);

  dirCache.update(cache);

  pthread_mutex_unlock(&dirCacheMutex);
}

void
RadosFsPriv::removeDirCache(std::tr1::shared_ptr<DirCache> &cache)
{
  pthread_mutex_lock(&dirCacheMutex);

  dirCache.removeCache(cache->inode());

  pthread_mutex_unlock(&dirCacheMutex);
}

std::tr1::shared_ptr<DirCache>
RadosFsPriv::getDirInfo(const std::string &inode, RadosFsPoolSP pool,
                        bool addToCache)
{
  std::tr1::shared_ptr<DirCache> cache;

  pthread_mutex_lock(&dirCacheMutex);

  if (dirCache.cacheMap.count(inode) == 0)
  {
    if (!pool)
    {
      return cache;
    }

    DirCache *dirInfo = new DirCache(inode, pool);
    cache = std::tr1::shared_ptr<DirCache>(dirInfo);

    if (addToCache)
      dirCache.update(cache);
  }
  else
    cache = dirCache.cacheMap[inode]->cachePtr;

  pthread_mutex_unlock(&dirCacheMutex);

  return cache;
}

RadosFsIOSP
RadosFsPriv::getRadosFsIO(const std::string &path)
{
  RadosFsIOSP fsIO;
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  if (operations.count(path) != 0)
    fsIO = operations[path];

  return fsIO;
}

RadosFsIOSP
RadosFsPriv::getOrCreateFsIO(const std::string &path, const RadosFsStat *stat)
{
  RadosFsIOSP fsIO = getRadosFsIO(path);

  if (!fsIO)
  {
    int stripeSize = 0;

    if (stat->extraData.count(XATTR_FILE_STRIPE_SIZE))
    {
      stripeSize = atol(stat->extraData.at(XATTR_FILE_STRIPE_SIZE).c_str());
    }

    if (stripeSize == 0)
      stripeSize = alignStripeSize(radosFs->fileStripeSize(),
                                   stat->pool->alignment);

    fsIO = RadosFsIOSP(new RadosFsIO(radosFs, stat->pool, stat->translatedPath,
                                     stripeSize));

    setRadosFsIO(fsIO);
  }

  return fsIO;
}

void
RadosFsPriv::setRadosFsIO(RadosFsIOSP sharedFsIO)
{
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  operations[sharedFsIO->inode()] = sharedFsIO;
}

void
RadosFsPriv::removeRadosFsIO(RadosFsIOSP sharedFsIO)
{
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  if (operations.count(sharedFsIO->inode()))
    operations.erase(sharedFsIO->inode());
}

std::vector<RadosFsStat>
RadosFsPriv::getParentsForTMTimeUpdate(const std::string &path)
{
  std::vector<RadosFsStat> parents;
  std::string currentPath = path;

  while ((currentPath = getParentDir(currentPath, 0)) != "")
  {
    RadosFsStat parentStat;

    int ret = stat(currentPath, &parentStat);

    if (ret != 0)
    {
      radosfs_debug("Problem statting %s : %s", currentPath.c_str(),
                    strerror(abs(ret)));
      break;
    }

    if (!hasTMTimeEnabled(parentStat.statBuff.st_mode))
      break;

    parents.push_back(parentStat);
  }

  return parents;
}

void
RadosFsPriv::updateTMTime(RadosFsStat *stat, timespec *spec)
{
  std::vector<RadosFsStat> parents = getParentsForTMTimeUpdate(stat->path);
  std::vector<RadosFsStat>::iterator it;
  std::string timeStr;

  if (spec)
    timeStr = timespecToStr(spec);
  else
    timeStr = getCurrentTimeStr();

  for (it = parents.begin(); it != parents.end(); it++)
  {
    updateTimeAsync(&(*it), XATTR_TMTIME, timeStr);
  }
}

void
RadosFsPriv::updateDirTimes(RadosFsStat *stat, timespec *spec)
{
  std::string timeStr;
  timespec timeInfo;

  if (spec)
    timeInfo = *spec;
  else
    clock_gettime(CLOCK_REALTIME, &timeInfo);

  timeStr = timespecToStr(&timeInfo);

  updateTimeAsync(stat, XATTR_MTIME, timeStr);

  updateTMTime(stat, &timeInfo);
}

void
RadosFsPriv::checkFileLocks(void)
{
  const boost::chrono::milliseconds sleepTime(FILE_OPS_IDLE_CHECKER_SLEEP);
  std::map<std::string, std::tr1::shared_ptr<RadosFsIO> >::iterator it, oldIt;

  while (true)
  {
    boost::unique_lock<boost::mutex> lock(operationsMutex);
    it = operations.begin();
    while (it != operations.end())
    {
      boost::this_thread::interruption_point();
      RadosFsIOSP fsIO = (*it).second;
      if (fsIO)
      {
        if (RadosFsIO::hasSingleClient(fsIO))
        {
          oldIt = it;
          it++;
          operations.erase(oldIt);
          continue;
        }
        else
          fsIO->manageIdleLock(FILE_IDLE_LOCK_TIMEOUT);
      }

      it++;
    }
    lock.unlock();
    boost::this_thread::sleep_for(sleepTime);
  }
}

RadosFs::RadosFs()
  : mPriv(new RadosFsPriv(this))
{
}

RadosFs::~RadosFs()
{
  delete mPriv;
}

int
RadosFs::init(const std::string &userName, const std::string &configurationFile)
{
  int ret = mPriv->createCluster(userName, configurationFile);

  return ret;
}

int
RadosFs::addDataPool(const std::string &name,
                     const std::string &prefix,
                     size_t size)
{
  RadosFsPool *pool;
  RadosFsPoolList *pools = 0;
  RadosFsPoolListMap *map;
  rados_ioctx_t ioctx;
  int ret = -EPERM;
  const std::string &cleanPrefix = sanitizePath(prefix  + "/");

  if (mPriv->radosCluster == 0)
    return ret;

  if (prefix == "")
  {
    radosfs_debug("The pool's prefix cannot be an empty string");
    return -EINVAL;
  }

  pthread_mutex_lock(&mPriv->poolMutex);

  map = &mPriv->poolMap;

  if (map->count(cleanPrefix) > 0)
  {
    pools = &map->at(cleanPrefix);

    RadosFsPoolList::const_iterator it;

    for (it = pools->begin(); it != pools->end(); it++)
    {
      if ((*it)->name == name)
      {
        radosfs_debug("The pool %s is already associated with the prefix %s. "
                      "Not adding.", name.c_str(), cleanPrefix.c_str());
        ret = -EEXIST;
        goto unlockAndExit;
      }
    }
  }

  ret = rados_ioctx_create(mPriv->radosCluster, name.c_str(), &ioctx);

  if (ret != 0)
    goto unlockAndExit;

  pool = new RadosFsPool(name.c_str(), size * MEGABYTE_CONVERSION, ioctx);
  pool->setAlignment(rados_ioctx_pool_required_alignment(ioctx));

  if (pools == 0)
  {
    RadosFsPoolList poolList;
    std::pair<std::string, RadosFsPoolList> entry(cleanPrefix, poolList);
    map->insert(entry);
    pools = &map->at(cleanPrefix);
  }

  pools->push_back(RadosFsPoolSP(pool));

unlockAndExit:
  pthread_mutex_unlock(&mPriv->poolMutex);

  return ret;
}

int
RadosFs::removeDataPool(const std::string &name)
{
  int ret = -ENOENT;
  RadosFsPoolListMap *map = &mPriv->poolMap;

  pthread_mutex_lock(&mPriv->poolMutex);

  RadosFsPoolListMap::iterator it;

  for (it = map->begin(); it != map->end(); it++)
  {
    RadosFsPoolList &pools = (*it).second;
    RadosFsPoolList::iterator poolsIt;

    for (poolsIt = pools.begin(); poolsIt != pools.end(); poolsIt++)
    {
      if ((*poolsIt)->name == name)
      {
        pools.erase(poolsIt);
        ret = 0;

        if (pools.empty())
        {
          map->erase((*it).first);
        }

        break;
      }
    }

    if (ret == 0)
      break;
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return ret;
}

std::vector<std::string>
RadosFs::dataPools(const std::string &prefix) const
{
  std::vector<std::string> pools;
  const std::string &dirPrefix = getDirPath(prefix);

  pthread_mutex_lock(&mPriv->poolMutex);

  if (mPriv->poolMap.count(dirPrefix) > 0)
  {
    const RadosFsPoolList &poolList = mPriv->poolMap[dirPrefix];
    RadosFsPoolList::const_iterator it;

    for (it = poolList.begin(); it != poolList.end(); it++)
    {
      pools.push_back((*it)->name);
    }
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return pools;
}

std::string
RadosFs::dataPoolPrefix(const std::string &pool) const
{
  std::string prefix("");

  pthread_mutex_lock(&mPriv->poolMutex);

  RadosFsPoolListMap::const_iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    const RadosFsPoolList &pools = (*it).second;
    RadosFsPoolList::const_iterator poolIt;

    for (poolIt = pools.begin(); poolIt != pools.end(); poolIt++)
    {
      if ((*poolIt)->name == pool)
      {
        prefix = (*it).first;
        break;
      }
    }

    if (prefix != "")
      break;
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return prefix;
}

int
RadosFs::dataPoolSize(const std::string &pool) const
{
  int size = 0;

  pthread_mutex_lock(&mPriv->poolMutex);

  RadosFsPoolListMap::const_iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    const RadosFsPoolList &pools = (*it).second;
    RadosFsPoolList::const_iterator poolIt;

    for (poolIt = pools.begin(); poolIt != pools.end(); poolIt++)
    {
      if ((*poolIt)->name == pool)
      {
        size = (*poolIt)->size;
        break;
      }
    }

    if (size != 0)
      break;
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return size;
}

int
RadosFs::addMetadataPool(const std::string &name, const std::string &prefix)
{
  return mPriv->addPool(name,
                        prefix,
                        &mPriv->mtdPoolMap,
                        &mPriv->mtdPoolMutex);
}

int
RadosFs::removeMetadataPool(const std::string &name)
{
  return mPriv->removePool(name, &mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

std::vector<std::string>
RadosFs::metadataPools() const
{
  return mPriv->pools(&mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

std::string
RadosFs::metadataPoolPrefix(const std::string &pool) const
{
  return mPriv->poolPrefix(pool, &mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

std::string
RadosFs::metadataPoolFromPrefix(const std::string &prefix) const
{
  return mPriv->poolFromPrefix(prefix, &mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

void
RadosFs::setIds(uid_t uid, gid_t gid)
{
  mPriv->setUid(uid);
  mPriv->setGid(gid);
}

void
RadosFs::getIds(uid_t *uid, gid_t *gid) const
{
  *uid = mPriv->getUid();
  *gid = mPriv->getGid();
}

uid_t
RadosFs::uid(void) const
{
  return mPriv->uid;
}

uid_t
RadosFs::gid(void) const
{
  return mPriv->gid;
}

static void
gatherPathsByParentDir(const std::vector<std::string> &paths,
                       std::map<std::string, std::vector<std::string> > &entries)
{
  for (size_t i = 0; i < paths.size(); i++)
  {
    const std::string &path = paths[i];

    if (path == "/" && entries.count(path) == 0)
    {
      entries[path].clear();
      continue;
    }

    std::string parentDir = getParentDir(path, 0);
    entries[parentDir].push_back(path.substr(parentDir.length()));
  }
}

std::vector<std::pair<int, struct stat> >
RadosFs::stat(const std::vector<std::string> &paths)
{
  std::map<std::string, std::pair<int, struct stat> > stats;
  std::map<std::string, std::vector<std::string> > entries;

  gatherPathsByParentDir(paths, entries);

  mPriv->parallelStat(entries, &stats);

  // Call parallel stat again on the paths that were not found
  entries.clear();
  std::map<std::string, std::pair<int, struct stat> >::iterator it;
  for (it = stats.begin(); it != stats.end(); it++)
  {
    std::pair<int, struct stat> statResult = (*it).second;

    if (statResult.first != 0)
    {
      const std::string &path = (*it).first;
      entries[path].clear();
    }
  }

  if (entries.size())
    mPriv->parallelStat(entries, &stats);

  std::vector<std::pair<int, struct stat> > results;
  for (size_t i = 0; i < paths.size(); i++)
  {
    const std::string &path = paths[i];
    results.push_back(stats[path]);
  }

  return results;
}

int
RadosFs::stat(const std::string &path, struct stat *buff)
{
  int ret = -ENOENT;

  const std::string &sanitizedPath = sanitizePath(path);

  if (isDirPath(sanitizedPath))
  {
    RadosFsDir dir(this, sanitizedPath);

    ret = dir.stat(buff);
  }

  if (ret != 0)
  {
    RadosFsFile file(this, sanitizedPath, RadosFsFile::MODE_READ);

    ret = file.stat(buff);
  }

  return ret;
}

std::vector<std::string>
RadosFs::allPoolsInCluster() const
{
  const int poolListMaxSize = 1024;
  char poolList[poolListMaxSize];
  rados_pool_list(mPriv->radosCluster, poolList, poolListMaxSize);

  char *currentPool = poolList;
  std::vector<std::string> poolVector;

  while (strlen(currentPool) != 0)
  {
    poolVector.push_back(currentPool);
    currentPool += strlen(currentPool) + 1;
  }

  return poolVector;
}

int
RadosFs::statCluster(uint64_t *totalSpaceKb,
                     uint64_t *usedSpaceKb,
                     uint64_t *availableSpaceKb,
                     uint64_t *numberOfObjects)
{
  int ret;
  rados_cluster_stat_t clusterStat;

  ret = rados_cluster_stat(mPriv->radosCluster, &clusterStat);

  if (ret != 0)
    return ret;

  if (totalSpaceKb)
    *totalSpaceKb = clusterStat.kb;

  if (usedSpaceKb)
    *usedSpaceKb = clusterStat.kb_used;

  if (availableSpaceKb)
    *availableSpaceKb = clusterStat.kb_avail;

  if (numberOfObjects)
    *numberOfObjects = clusterStat.num_objects;

  return ret;
}

int
RadosFs::setXAttr(const std::string &path,
                  const std::string &attrName,
                  const std::string &value)
{
  RadosFsStat stat;

  int ret = mPriv->stat(path, &stat);

  if (ret != 0)
    return ret;

  if (S_ISLNK(stat.statBuff.st_mode))
    return setXAttr(stat.translatedPath, attrName, value);

  if (!stat.pool)
    return -ENODEV;

  if (S_ISDIR(stat.statBuff.st_mode))
    return setXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                            stat.path, attrName, value);

  if (S_ISREG(stat.statBuff.st_mode))
  {
    return setXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                            stat.translatedPath, attrName, value);
  }

  return -EPERM;
}

int
RadosFs::getXAttr(const std::string &path,
                  const std::string &attrName,
                  std::string &value,
                  size_t length)
{
  RadosFsStat stat;

  int ret = mPriv->stat(path, &stat);

  if (ret != 0)
    return ret;

  if (S_ISLNK(stat.statBuff.st_mode))
    return getXAttr(stat.translatedPath, attrName, value, length);

  if (!stat.pool)
    return -ENODEV;

  if (S_ISDIR(stat.statBuff.st_mode))
    return getXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                            stat.path, attrName, value, length);

  if (S_ISREG(stat.statBuff.st_mode))
  {
    return getXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                            stat.translatedPath, attrName, value, length);
  }

  return -EPERM;
}

int
RadosFs::removeXAttr(const std::string &path,
                     const std::string &attrName)
{
  RadosFsStat stat;

  int ret = mPriv->stat(path, &stat);

  if (ret != 0)
    return ret;

  if (S_ISLNK(stat.statBuff.st_mode))
    return removeXAttr(stat.translatedPath, attrName);

  if (!stat.pool)
    return -ENODEV;

  if (S_ISDIR(stat.statBuff.st_mode))
    return removeXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                               path, attrName);

  if (S_ISREG(stat.statBuff.st_mode))
  {
    return removeXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                               stat.translatedPath, attrName);
  }

  return -EPERM;
}

int
RadosFs::getXAttrsMap(const std::string &path,
                      std::map<std::string, std::string> &map)
{
  RadosFsStat stat;

  int ret = mPriv->stat(path, &stat);

  if (ret != 0)
    return ret;

  if (S_ISLNK(stat.statBuff.st_mode))
    return getXAttrsMap(stat.translatedPath, map);

  if (!stat.pool)
    return -ENODEV;

  if (S_ISDIR(stat.statBuff.st_mode))
    return getMapOfXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                                 stat.path, map);

  if (S_ISREG(stat.statBuff.st_mode))
  {
    return getMapOfXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                                 stat.translatedPath, map);
  }

  return -EPERM;
}

void
RadosFs::setDirCacheMaxSize(size_t size)
{
  mPriv->dirCache.maxCacheSize = size;
  mPriv->dirCache.adjustCache();
}

size_t
RadosFs::dirCacheMaxSize(void) const
{
  return mPriv->dirCache.maxCacheSize;
}

void
RadosFs::setDirCompactRatio(float ratio)
{
  mPriv->dirCompactRatio = ratio;
}

float
RadosFs::dirCompactRatio(void) const
{
  return mPriv->dirCompactRatio;
}

void
RadosFs::setLogLevel(const RadosFs::LogLevel level)
{
  mPriv->logger.setLogLevel(level);
}

RadosFs::LogLevel
RadosFs::logLevel(void) const
{
  return mPriv->logger.logLevel();
}

void
RadosFs::setFileStripeSize(const size_t size)
{
  size_t realSize = size;

  if (size == 0)
  {
    realSize = 1;
    radosfs_debug("Cannot set the file stripe size as 0. Setting to %d.",
                  realSize);
  }

  mPriv->fileStripeSize = realSize;
}

size_t
RadosFs::fileStripeSize(void) const
{
  return mPriv->fileStripeSize;
}


void
RadosFs::setFileLocking(bool lock)
{
  mPriv->lockFiles = lock;
}

bool
RadosFs::fileLocking(void) const
{
  return mPriv->lockFiles;
}

RadosFsInfo *
RadosFs::getFsInfo(const std::string &path)
{
  RadosFsStat stat;

  if (mPriv->stat(path, &stat) != 0)
  {
    return 0;
  }

  if (S_ISLNK(stat.statBuff.st_mode) && isDirPath(stat.translatedPath))
  {
    return new RadosFsDir(this, stat.path);
  }
  else if (S_ISDIR(stat.statBuff.st_mode))
  {
    return new RadosFsDir(this, stat.path);
  }

  return new RadosFsFile(this, stat.path);
}

int
RadosFs::getInodeAndPool(const std::string &path, std::string *inode,
                         std::string *pool)
{
  RadosFsStat stat;
  int ret = mPriv->stat(path, &stat);

  if (ret == 0)
  {
    if (S_ISLNK(stat.statBuff.st_mode))
    {
      ret = -EINVAL;
      radosfs_debug("%s: The path '%s' is a link. Links have no inodes.",
                    strerror(abs(ret)), path.c_str());
    }

    if (inode)
      inode->assign(stat.translatedPath);

    if (pool)
      pool->assign(stat.pool->name);
  }

  return ret;
}

RADOS_FS_END_NAMESPACE
