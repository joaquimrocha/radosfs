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
#include <rados/librados.hpp>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include "RadosFs.hh"
#include "RadosFsFile.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

__thread uid_t FsPriv::uid;
__thread gid_t FsPriv::gid;

FsPriv::FsPriv(Fs *radosFs)
  : radosFs(radosFs),
    initialized(false),
    dirCompactRatio(DEFAULT_DIR_COMPACT_RATIO),
    fileStripeSize(FILE_STRIPE_SIZE),
    lockFiles(true),
    ioService(new boost::asio::io_service),
    asyncWork(new boost::asio::io_service::work(*ioService)),
    fileOpsIdleChecker(boost::bind(&FsPriv::checkFileLocks, this))
{
  uid = 0;
  gid = 0;
  dirCache.maxCacheSize = DEFAULT_DIR_CACHE_MAX_SIZE;

  pthread_mutex_init(&poolMutex, 0);
  pthread_mutex_init(&mtdPoolMutex, 0);
  pthread_mutex_init(&dirCacheMutex, 0);
  pthread_mutex_init(&dirPathInodeMutex, 0);
}

FsPriv::~FsPriv()
{
  fileOpsIdleChecker.interrupt();
  operationsMutex.lock();
  operations.clear();
  operationsMutex.unlock();

  poolMap.clear();
  mtdPoolMap.clear();
  dirPathInodeMap.clear();

  if (initialized)
    radosCluster.shutdown();

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
FsPriv::createCluster(const std::string &userName, const std::string &confFile)
{
  int ret;
  const char *user = 0;

  if (userName != "")
    user = userName.c_str();

  ret = radosCluster.init(user);

  if (ret != 0)
      return ret;

  if (confFile != "")
  {
    ret = radosCluster.conf_read_file(confFile.c_str());

    if (ret != 0)
        return ret;
  }

  ret = radosCluster.connect();

  initialized = (ret == 0);

  return ret;
}

int
FsPriv::statLink(PoolSP mtdPool, Stat *stat, std::string &pool)
{
  int ret = 0;
  const std::string &parentDir = getParentDir(stat->path, 0);
  const std::string &pathXAttr = XATTR_FILE_PREFIX +
                                 stat->path.substr(parentDir.length());

  stat->statBuff.st_size = 0;

  if (mtdPool == 0)
    return -ENODEV;

  Inode inode;
  ret = getDirInode(parentDir, inode, mtdPool);

  if (ret != 0)
    return ret;

  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> omap;

  keys.insert(pathXAttr);
  ret = inode.pool->ioctx.omap_get_vals_by_keys(inode.inode, keys, &omap);

  if (omap.count(pathXAttr) == 0)
    ret = -ENOENT;

  if (ret < 0)
    return ret;

  librados::bufferlist fileXAttr = omap[pathXAttr];
  std::string linkStr(fileXAttr.c_str(), fileXAttr.length());

  ret = statFromXAttr(stat->path, linkStr, &stat->statBuff,
                      stat->translatedPath, pool, stat->extraData);

  if (stat->translatedPath == "")
  {
    ret = -ENOLINK;
  }

  return ret;
}

int
FsPriv::getRealPath(const std::string &path,  Stat *stat, std::string &realPath)
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
FsPriv::getDirInode(const std::string &path, Inode &inode, PoolSP &mtdPool)
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

    PoolSP pool = getMtdPoolFromName(poolName);

    if (!pool)
      return -ENODEV;

    inode.inode = inodeName;
    inode.pool = pool;
    setDirInode(path, inode);
  }

  return 0;
}

void
FsPriv::setDirInode(const std::string &path, const Inode &inode)
{
  pthread_mutex_lock(&dirPathInodeMutex);

  dirPathInodeMap[path] = inode;

  pthread_mutex_unlock(&dirPathInodeMutex);
}

void
FsPriv::updateDirInode(const std::string &oldPath,
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
FsPriv::removeDirInode(const std::string &path)
{
  pthread_mutex_lock(&dirPathInodeMutex);

  dirPathInodeMap.erase(path);

  pthread_mutex_unlock(&dirPathInodeMutex);
}

void
FsPriv::launchThreads(void)
{
  size_t currentLaunchedThreads = generalWorkerThreads.size();
  size_t threadsToLaunch = DEFAULT_NUM_WORKER_THREADS - currentLaunchedThreads;
  while (threadsToLaunch-- > 0)
  {
    generalWorkerThreads.create_thread(
          boost::bind(&FsPriv::generalWorkerThread, this, ioService));
  }
}

boost::shared_ptr<boost::asio::io_service>
FsPriv::getIoService()
{
  launchThreads();
  return ioService;
}

void
FsPriv::statXAttrInThread(std::string path, std::string xattr, Stat *stat,
                          int *ret, boost::mutex *mutex,
                          boost::condition_variable *cond, int *numJobs)
{
  PoolSP dataPool;
  std::string pool;
  *ret = statFromXAttr(path, xattr, &stat->statBuff, stat->translatedPath,
                          pool, stat->extraData);

  if (*ret != 0)
  {
    return;
  }

  dataPool = getDataPool(path, pool);
  stat->pool = dataPool;
  FileIOSP fileIO = getOrCreateFileIO(stat->translatedPath, stat);
  stat->statBuff.st_size = fileIO->getSize();

  bool notify = false;

  {
    boost::unique_lock<boost::mutex> lock(*mutex);

    notify = --*numJobs == 0;
  }

  if (notify)
    cond->notify_all();
}

void
FsPriv::generalWorkerThread(
    boost::shared_ptr<boost::asio::io_service> ioService)
{
  ioService->run();
}

void
FsPriv::statEntries(StatAsyncInfo *info,
                    std::map<std::string, std::string> &xattrs)
{
  Stat *stats = new Stat[info->entries->size()];
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

    ioService->post(boost::bind(&FsPriv::statXAttrInThread, this, path,
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
FsPriv::statAsync(StatAsyncInfo *info)
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
FsPriv::statAsyncInfoInThread(const std::string path, StatAsyncInfo *info,
                              boost::mutex *mutex,
                              boost::condition_variable *cond, int *numJobs)
{
  int ret;
  info->stat.reset();
  info->stat.path = getDirPath(path);
  PoolSP mtdPool = getMetadataPoolFromPath(info->stat.path);

  if (!mtdPool.get())
  {
    ret = -ENODEV;
    return ret;
  }

  Inode inode;
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
FsPriv::parallelStat(
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

    getIoService()->post(boost::bind(&FsPriv::statAsyncInfoInThread, this,
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
FsPriv::stat(const std::string &path, Stat *stat)
{
  PoolSP mtdPool;
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
FsPriv::statDir(PoolSP mtdPool, Stat *stat)
{
  Inode inode;
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
FsPriv::statFile(PoolSP mtdPool, Stat *stat)
{
  int ret;
  PoolSP dataPool;
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
    const PoolList &pools = getDataPools(stat->path);
    if (poolName != "")
    {
      const PoolList &pools = getDataPools(stat->path);
      PoolList::const_iterator it;

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
FsPriv::createPrefixDir(PoolSP pool, const std::string &prefix)
{
  int ret = 0;

  if (pool->ioctx.stat(prefix, 0, 0) != 0)
  {
    Stat stat;
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
FsPriv::addPool(const std::string &name, const std::string &prefix,
                PoolMap *map, pthread_mutex_t *mutex, size_t size)
{
  int ret = -ENODEV;
  const std::string &cleanPrefix = sanitizePath(prefix  + "/");

  if (!initialized)
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

  librados::IoCtx ioctx;

  ret = radosCluster.ioctx_create(name.c_str(), ioctx);

  if (ret != 0)
    return ret;

  Pool *pool = new Pool(name.c_str(), size * MEGABYTE_CONVERSION,
                        ioctx);
  PoolSP poolSP(pool);

  if (size == 0)
  {
    ret = createPrefixDir(poolSP, cleanPrefix);

    if (ret < 0)
      return ret;
  }

  pthread_mutex_lock(mutex);

  std::pair<std::string, PoolSP > entry(cleanPrefix, poolSP);
  map->insert(entry);

  pthread_mutex_unlock(mutex);

  return ret;
}

PoolSP
FsPriv::getDataPool(const std::string &path, const std::string &poolName)
{
  PoolSP pool;
  size_t maxLength = 0;

  pthread_mutex_lock(&poolMutex);

  PoolListMap::const_iterator it;
  for (it = poolMap.begin(); it != poolMap.end(); it++)
  {
    const std::string &prefix = (*it).first;
    const size_t prefixLength = prefix.length();

    if (prefixLength < maxLength)
      continue;

    if (path.compare(0, prefixLength, prefix) == 0)
    {
      const PoolList &pools = (*it).second;

      if (poolName != "")
      {
        PoolList::const_iterator poolIt;
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

PoolSP
FsPriv::getMtdPoolFromName(const std::string &name)
{
  PoolSP pool;

  pthread_mutex_lock(&mtdPoolMutex);

  PoolMap::const_iterator it;
  for (it = mtdPoolMap.begin(); it != mtdPoolMap.end(); it++)
  {
    pool = (*it).second;

    if (pool->name == name)
      break;
  }

  pthread_mutex_unlock(&mtdPoolMutex);

  return pool;
}

PoolSP
FsPriv::getMetadataPoolFromPath(const std::string &path)
{
  return getPool(path, &mtdPoolMap, &mtdPoolMutex);
}

PoolSP
FsPriv::getPool(const std::string &path,
                PoolMap *map,
                pthread_mutex_t *mutex)
{
  PoolSP pool;
  size_t maxLength = 0;

  pthread_mutex_lock(mutex);

  PoolMap::const_iterator it;
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
FsPriv::poolPrefix(const std::string &pool, PoolMap *map,
                   pthread_mutex_t *mutex) const
{
  std::string prefix("");

  pthread_mutex_lock(mutex);

  PoolMap::iterator it;
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
FsPriv::removePool(const std::string &name, PoolMap *map,
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
FsPriv::poolFromPrefix(const std::string &prefix, PoolMap *map,
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
FsPriv::pools(PoolMap *map,
              pthread_mutex_t *mutex) const
{
  pthread_mutex_lock(mutex);

  std::vector<std::string> pools;

  PoolMap::iterator it;
  for (it = map->begin(); it != map->end(); it++)
  {
    pools.push_back((*it).second->name);
  }

  pthread_mutex_unlock(mutex);

  return pools;
}

PoolList
FsPriv::getDataPools(const std::string &path)
{
  size_t maxLength = 0;
  std::string prefixFound("");
  PoolList pools;

  pthread_mutex_lock(&poolMutex);

  PoolListMap::const_iterator it;
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
FsPriv::getParentDir(const std::string &obj, int *pos)
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
FsPriv::updateDirCache(std::tr1::shared_ptr<DirCache> &cache)
{
  pthread_mutex_lock(&dirCacheMutex);

  dirCache.update(cache);

  pthread_mutex_unlock(&dirCacheMutex);
}

void
FsPriv::removeDirCache(std::tr1::shared_ptr<DirCache> &cache)
{
  pthread_mutex_lock(&dirCacheMutex);

  dirCache.removeCache(cache->inode());

  pthread_mutex_unlock(&dirCacheMutex);
}

std::tr1::shared_ptr<DirCache>
FsPriv::getDirInfo(const std::string &inode, PoolSP pool,
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

FileIOSP
FsPriv::getFileIO(const std::string &path)
{
  FileIOSP io;
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  if (operations.count(path) != 0)
    io = operations[path];

  return io;
}

FileIOSP
FsPriv::getOrCreateFileIO(const std::string &path, const Stat *stat)
{
  FileIOSP io = getFileIO(path);

  if (!io)
  {
    int stripeSize = 0;

    if (stat->extraData.count(XATTR_FILE_STRIPE_SIZE))
    {
      stripeSize = atol(stat->extraData.at(XATTR_FILE_STRIPE_SIZE).c_str());
    }

    if (stripeSize == 0)
      stripeSize = alignStripeSize(radosFs->fileStripeSize(),
                                   stat->pool->alignment);

    io = FileIOSP(new FileIO(radosFs, stat->pool, stat->translatedPath,
                               stripeSize));

    setFileIO(io);
  }

  return io;
}

void
FsPriv::setFileIO(FileIOSP sharedFileIO)
{
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  operations[sharedFileIO->inode()] = sharedFileIO;
}

void
FsPriv::removeFileIO(FileIOSP sharedFileIO)
{
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  if (operations.count(sharedFileIO->inode()))
    operations.erase(sharedFileIO->inode());
}

std::vector<Stat>
FsPriv::getParentsForTMTimeUpdate(const std::string &path)
{
  std::vector<Stat> parents;
  std::string currentPath = path;

  while ((currentPath = getParentDir(currentPath, 0)) != "")
  {
    Stat parentStat;

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
FsPriv::updateTMTime(Stat *stat, timespec *spec)
{
  std::vector<Stat> parents = getParentsForTMTimeUpdate(stat->path);
  std::vector<Stat>::iterator it;
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
FsPriv::updateDirTimes(Stat *stat, timespec *spec)
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
FsPriv::checkFileLocks(void)
{
  const boost::chrono::milliseconds sleepTime(FILE_OPS_IDLE_CHECKER_SLEEP);
  std::map<std::string, std::tr1::shared_ptr<FileIO> >::iterator it, oldIt;

  while (true)
  {
    boost::unique_lock<boost::mutex> lock(operationsMutex);
    it = operations.begin();
    while (it != operations.end())
    {
      boost::this_thread::interruption_point();
      FileIOSP io = (*it).second;
      if (io)
      {
        if (FileIO::hasSingleClient(io))
        {
          oldIt = it;
          it++;
          operations.erase(oldIt);
          continue;
        }
        else
          io->manageIdleLock(FILE_IDLE_LOCK_TIMEOUT);
      }

      it++;
    }
    lock.unlock();
    boost::this_thread::sleep_for(sleepTime);
  }
}

Fs::Fs()
  : mPriv(new FsPriv(this))
{
}

Fs::~Fs()
{
  delete mPriv;
}

int
Fs::init(const std::string &userName, const std::string &configurationFile)
{
  int ret = mPriv->createCluster(userName, configurationFile);

  return ret;
}

int
Fs::addDataPool(const std::string &name, const std::string &prefix, size_t size)
{
  librados::IoCtx ioctx;
  Pool *pool;
  PoolList *pools = 0;
  PoolListMap *map;
  int ret = -EPERM;
  const std::string &cleanPrefix = sanitizePath(prefix  + "/");

  if (!mPriv->initialized)
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

    PoolList::const_iterator it;

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

  ret = mPriv->radosCluster.ioctx_create(name.c_str(), ioctx);

  if (ret != 0)
    goto unlockAndExit;

  pool = new Pool(name.c_str(), size * MEGABYTE_CONVERSION, ioctx);
  pool->setAlignment(ioctx.pool_required_alignment());

  if (pools == 0)
  {
    PoolList poolList;
    std::pair<std::string, PoolList> entry(cleanPrefix, poolList);
    map->insert(entry);
    pools = &map->at(cleanPrefix);
  }

  pools->push_back(PoolSP(pool));

unlockAndExit:
  pthread_mutex_unlock(&mPriv->poolMutex);

  return ret;
}

int
Fs::removeDataPool(const std::string &name)
{
  int ret = -ENOENT;
  PoolListMap *map = &mPriv->poolMap;

  pthread_mutex_lock(&mPriv->poolMutex);

  PoolListMap::iterator it;

  for (it = map->begin(); it != map->end(); it++)
  {
    PoolList &pools = (*it).second;
    PoolList::iterator poolsIt;

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
Fs::dataPools(const std::string &prefix) const
{
  std::vector<std::string> pools;
  const std::string &dirPrefix = getDirPath(prefix);

  pthread_mutex_lock(&mPriv->poolMutex);

  if (mPriv->poolMap.count(dirPrefix) > 0)
  {
    const PoolList &poolList = mPriv->poolMap[dirPrefix];
    PoolList::const_iterator it;

    for (it = poolList.begin(); it != poolList.end(); it++)
    {
      pools.push_back((*it)->name);
    }
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return pools;
}

std::string
Fs::dataPoolPrefix(const std::string &pool) const
{
  std::string prefix("");

  pthread_mutex_lock(&mPriv->poolMutex);

  PoolListMap::const_iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    const PoolList &pools = (*it).second;
    PoolList::const_iterator poolIt;

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
Fs::dataPoolSize(const std::string &pool) const
{
  int size = 0;

  pthread_mutex_lock(&mPriv->poolMutex);

  PoolListMap::const_iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    const PoolList &pools = (*it).second;
    PoolList::const_iterator poolIt;

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
Fs::addMetadataPool(const std::string &name, const std::string &prefix)
{
  return mPriv->addPool(name,
                        prefix,
                        &mPriv->mtdPoolMap,
                        &mPriv->mtdPoolMutex);
}

int
Fs::removeMetadataPool(const std::string &name)
{
  return mPriv->removePool(name, &mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

std::vector<std::string>
Fs::metadataPools() const
{
  return mPriv->pools(&mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

std::string
Fs::metadataPoolPrefix(const std::string &pool) const
{
  return mPriv->poolPrefix(pool, &mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

std::string
Fs::metadataPoolFromPrefix(const std::string &prefix) const
{
  return mPriv->poolFromPrefix(prefix, &mPriv->mtdPoolMap, &mPriv->mtdPoolMutex);
}

void
Fs::setIds(uid_t uid, gid_t gid)
{
  mPriv->setUid(uid);
  mPriv->setGid(gid);
}

void
Fs::getIds(uid_t *uid, gid_t *gid) const
{
  *uid = mPriv->getUid();
  *gid = mPriv->getGid();
}

uid_t
Fs::uid(void) const
{
  return mPriv->uid;
}

uid_t
Fs::gid(void) const
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
Fs::stat(const std::vector<std::string> &paths)
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
Fs::stat(const std::string &path, struct stat *buff)
{
  int ret = -ENOENT;

  const std::string &sanitizedPath = sanitizePath(path);

  if (isDirPath(sanitizedPath))
  {
    Dir dir(this, sanitizedPath);

    ret = dir.stat(buff);
  }

  if (ret != 0)
  {
    File file(this, sanitizedPath, File::MODE_READ);

    ret = file.stat(buff);
  }

  return ret;
}

std::vector<std::string>
Fs::allPoolsInCluster() const
{
  std::vector<std::string> poolVector;
  std::list<std::string> poolList;

  mPriv->radosCluster.pool_list(poolList);
  poolVector.insert(poolVector.begin(), poolList.begin(), poolList.end());

  return poolVector;
}

int
Fs::statCluster(uint64_t *totalSpaceKb, uint64_t *usedSpaceKb,
                uint64_t *availableSpaceKb, uint64_t *numberOfObjects)
{
  int ret;
  librados::cluster_stat_t clusterStat;

  ret = mPriv->radosCluster.cluster_stat(clusterStat);

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
Fs::setXAttr(const std::string &path, const std::string &attrName,
             const std::string &value)
{
  Stat stat;

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
Fs::getXAttr(const std::string &path, const std::string &attrName,
             std::string &value)
{
  Stat stat;

  int ret = mPriv->stat(path, &stat);

  if (ret != 0)
    return ret;

  if (S_ISLNK(stat.statBuff.st_mode))
    return getXAttr(stat.translatedPath, attrName, value);

  if (!stat.pool)
    return -ENODEV;

  if (S_ISDIR(stat.statBuff.st_mode))
    return getXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                            stat.path, attrName, value);

  if (S_ISREG(stat.statBuff.st_mode))
  {
    return getXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                            stat.translatedPath, attrName, value);
  }

  return -EPERM;
}

int
Fs::removeXAttr(const std::string &path, const std::string &attrName)
{
  Stat stat;

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
Fs::getXAttrsMap(const std::string &path,
                 std::map<std::string, std::string> &map)
{
  Stat stat;

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
Fs::setDirCacheMaxSize(size_t size)
{
  mPriv->dirCache.maxCacheSize = size;
  mPriv->dirCache.adjustCache();
}

size_t
Fs::dirCacheMaxSize(void) const
{
  return mPriv->dirCache.maxCacheSize;
}

void
Fs::setDirCompactRatio(float ratio)
{
  mPriv->dirCompactRatio = ratio;
}

float
Fs::dirCompactRatio(void) const
{
  return mPriv->dirCompactRatio;
}

void
Fs::setLogLevel(const Fs::LogLevel level)
{
  mPriv->logger.setLogLevel(level);
}

Fs::LogLevel
Fs::logLevel(void) const
{
  return mPriv->logger.logLevel();
}

void
Fs::setFileStripeSize(const size_t size)
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
Fs::fileStripeSize(void) const
{
  return mPriv->fileStripeSize;
}


void
Fs::setFileLocking(bool lock)
{
  mPriv->lockFiles = lock;
}

bool
Fs::fileLocking(void) const
{
  return mPriv->lockFiles;
}

Info *
Fs::getFsInfo(const std::string &path)
{
  Stat stat;

  if (mPriv->stat(path, &stat) != 0)
  {
    return 0;
  }

  if (S_ISLNK(stat.statBuff.st_mode) && isDirPath(stat.translatedPath))
  {
    return new Dir(this, stat.path);
  }
  else if (S_ISDIR(stat.statBuff.st_mode))
  {
    return new Dir(this, stat.path);
  }

  return new File(this, stat.path);
}

int
Fs::getInodeAndPool(const std::string &path, std::string *inode,
                    std::string *pool)
{
  Stat stat;
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
