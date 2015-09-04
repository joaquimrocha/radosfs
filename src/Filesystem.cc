/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2014-2015 CERN, Switzerland
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

#include "Filesystem.hh"
#include "File.hh"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

__thread uid_t FilesystemPriv::uid;
__thread gid_t FilesystemPriv::gid;

FilesystemPriv::FilesystemPriv(Filesystem *radosFs)
  : radosFs(radosFs),
    initialized(false),
    dirCompactRatio(DEFAULT_DIR_COMPACT_RATIO),
    fileChunkSize(FILE_CHUNK_SIZE),
    numGenericWorkers(DEFAULT_NUM_WORKER_THREADS),
    ioService(new boost::asio::io_service),
    asyncWork(new boost::asio::io_service::work(*ioService)),
    fileOpsIdleChecker(boost::bind(&FilesystemPriv::checkFileLocks, this))
{
  uid = 0;
  gid = 0;
  dirCache.maxCacheSize = DEFAULT_DIR_CACHE_MAX_SIZE;
}

FilesystemPriv::~FilesystemPriv()
{
  asyncWork.reset();
  generalWorkerThreads.join_all();

  fileOpsIdleChecker.interrupt();
  operationsMutex.lock();
  operations.clear();
  operationsMutex.unlock();

  poolMap.clear();
  mtdPoolMap.clear();
  dirPathInodeMap.clear();

  if (initialized)
    radosCluster.shutdown();

  boost::unique_lock<boost::mutex> lock(dirCacheMutex);

  dirCache.cleanCache();
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
FilesystemPriv::createCluster(const std::string &userName,
                              const std::string &confFile)
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
FilesystemPriv::statLink(PoolSP mtdPool, Stat *stat, std::string &pool)
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
FilesystemPriv::getRealPath(const std::string &path,  Stat *stat,
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
FilesystemPriv::getDirInode(const std::string &path, Inode &inode,
                            PoolSP &mtdPool)
{
  {
    boost::unique_lock<boost::mutex> (dirPathInodeMutex);

    if (dirPathInodeMap.count(path) > 0)
      inode = dirPathInodeMap[path];
  }

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

int
FilesystemPriv::getDirInode(const std::string &path, Inode &inode)
{
  PoolSP mtdPool = getMetadataPoolFromPath(path);
  return getDirInode(path, inode, mtdPool);
}

void
FilesystemPriv::setDirInode(const std::string &path, const Inode &inode)
{
  boost::unique_lock<boost::mutex> (dirPathInodeMutex);
  dirPathInodeMap[path] = inode;
}

void
FilesystemPriv::updateDirInode(const std::string &oldPath,
                               const std::string &newPath)
{
  boost::unique_lock<boost::mutex> (dirPathInodeMutex);

  if (dirPathInodeMap.count(oldPath) > 0)
  {
    dirPathInodeMap[newPath] = dirPathInodeMap[oldPath];
    dirPathInodeMap.erase(oldPath);
  }
}

void
FilesystemPriv::removeDirInode(const std::string &path)
{
  boost::unique_lock<boost::mutex> (dirPathInodeMutex);
  dirPathInodeMap.erase(path);
}

void
FilesystemPriv::launchThreads(void)
{
  size_t threadsToLaunch = DEFAULT_NUM_WORKER_THREADS;
  size_t currentLaunchedThreads = generalWorkerThreads.size();

  {
    boost::unique_lock<boost::mutex> lock(genericWorkersMutex);

    threadsToLaunch = numGenericWorkers;
  }

  while (threadsToLaunch < currentLaunchedThreads)
  {
    boost::thread *thread = genericWorkersList.back();
    genericWorkersList.pop_back();
    currentLaunchedThreads--;

    generalWorkerThreads.remove_thread(thread);
    delete thread;
  }

  while (threadsToLaunch > currentLaunchedThreads)
  {
    boost::thread *thread = generalWorkerThreads.create_thread(
                              boost::bind(&FilesystemPriv::generalWorkerThread,
                                          this, ioService));
    genericWorkersList.push_back(thread);
    currentLaunchedThreads++;
  }
}

boost::shared_ptr<boost::asio::io_service>
FilesystemPriv::getIoService()
{
  launchThreads();
  return ioService;
}

int
FilesystemPriv::resetFileEntry(Stat &fileStat)
{
  if (!fileStat.pool)
    return -ENODEV;

  std::string parentDir = getParentDir(fileStat.path, 0);
  Stat parentStat;

  int ret = stat(parentDir, &parentStat);

  if (ret != 0)
    return ret;

  std::string omapFileEntry = XATTR_FILE_PREFIX +
                              fileStat.path.substr(parentDir.length());
  std::map<std::string, librados::bufferlist> omap;

  std::string fileEntry = getFileXAttrDirRecord(&fileStat);
  omap[omapFileEntry].append(fileEntry);

  librados::ObjectWriteOperation writeOp;
  writeOp.assert_exists();
  writeOp.omap_set(omap);

  return parentStat.pool->ioctx.operate(parentStat.translatedPath, &writeOp);
}

int
FilesystemPriv::resetDirLogicalObj(Stat &dirStat)
{
  if (!dirStat.pool)
    return -ENODEV;

  std::map<std::string, librados::bufferlist> omap;
  std::string inode = makeInodeXattr(&dirStat);

  omap[XATTR_INODE].append(inode);

  librados::ObjectWriteOperation writeOp;
  writeOp.assert_exists();
  writeOp.omap_set(omap);

  return dirStat.pool->ioctx.operate(dirStat.path, &writeOp);
}

static size_t
getInlineBufferCapacityFromExtraData(
                                 const std::map<std::string, std::string> &data)
{
  std::map<std::string, std::string>::const_iterator it;
  it = data.find(XATTR_FILE_INLINE_BUFFER_SIZE);

  if (it == data.end())
    return 0;

  const std::string &sizeStr = (*it).second;

  return strtoul(sizeStr.c_str(), 0, 10);
}

void
FilesystemPriv::statEntryInThread(std::string path, std::string entry,
                                  size_t inlineBufferSize, Stat *stat, int *ret,
                                  boost::mutex *mutex,
                                  boost::condition_variable *cond, int *numJobs)
{
  PoolSP dataPool;
  std::string pool;
  *ret = statFromXAttr(path, entry, &stat->statBuff, stat->translatedPath,
                       pool, stat->extraData);

  if (*ret != 0)
  {
    return;
  }

  dataPool = getDataPool(path, pool);
  stat->pool = dataPool;

  // We only have to check the file inode's chunks if the inline buffer's
  // capacity is zero of it is completely filled up
  size_t capacity = getInlineBufferCapacityFromExtraData(stat->extraData);
  if (capacity > 0 && inlineBufferSize != capacity)
  {
    stat->statBuff.st_size = inlineBufferSize;
  }
  else
  {
    FileIOSP fileIO = getOrCreateFileIO(stat->translatedPath, stat);
    stat->statBuff.st_size = fileIO->getSize();
  }

  bool notify = false;

  {
    boost::unique_lock<boost::mutex> lock(*mutex);

    notify = --*numJobs == 0;
  }

  if (notify)
    cond->notify_all();
}

void
FilesystemPriv::generalWorkerThread(
    boost::shared_ptr<boost::asio::io_service> ioService)
{
  ioService->run();
}

void
FilesystemPriv::statEntries(StatAsyncInfo *info,
                            std::map<std::string, std::string> &xattrs)
{
  Stat *stats = new Stat[info->entries->size()];
  int *rets = new int[info->entries->size()];
  boost::mutex mutex;
  boost::condition_variable cond;
  int numJobs = info->entries->size();

  for (size_t i = 0; i < info->entries->size(); i++)
  {
    const std::string &entryName = (*info->entries)[i];
    const std::string &path = info->stat.path + entryName;
    const std::string &xattr = xattrs[XATTR_FILE_PREFIX + entryName];

    if (xattr == "")
    {
      rets[i] = -ENOENT;
      mutex.lock();
      numJobs--;
      mutex.unlock();
      continue;
    }

    // Get the inline buffer's size as this might determine the file's size
    const std::string &inlineBuffer = xattrs[XATTR_FILE_INLINE_BUFFER +
                                             entryName];
    size_t inlineBufferSize = 0;
    if (inlineBuffer.length() > XATTR_FILE_INLINE_BUFFER_HEADER_SIZE)
    {
      inlineBufferSize = inlineBuffer.length() -
                         XATTR_FILE_INLINE_BUFFER_HEADER_SIZE;
    }

    ioService->post(boost::bind(&FilesystemPriv::statEntryInThread, this, path,
                                xattr, inlineBufferSize, &stats[i], &rets[i],
                                &mutex, &cond, &numJobs));
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
FilesystemPriv::statAsync(StatAsyncInfo *info)
{
  std::map<std::string, std::string> xattrs;
  u_int64_t size;
  time_t mtime;
  xattrs[XATTR_PERMISSIONS] = "";
  xattrs[XATTR_MTIME] = "";
  xattrs[XATTR_CTIME] = "";

  for (size_t i = 0; i < info->entries->size(); i++)
  {
    const std::string &entryName = (*info->entries)[i];
    const std::string &entry = XATTR_FILE_PREFIX + entryName;
    const std::string &inlineBuffer = XATTR_FILE_INLINE_BUFFER + entryName;
    xattrs[entry] = "";
    xattrs[inlineBuffer] = "";
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
FilesystemPriv::statAsyncInfoInThread(const std::string path,
                                      StatAsyncInfo *info, boost::mutex *mutex,
                                      boost::condition_variable *cond,
                                      int *numJobs)
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
FilesystemPriv::parallelStat(
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

    getIoService()->post(boost::bind(&FilesystemPriv::statAsyncInfoInThread,
                                     this, dir, info, &mutex, &cond, &numJobs));
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
FilesystemPriv::stat(const std::string &path, Stat *stat)
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
FilesystemPriv::statDir(PoolSP mtdPool, Stat *stat)
{
  Inode inode;
  int ret = getDirInode(stat->path, inode, mtdPool);

  if (ret == 0)
  {
    stat->pool = inode.pool;
    stat->translatedPath = inode.inode;

    ret = statDirObj(*stat);
  }

  return ret;
}

int
FilesystemPriv::statFile(PoolSP mtdPool, Stat *stat)
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
FilesystemPriv::createPrefixDir(PoolSP pool, const std::string &prefix)
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
    clock_gettime(CLOCK_REALTIME, &stat.statBuff.st_ctim);
    clock_gettime(CLOCK_REALTIME, &stat.statBuff.st_mtim);

    ret = createDirAndInode(&stat);
  }

  return ret;
}

int
FilesystemPriv::addPool(const std::string &name, const std::string &prefix,
                        PoolMap *map, boost::mutex &mutex, size_t size)
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

  boost::unique_lock<boost::mutex> lock(mutex);

  std::pair<std::string, PoolSP > entry(cleanPrefix, poolSP);
  map->insert(entry);

  return ret;
}

PoolSP
FilesystemPriv::getDataPool(const std::string &path, const std::string &poolName)
{
  PoolSP pool;
  size_t maxLength = 0;
  boost::unique_lock<boost::mutex> lock(poolMutex);

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

  return pool;
}

PoolSP
FilesystemPriv::getDataPoolFromName(const std::string &poolName)
{
  PoolSP pool;
  boost::unique_lock<boost::mutex> lock(poolMutex);

  PoolListMap::const_iterator it;
  for (it = poolMap.begin(); it != poolMap.end(); it++)
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
          break;
        }
      }
    }
    else
    {
      pool = pools.front();
      break;
    }
  }

  return pool;
}

PoolSP
FilesystemPriv::getMtdPoolFromName(const std::string &name)
{
  PoolSP pool;
  boost::unique_lock<boost::mutex> lock(poolMutex);

  PoolMap::const_iterator it;
  for (it = mtdPoolMap.begin(); it != mtdPoolMap.end(); it++)
  {
    PoolSP currentPool = (*it).second;

    if (currentPool->name == name)
    {
      pool = currentPool;
      break;
    }
  }

  return pool;
}

PoolSP
FilesystemPriv::getMetadataPoolFromPath(const std::string &path)
{
  return getPool(path, &mtdPoolMap, mtdPoolMutex);
}

PoolSP
FilesystemPriv::getPool(const std::string &path,
                        PoolMap *map,
                        boost::mutex &mutex)
{
  PoolSP pool;
  size_t maxLength = 0;
  boost::unique_lock<boost::mutex> lock(mutex);

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

  return pool;
}

std::string
FilesystemPriv::poolPrefix(const std::string &pool, PoolMap *map,
                           boost::mutex &mutex) const
{
  std::string prefix("");
  boost::unique_lock<boost::mutex> lock(mutex);

  PoolMap::iterator it;
  for (it = map->begin(); it != map->end(); it++)
  {
    if ((*it).second->name == pool)
    {
      prefix = (*it).first;
      break;
    }
  }

  return prefix;
}

int
FilesystemPriv::removePool(const std::string &name, PoolMap *map,
                           boost::mutex &mutex)
{
  int ret = -ENOENT;
  const std::string &prefix = poolPrefix(name, map, mutex);
  boost::unique_lock<boost::mutex> lock(mutex);

  if (map->count(prefix) > 0)
  {
    map->erase(prefix);
    ret = 0;
  }

  return ret;
}

std::string
FilesystemPriv::poolFromPrefix(const std::string &prefix, PoolMap *map,
                               boost::mutex &mutex) const
{
  std::string pool("");
  boost::unique_lock<boost::mutex> lock(mutex);

  if (map->count(prefix) > 0)
    pool = map->at(prefix)->name;

  return pool;
}

std::vector<std::string>
FilesystemPriv::pools(PoolMap *map,
                      boost::mutex &mutex) const
{
  boost::unique_lock<boost::mutex> lock(mutex);
  std::vector<std::string> pools;

  PoolMap::iterator it;
  for (it = map->begin(); it != map->end(); it++)
  {
    pools.push_back((*it).second->name);
  }

  return pools;
}

PoolList
FilesystemPriv::getDataPools(const std::string &path)
{
  size_t maxLength = 0;
  std::string prefixFound("");
  PoolList pools;
  boost::unique_lock<boost::mutex> lock(poolMutex);

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

  return pools;
}

PoolList
FilesystemPriv::getDataPools()
{
  PoolList pools;
  boost::unique_lock<boost::mutex> lock(poolMutex);

  PoolListMap::const_iterator it;
  for (it = poolMap.begin(); it != poolMap.end(); it++)
  {
    const PoolList &list = (*it).second;
    pools.insert(pools.end(), list.begin(), list.end());
  }

  return pools;
}

PoolList
FilesystemPriv::getMtdPools()
{
  PoolList pools;
  boost::unique_lock<boost::mutex> lock(mtdPoolMutex);

  PoolMap::const_iterator it;
  for (it = mtdPoolMap.begin(); it != mtdPoolMap.end(); it++)
  {
    pools.push_back((*it).second);
  }

  return pools;
}

const std::string
FilesystemPriv::getParentDir(const std::string &obj, int *pos)
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
FilesystemPriv::updateDirCache(std::tr1::shared_ptr<DirCache> &cache)
{
  boost::unique_lock<boost::mutex> lock(dirCacheMutex);
  dirCache.update(cache);
}

void
FilesystemPriv::removeDirCache(std::tr1::shared_ptr<DirCache> &cache)
{
  boost::unique_lock<boost::mutex> lock(dirCacheMutex);
  dirCache.removeCache(cache->inode());
}

std::tr1::shared_ptr<DirCache>
FilesystemPriv::getDirInfo(const std::string &inode, PoolSP pool,
                           bool addToCache)
{
  std::tr1::shared_ptr<DirCache> cache;
  boost::unique_lock<boost::mutex> lock(dirCacheMutex);

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

  return cache;
}

FileIOSP
FilesystemPriv::getFileIO(const std::string &path)
{
  FileIOSP io;
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  if (operations.count(path) != 0)
    io = operations[path];

  return io;
}

FileIOSP
FilesystemPriv::getOrCreateFileIO(const std::string &path, const Stat *stat)
{
  FileIOSP io = getFileIO(path);

  if (!io)
  {
    int chunkSize = 0;

    if (stat->extraData.count(XATTR_FILE_CHUNK_SIZE))
    {
      chunkSize = atol(stat->extraData.at(XATTR_FILE_CHUNK_SIZE).c_str());
    }

    if (chunkSize == 0)
      chunkSize = alignChunkSize(radosFs->fileChunkSize(),
                                   stat->pool->alignment);

    io = FileIOSP(new FileIO(radosFs, stat->pool, stat->translatedPath,
                             stat->path, chunkSize));

    setFileIO(io);
  }

  return io;
}

void
FilesystemPriv::setFileIO(FileIOSP sharedFileIO)
{
  boost::unique_lock<boost::mutex> lock(operationsMutex);
  operations[sharedFileIO->inode()] = sharedFileIO;
}

void
FilesystemPriv::removeFileIO(FileIOSP sharedFileIO)
{
  boost::unique_lock<boost::mutex> lock(operationsMutex);

  if (operations.count(sharedFileIO->inode()))
    operations.erase(sharedFileIO->inode());
}

void
FilesystemPriv::updateTMIdSync(std::string path)
{
  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_TMID].append(generateUuid());

  librados::bufferlist expectedValue;
  expectedValue.append(1);

  std::pair<librados::bufferlist, int> cmp(expectedValue,
                                           LIBRADOS_CMPXATTR_OP_EQ);

  std::map<std::string, std::pair<librados::bufferlist, int> > omapCmp;
  omapCmp[XATTR_USE_TMID] = cmp;

  std::string currentPath = path;
  while ((currentPath = getParentDir(currentPath, 0)) != "")
  {
    Inode inode;
    int ret = getDirInode(currentPath, inode);

    if (ret != 0)
    {
      radosfs_debug("Error getting dir's '%s' inode for setting TM id: "
                    "%s (retcode=%d)", currentPath.c_str(), strerror(abs(ret)),
                    ret);
      return;
    }

    librados::ObjectWriteOperation op;
    op.omap_cmp(omapCmp, 0);
    op.omap_set(omap);
    ret = inode.pool->ioctx.operate(inode.inode, &op);

    if (ret != 0 && ret != -ECANCELED)
    {
      radosfs_debug("Error setting TM id on %s (%s): %s (retcode=%d)",
                    currentPath.c_str(), inode.inode.c_str(),
                    strerror(abs(ret)), ret);
      return;
    }
  }
}

void
FilesystemPriv::updateTMId(Stat *stat)
{
  getIoService()->post(boost::bind(&FilesystemPriv::updateTMIdSync, this,
                                   stat->path));
}

void
FilesystemPriv::checkFileLocks(void)
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
        if (!io->hasRunningAsyncOps() && FileIO::hasSingleClient(io))
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

/**
 * @class Filesystem
 *
 * Represents the filesystem. Instantiating a Filesystem object is usually the
 * first step when using the library since it corresponds to a Ceph cluster.
 *
 * Before using any of the methods in the class, Filesystem::init should be
 * called in order to set the appropriate Ceph cluster. After the cluster is
 * initialized, at least one metadata and one data pool should be set using
 * Filesystem::addMetadataPool and Filesystem::addDataPool, respectively.
 */

/**
 * @struct FileReadData
 *
 * A struct describing how to read data from a file.
 *
 * @fn FileReadData::FileReadData(char *buff, off_t offset, size_t length,
 *                                ssize_t *retValue=0)
 * Builds an instance of FileReadData.
 *
 * @param[out] buff a pointer to a buffer where the data will be stored.
 * @param offset the offset in the file from which the data will start being
 *        read.
 * @param length the number of bytes to read. The given \a buff should be large
 *        enough to hold this number of bytes.
 * @param[out] retValue the location in which to return the return code of the
 *             read operation (or a null pointer in case this value should not
 *             be returned).
 */

/**
 * Creates a new instance of Filesystem.
 *
 * This is usually the first step to be done when using the library but keep in
 * mind that Filesystem::init should be called in order to do anything
 * meaningful (as otherwise no Ceph cluster will be configured).
 */
Filesystem::Filesystem()
  : mPriv(new FilesystemPriv(this))
{
}

Filesystem::~Filesystem()
{
  delete mPriv;
}

/**
 * Initialize the Ceph cluster.
 * @param userName the user name for for initializing the cluster.
 * @param configurationFile the path to the configuration file describing the
 *        cluster.
 * @return 0 if the cluster has been successfully initialized, an error
 *         code otherwise.
 */
int
Filesystem::init(const std::string &userName,
                 const std::string &configurationFile)
{
  int ret = mPriv->createCluster(userName, configurationFile);

  return ret;
}

/**
 * Sets a pool to be used for the data associated with the given \a prefix.
 * File objects whose path prefixes match the one set by this method will have
 * their data stored in the mapped pool.
 *
 * @note Keep in mind this method does not create the pool in the cluster so it
 *       needs to already exist beforehand.
 * @param name the name of the pool in the cluster.
 * @param prefix the prefix of the paths that is associated with this pool.
 * @param size the greatest size allowed for a file (given in MB).
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::addDataPool(const std::string &name, const std::string &prefix,
                        size_t size)
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

  boost::unique_lock<boost::mutex> lock(mPriv->poolMutex);

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
        return -EEXIST;
      }
    }
  }

  ret = mPriv->radosCluster.ioctx_create(name.c_str(), ioctx);

  if (ret != 0)
    return ret;

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

  return ret;
}

/**
 * Removes a data pool. @note As with Filesystem::addDataPool , this method does
 * not affect the cluster itself which means that the pool will not be deleted
 * from it but rather disassociated from the prefix to which it was previously
 * set.
 * @param name the name
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::removeDataPool(const std::string &name)
{
  int ret = -ENOENT;
  PoolListMap *map = &mPriv->poolMap;
  boost::unique_lock<boost::mutex> lock(mPriv->poolMutex);

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

  return ret;
}

/**
 * Gets all the data pools configured for the given \a prefix.
 * @param prefix the prefix for which the data pools are mapped.
 * @return A vector with the names of the pools in question.
 */
std::vector<std::string>
Filesystem::dataPools(const std::string &prefix) const
{
  std::vector<std::string> pools;
  const std::string &dirPrefix = getDirPath(prefix);
  boost::unique_lock<boost::mutex> lock(mPriv->poolMutex);

  if (mPriv->poolMap.count(dirPrefix) > 0)
  {
    const PoolList &poolList = mPriv->poolMap[dirPrefix];
    PoolList::const_iterator it;

    for (it = poolList.begin(); it != poolList.end(); it++)
    {
      pools.push_back((*it)->name);
    }
  }

  return pools;
}

/**
 * Gets the prefix associated with the given data pool.
 * @param pool a data pool's name associated with a prefix.
 * @return The prefix associated with the given data pool.
 */
std::string
Filesystem::dataPoolPrefix(const std::string &pool) const
{
  std::string prefix("");
  boost::unique_lock<boost::mutex> lock(mPriv->poolMutex);

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

  return prefix;
}

/**
 * Gets the maximum size of files for the given data pool.
 * @param pool the name of the data pool from which to get the size.
 * @return the maximum size of files for the given data pool.
 */
ssize_t
Filesystem::dataPoolSize(const std::string &pool) const
{
  ssize_t size = -ENOENT;
  boost::unique_lock<boost::mutex> lock(mPriv->poolMutex);

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

    if (size >= 0)
      break;
  }

  return size;
}

/**
 * Sets a pool to be used for the metadata associated with the given \a prefix.
 * Directory objects whose path prefixes match the one set by this method will
 * have their data stored in the mapped pool.
 *
 * @note Keep in mind this method does not create the pool in the cluster so it
 *       needs to already exist beforehand.
 * @param name the name of the pool in the cluster.
 * @param prefix the prefix of the paths that is associated with this pool.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::addMetadataPool(const std::string &name, const std::string &prefix)
{
  return mPriv->addPool(name,
                        prefix,
                        &mPriv->mtdPoolMap,
                        mPriv->mtdPoolMutex);
}

/**
 * Removes a metadata pool.
 *
 * @note As with Filesystem::addMetadataPool , this method does not affect the
 *       cluster itself which means that the pool will not be deleted from it
 *       but rather disassociated from the prefix to which it was previously set.
 * @param name the name of the metadata pool to remove.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::removeMetadataPool(const std::string &name)
{
  return mPriv->removePool(name, &mPriv->mtdPoolMap, mPriv->mtdPoolMutex);
}

/**
 * Gets all the metadata pools.
 * @return A vector with the names of the metadata pools.
 */
std::vector<std::string>
Filesystem::metadataPools() const
{
  return mPriv->pools(&mPriv->mtdPoolMap, mPriv->mtdPoolMutex);
}

/**
 * Gets the prefix associated with the given metadata pool.
 * @param pool a pool's name associated with a prefix.
 * @return The prefix associated with the given data pool.
 */
std::string
Filesystem::metadataPoolPrefix(const std::string &pool) const
{
  return mPriv->poolPrefix(pool, &mPriv->mtdPoolMap, mPriv->mtdPoolMutex);
}

/**
 * Gets the pool associated with the given \a prefix. (Does the opposite of
 * Filesystem::metadataPoolPrefix).
 * @param prefix a prefix associated with a metadata pool.
 * @return the name of the pool associated with the given
 */
std::string
Filesystem::metadataPoolFromPrefix(const std::string &prefix) const
{
  return mPriv->poolFromPrefix(prefix, &mPriv->mtdPoolMap, mPriv->mtdPoolMutex);
}

/**
 * Sets the \b uid and \b gid . The \b uid and \b gid are thread local and will
 * be used for any subsequent operations that require checking permissions.
 * @param uid the uid to be set.
 * @param gid the gid to be set.
 */
void
Filesystem::setIds(uid_t uid, gid_t gid)
{
  mPriv->setUid(uid);
  mPriv->setGid(gid);
}

/**
 * Gets the current \b uid and \b gid values and sets them in the given \a uid
 * and \a gid.
 * @param[out] uid the location in which the uid will be set.
 * @param[out] gid the location in which the gid will be set.
 */
void
Filesystem::getIds(uid_t *uid, gid_t *gid) const
{
  *uid = mPriv->getUid();
  *gid = mPriv->getGid();
}

/**
 * Gets the current \b uid value.
 * @return the \b uid currently set.
 */
uid_t
Filesystem::uid(void) const
{
  return mPriv->uid;
}

/**
 * Gets the current \b gid value.
 * @return the \b gid currently set.
 */
gid_t
Filesystem::gid(void) const
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

/**
 * Stats the given \a paths in parallel.
 *
 * Paths are statted in parallel not in to the worker threads that will
 * parallelize the operations from the client side but, in case they have the
 * same parent, they may be also statted in parallel on the cluster side. This
 * makes it ideal for e.g. statting all the entries in a directory as all the
 * files present in the directory will be statted in one go.
 *
 * @param paths a vector of paths to files or directories.
 * @return a vector of pairs with the result of the stat operation over the
 *         paths. Each pair contains the operation's return code (0 on success,
 *         an error code otherwise) and a \b stat struct with the details of the
 *         stat operation.
 */
std::vector<std::pair<int, struct stat> >
Filesystem::stat(const std::vector<std::string> &paths)
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

/**
 * Stats the given \a path and fills the details in the given \a stat parameter.
 * @param path the path to be statted.
 * @param[out] buff the struct to be filled with the stat details.
 * @return 0 if the path was successfully statted, an error code
 *         otherwise.
 */
int
Filesystem::stat(const std::string &path, struct stat *buff)
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

/**
 * Gets the list of all the pools in the configured cluster.
 * @return A vector with the name of all the pools in the configured cluster.
 */
std::vector<std::string>
Filesystem::allPoolsInCluster() const
{
  std::vector<std::string> poolVector;
  std::list<std::string> poolList;

  mPriv->radosCluster.pool_list(poolList);
  poolVector.insert(poolVector.begin(), poolList.begin(), poolList.end());

  return poolVector;
}

/**
 * Stats the currently configured cluster. The space related values are returned
 * in KB.
 * @param[out] totalSpaceKb the location in which to return the total space.
 * @param[out] usedSpaceKb the location in which to return the used space.
 * @param[out] availableSpaceKb the location in which to return the available
 *             space.
 * @param[out] numberOfObjects  the location in which to return the number of
 *             objects.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::statCluster(uint64_t *totalSpaceKb, uint64_t *usedSpaceKb,
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

/**
 * Sets an xattribute in the given \a path .
 * @param path a path to a file or directory.
 * @param attrName the name of the xattribute.
 * @param value the value of the xattribute.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::setXAttr(const std::string &path, const std::string &attrName,
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

  return setXAttrFromPath(stat, uid(), gid(), attrName, value);
}

/**
 * Gets an xattribute from the given \a path .
 * @param path a path to a file or directory.
 * @param attrName the name of the xattribute.
 * @param[out] value the variable to receive the value of the xattribute.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::getXAttr(const std::string &path, const std::string &attrName,
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

  return getXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                          stat.translatedPath, attrName, value);
}

/**
 * Removes an xattribute from a given \a path .
 * @param path a path to a file or directory.
 * @param attrName the name of the xattribute.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::removeXAttr(const std::string &path, const std::string &attrName)
{
  Stat stat;

  int ret = mPriv->stat(path, &stat);

  if (ret != 0)
    return ret;

  if (S_ISLNK(stat.statBuff.st_mode))
    return removeXAttr(stat.translatedPath, attrName);

  if (!stat.pool)
    return -ENODEV;

  return removeXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                             stat.translatedPath, attrName);
}

/**
 * Gets a map of all the xattributes set in the given \a path.
 * @param path a path to a file or directory.
 * @param[out] map the map into which the xattributes will be set.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::getXAttrsMap(const std::string &path,
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

  return getMapOfXAttrFromPath(stat.pool->ioctx, stat.statBuff, uid(), gid(),
                               stat.translatedPath, map);
}

/**
 * Sets the maximum size of the directory cache.
 * @param size the size to set.
 */
void
Filesystem::setDirCacheMaxSize(size_t size)
{
  mPriv->dirCache.maxCacheSize = size;
  mPriv->dirCache.adjustCache();
}

/**
 * Gets the maximum size of the directory cache.
 * @return the maximum size of the directory cache.
 */
size_t
Filesystem::dirCacheMaxSize(void) const
{
  return mPriv->dirCache.maxCacheSize;
}

/**
 * Sets the directory compaction ratio.
 * @param ratio the ratio to set.
 */
void
Filesystem::setDirCompactRatio(float ratio)
{
  mPriv->dirCompactRatio = ratio;
}

/**
 * Gets the directory compaction ratio.
 * @return the directory compaction ratio.
 */
float
Filesystem::dirCompactRatio(void) const
{
  return mPriv->dirCompactRatio;
}

/**
 * Sets the log level to be used.
 * @param level the new log level.
 */
void
Filesystem::setLogLevel(const Filesystem::LogLevel level)
{
  mPriv->logger.setLogLevel(level);
}

/**
 * @enum Filesystem::LogLevel
 *
 * The possible values for setting the log level.
 *
 * @var Filesystem::LogLevel Filesystem::LOG_LEVEL_NONE
 *      Turns off debug messages (nothing will be printed).
 *
 * @var Filesystem::LogLevel Filesystem::LOG_LEVEL_DEBUG
 *      Turns on debug messages (prints all debug messages).
 */

/**
 * Gets the log level in use.
 */
Filesystem::LogLevel
Filesystem::logLevel(void) const
{
  return mPriv->logger.logLevel();
}

/**
 * Sets the file stripe size to be used by default.
 * @param size the stripe size (in bytes).
 */
void
Filesystem::setFileChunkSize(const size_t size)
{
  size_t realSize = size;

  if (size == 0)
  {
    realSize = 1;
    radosfs_debug("Cannot set the file chunk size as 0. Setting to %d.",
                  realSize);
  }

  mPriv->fileChunkSize = realSize;
}

/**
 * Gets the default file stripe size.
 * @return the file stripe size.
 */
size_t
Filesystem::fileChunkSize(void) const
{
  return mPriv->fileChunkSize;
}

/**
 * Instantiates an FsObj (File or Dir) from the given \a path.
 * Using this method is convenient if one does not know whether the \a path
 * points to a file or directory.
 *
 * @note With the current implementation, it is slower to use this method than
 *       to instance a File or Dir object directly.
 * @param path a path to a file or directory.
 * @return an instance of the file/directory that the \a path represents or a
 *         null pointer if an object could not be instantiated (e.g. if the path
 *         does not exist).
 */
FsObj *
Filesystem::getFsObj(const std::string &path)
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

/**
 * Gets the inode and the pool associated with the given \a path.
 * @param path a path to a file or directory.
 * @param[out] inode the location into which the inode's name should be
 *             returned.
 * @param[out] pool  the location into which the pool's name should be returned.
 * @return 0 on success, an error code otherwise.
 */
int
Filesystem::getInodeAndPool(const std::string &path, std::string *inode,
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

/**
 * Sets the number of generic worker threads to be used.
 * @param numWorkers the number of worker threads (minimum is 1).
 * @note The minimum number of generic workers is 1. Setting a lower value will
 *       instead set the minimum value.
 */
void
Filesystem::setNumGenericWorkers(size_t numWorkers)
{
  if (numWorkers < MIN_NUM_WORKER_THREADS)
  {
    radosfs_debug("Error: Cannot set the number of generic worker threads to 0. "
                  "Setting the number to the minimum allowed instead (minimum "
                  "is 1).");
    numWorkers = MIN_NUM_WORKER_THREADS;
  }

  {
    boost::unique_lock<boost::mutex> lock(mPriv->genericWorkersMutex);
    mPriv->numGenericWorkers = numWorkers;
  }

  size_t currentNumWorkers = mPriv->generalWorkerThreads.size();

  if (currentNumWorkers > 0 && currentNumWorkers != numWorkers)
    mPriv->launchThreads();
}

/**
 * Returns the number of generic worker threads to be used.
 * @return the number of generic worker threads to be used.
 * @note This does not indicate the number of worker threads in use currently
 * as they might have not been launched yet.
 */
size_t
Filesystem::numGenericWorkers(void)
{
  boost::unique_lock<boost::mutex> lock(mPriv->genericWorkersMutex);
  return mPriv->numGenericWorkers;
}

RADOS_FS_END_NAMESPACE
