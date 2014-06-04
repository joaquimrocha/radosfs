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

#include <rados/librados.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include "RadosFs.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

__thread uid_t RadosFsPriv::uid;
__thread gid_t RadosFsPriv::gid;

RadosFsPriv::RadosFsPriv(RadosFs *radosFs)
  : radosCluster(0),
    dirCompactRatio(DEFAULT_DIR_COMPACT_RATIO),
    finder(radosFs)
{
  uid = 0;
  gid = 0;
  dirCache.maxCacheSize = DEFAULT_DIR_CACHE_MAX_SIZE;

  pthread_mutex_init(&poolMutex, 0);
  pthread_mutex_init(&mtdPoolMutex, 0);
  pthread_mutex_init(&dirCacheMutex, 0);
  pthread_mutex_init(&operationsMutex, 0);
}

RadosFsPriv::~RadosFsPriv()
{
  std::map<std::string, RadosFsPool>::iterator it;

  for (it = poolMap.begin(); it != poolMap.end(); it++)
    rados_ioctx_destroy((*it).second.ioctx);

  if (radosCluster)
    rados_shutdown(radosCluster);

  pthread_mutex_lock(&dirCacheMutex);

  dirCache.cleanCache();

  pthread_mutex_unlock(&dirCacheMutex);

  pthread_mutex_destroy(&poolMutex);
  pthread_mutex_destroy(&dirCacheMutex);
  pthread_mutex_destroy(&operationsMutex);
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
    cacheMap.erase(link->cachePtr->path());
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

  if (cacheMap.count(cache->path()) == 0)
  {
    list = new LinkedList;
    list->cachePtr = cache;
    list->next = 0;
    list->previous = 0;
    list->lastNumEntries = 0;

    cacheMap[cache->path()] = list;
  }
  else
  {
    list = cacheMap[cache->path()];
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
RadosFsPriv::indexObject(rados_ioctx_t &ioctx,
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
  contents += INDEX_NAME_KEY "=\"" + escapeObjName(baseName) + "\" ";
  contents += "\n";

  return rados_append(ioctx, dirName.c_str(),
                      contents.c_str(), strlen(contents.c_str()));
}

int
RadosFsPriv::getIoctxFromPath(const std::string &objectName,
                              rados_ioctx_t *ioctx)
{
  const RadosFsPool *pool = getDataPoolFromPath(objectName);

  if (!pool)
    return -ENODEV;

  *ioctx = pool->ioctx;

  return 0;
}

int
RadosFsPriv::createRootIfNeeded(const RadosFsPool &pool)
{
  int ret = 0;

  const char root[] = {PATH_SEP, 0};

  if (rados_stat(pool.ioctx, root, 0, 0) != 0)
  {
    int nBytes = rados_write(pool.ioctx, root, 0, 0, 0);
    if (nBytes < 0)
      ret = nBytes;
  }

  return ret;
}

int
RadosFsPriv::addPool(const std::string &name,
                     const std::string &prefix,
                     std::map<std::string, RadosFsPool> *map,
                     pthread_mutex_t *mutex,
                     int size)
{
  int ret = -EPERM;

  if (radosCluster == 0)
    return ret;

  if (name == "")
  {
    radosfs_debug("The pool's name cannot be an empty string");
    return -EINVAL;
  }

  if (prefix == "")
  {
    radosfs_debug("The pool's prefix cannot be an empty string");
    return -EINVAL;
  }

  if (map->count(prefix) > 0)
  {
    radosfs_debug("There is already a pool with the prefix %s. "
                  "Not adding.", prefix.c_str());
    return -EEXIST;
  }

  rados_ioctx_t ioctx;

  ret = rados_ioctx_create(radosCluster, name.c_str(), &ioctx);

  if (ret != 0)
    return ret;

  RadosFsPool pool = {name.c_str(), size * MEGABYTE_CONVERSION, ioctx};

  if (size == 0)
  {
    ret = createRootIfNeeded(pool);

    if (ret < 0)
      return ret;
  }

  pthread_mutex_lock(mutex);

  const std::pair<std::string, RadosFsPool> entry(prefix.c_str(), pool);
  map->insert(entry);

  pthread_mutex_unlock(mutex);

  return ret;
}

const RadosFsPool *
RadosFsPriv::getDataPoolFromPath(const std::string &path)
{
  RadosFsPool *pool = 0;

  size_t maxLength = 0;
  pthread_mutex_lock(&poolMutex);

  std::map<std::string, RadosFsPool>::const_iterator it;
  for (it = poolMap.begin(); it != poolMap.end(); it++)
  {
    const std::string &prefix = (*it).first;
    const size_t prefixLength = prefix.length();

    if (prefixLength < maxLength)
      continue;

    if (path.compare(0, prefixLength, prefix) == 0)
    {
      pool = &poolMap[prefix];
      maxLength = prefixLength;
      break;
    }
  }

  pthread_mutex_unlock(&poolMutex);

  return pool;
}

int
RadosFsPriv::checkIfPathExists(rados_ioctx_t &ioctx,
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
    return 0;
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

    return 0;
  }

  return -1;
}

const std::string
RadosFsPriv::getParentDir(const std::string &obj, int *pos)
{
  int length = obj.length();
  int index = obj.rfind(PATH_SEP, length - 2);

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

  dirCache.removeCache(cache->path());

  pthread_mutex_unlock(&dirCacheMutex);
}

std::tr1::shared_ptr<DirCache>
RadosFsPriv::getDirInfo(const char *path, bool addToCache)
{
  std::tr1::shared_ptr<DirCache> cache;

  pthread_mutex_lock(&dirCacheMutex);

  if (dirCache.cacheMap.count(path) == 0)
  {
    rados_ioctx_t ioctx;
    int ret = getIoctxFromPath(path, &ioctx);

    if (ret != 0)
      return cache;

    DirCache *dirInfo = new DirCache(path, ioctx);
    cache = std::tr1::shared_ptr<DirCache>(dirInfo);

    if (addToCache)
      dirCache.update(cache);
  }
  else
    cache = dirCache.cacheMap[path]->cachePtr;

  pthread_mutex_unlock(&dirCacheMutex);

  return cache;
}

std::tr1::shared_ptr<RadosFsIO>
RadosFsPriv::getRadosFsIO(const std::string &path)
{
  std::tr1::shared_ptr<RadosFsIO> fsIO;

  pthread_mutex_lock(&operationsMutex);

  if (operations.count(path) != 0)
    fsIO = operations[path].lock();

  pthread_mutex_unlock(&operationsMutex);

  return fsIO;
}

void
RadosFsPriv::setRadosFsIO(std::tr1::shared_ptr<RadosFsIO> sharedFsIO)
{
  pthread_mutex_lock(&operationsMutex);

  operations[sharedFsIO->path()] = sharedFsIO;

  pthread_mutex_unlock(&operationsMutex);
}

void
RadosFsPriv::removeRadosFsIO(std::tr1::shared_ptr<RadosFsIO> sharedFsIO)
{
  pthread_mutex_lock(&operationsMutex);

  if (operations.count(sharedFsIO->path()))
    operations.erase(sharedFsIO->path());

  pthread_mutex_unlock(&operationsMutex);
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
RadosFs::addPool(const std::string &name,
                 const std::string &prefix,
                 int size)
{
  return mPriv->addPool(name, prefix, &mPriv->poolMap, &mPriv->poolMutex, size);
}

int
RadosFs::removePool(const std::string &name)
{
  int ret = -ENOENT;
  const std::string &prefix = poolPrefix(name);

  pthread_mutex_lock(&mPriv->poolMutex);

  if (mPriv->poolMap.count(prefix) > 0)
  {
    RadosFsPool pool = mPriv->poolMap[prefix];
    rados_ioctx_destroy(pool.ioctx);
    mPriv->poolMap.erase(prefix);
    ret = 0;
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return ret;
}

std::vector<std::string>
RadosFs::pools() const
{
  pthread_mutex_lock(&mPriv->poolMutex);

  std::vector<std::string> pools;

  std::map<std::string, RadosFsPool>::iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    pools.push_back((*it).second.name);
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return pools;
}

std::string
RadosFs::poolPrefix(const std::string &pool) const
{
  std::string prefix("");

  pthread_mutex_lock(&mPriv->poolMutex);

  std::map<std::string, RadosFsPool>::iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    if ((*it).second.name == pool)
    {
      prefix = (*it).first;
      break;
    }
  }

  pthread_mutex_unlock(&mPriv->poolMutex);

  return prefix;
}

std::string
RadosFs::poolFromPrefix(const std::string &prefix) const
{
  std::string pool("");

  pthread_mutex_lock(&mPriv->poolMutex);

  if (mPriv->poolMap.count(prefix) > 0)
    pool = mPriv->poolMap[prefix].name;

  pthread_mutex_unlock(&mPriv->poolMutex);

  return pool;
}

int
RadosFs::poolSize(const std::string &pool) const
{
  int size = 0;

  pthread_mutex_lock(&mPriv->poolMutex);

  std::map<std::string, RadosFsPool>::iterator it;
  for (it = mPriv->poolMap.begin(); it != mPriv->poolMap.end(); it++)
  {
    if ((*it).second.name == pool)
    {
      size = (*it).second.size;
      break;
    }
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

int
RadosFs::stat(const std::string &path, struct stat *buff)
{
  rados_ioctx_t ioctx;

  int ret = mPriv->getIoctxFromPath(path, &ioctx);

  if (ret != 0)
    return ret;

  return genericStat(ioctx, path.c_str(), buff);
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
  rados_ioctx_t ioctx;

  int ret = mPriv->getIoctxFromPath(path, &ioctx);

  if (ret != 0)
    return ret;

  const std::string &realPath = getRealPath(ioctx, path);

  if (realPath == "")
    return -ENOENT;

  struct stat buff;
  ret = genericStat(ioctx, realPath.c_str(), &buff);

  if (ret != 0)
    return ret;

  return setXAttrFromPath(ioctx, buff, uid(), gid(), realPath, attrName, value);
}

int
RadosFs::getXAttr(const std::string &path,
                  const std::string &attrName,
                  std::string &value,
                  size_t length)
{
  rados_ioctx_t ioctx;

  int ret = mPriv->getIoctxFromPath(path, &ioctx);

  if (ret != 0)
    return ret;

  const std::string &realPath = getRealPath(ioctx, path);

  if (realPath == "")
    return -ENOENT;

  struct stat buff;
  ret = genericStat(ioctx, realPath.c_str(), &buff);

  if (ret != 0)
    return ret;

  return getXAttrFromPath(ioctx, buff, uid(), gid(),
                          realPath, attrName, value, length);
}

int
RadosFs::removeXAttr(const std::string &path,
                     const std::string &attrName)
{
  rados_ioctx_t ioctx;

  int ret = mPriv->getIoctxFromPath(path, &ioctx);

  if (ret != 0)
    return ret;

  const std::string &realPath = getRealPath(ioctx, path);

  if (realPath == "")
    return -ENOENT;

  struct stat buff;
  ret = genericStat(ioctx, realPath.c_str(), &buff);

  if (ret != 0)
    return ret;

  return removeXAttrFromPath(ioctx, buff, uid(), gid(), realPath, attrName);
}

int
RadosFs::getXAttrsMap(const std::string &path,
                      std::map<std::string, std::string> &map)
{
  rados_ioctx_t ioctx;

  int ret = mPriv->getIoctxFromPath(path, &ioctx);

  if (ret != 0)
    return ret;

  const std::string &realPath = getRealPath(ioctx, path);

  if (realPath == "")
    return -ENOENT;

  struct stat buff;
  ret = genericStat(ioctx, realPath.c_str(), &buff);

  if (ret != 0)
    return ret;

  return getMapOfXAttrFromPath(ioctx, buff, uid(), gid(), realPath, map);
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
  mPriv->logger.level = level;
}

RadosFs::LogLevel
RadosFs::logLevel(void) const
{
  return mPriv->logger.level;
}

RADOS_FS_END_NAMESPACE
