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

#include "RadosFs.hh"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

__thread uid_t RadosFsPriv::uid;
__thread gid_t RadosFsPriv::gid;

RadosFsPriv::RadosFsPriv(RadosFs *radosFs)
  : radosCluster(0)
{
  uid = 0;
  gid = 0;

  pthread_mutex_init(&poolMutex, 0);
  pthread_mutex_init(&dirCacheMutex, 0);
  pthread_mutex_init(&operationsMutex, 0);
}

RadosFsPriv::~RadosFsPriv()
{
  if (radosCluster)
    rados_shutdown(radosCluster);

  pthread_mutex_destroy(&poolMutex);
  pthread_mutex_destroy(&dirCacheMutex);
  pthread_mutex_destroy(&operationsMutex);
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
  contents += INDEX_NAME_KEY "\"" + escapeObjName(baseName) + "\" ";
  contents += "\n";

  return rados_append(ioctx, dirName.c_str(),
                      contents.c_str(), strlen(contents.c_str()));
}

int
RadosFsPriv::getIoctxFromPath(const std::string &objectName,
                              rados_ioctx_t *ioctx)
{
  const RadosFsPool *pool = getPoolFromPath(objectName);

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
                     int size)
{
  int ret = -1;

  if (radosCluster == 0)
    return ret;

  if (name == "")
    return -1;

  if (prefix == "")
    return -1;

  rados_ioctx_t ioctx;

  ret = rados_ioctx_create(radosCluster, name.c_str(), &ioctx);

  if (ret != 0)
    return ret;

  RadosFsPool pool = {name.c_str(), size * MEGABYTE_CONVERSION, ioctx};

  ret = createRootIfNeeded(pool);

  if (ret < 0)
    return ret;

  pthread_mutex_lock(&poolMutex);

  poolMap[prefix.c_str()] = pool;

  // We keep a set to quickly look for the prefix though
  // in the future we could implement a trie for improved efficiency
  poolPrefixSet.insert(prefix.c_str());

  pthread_mutex_unlock(&poolMutex);

  return ret;
}

const RadosFsPool *
RadosFsPriv::getPoolFromPath(const std::string &path)
{
  RadosFsPool *pool = 0;

  pthread_mutex_lock(&poolMutex);

  std::set<std::string>::reverse_iterator it;
  for (it = poolPrefixSet.rbegin(); it != poolPrefixSet.rend(); it++)
  {
    if (path.compare(0, (*it).length(), *it) == 0)
    {
      pool = &poolMap[*it];
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

std::tr1::shared_ptr<DirCache>
RadosFsPriv::getDirInfo(const char *path)
{
  std::tr1::shared_ptr<DirCache> cache;

  pthread_mutex_lock(&dirCacheMutex);

  if (dirCache.count(path) == 0)
  {
    rados_ioctx_t ioctx;
    int ret = getIoctxFromPath(path, &ioctx);

    if (ret == 0)
    {
      DirCache *dirInfo = new DirCache(path, ioctx);
      dirCache[path] = std::tr1::shared_ptr<DirCache>(dirInfo);
    }
  }

  cache = dirCache[path];

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
  return mPriv->addPool(name, prefix, size);
}

int
RadosFs::removePool(const std::string &name)
{
  int ret = -ENOENT;
  const std::string &prefix = poolPrefix(name);

  pthread_mutex_lock(&mPriv->poolMutex);

  if (mPriv->poolMap.count(prefix) > 0)
  {
    mPriv->poolMap.erase(prefix);
    mPriv->poolPrefixSet.erase(prefix);
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

RADOS_FS_END_NAMESPACE
