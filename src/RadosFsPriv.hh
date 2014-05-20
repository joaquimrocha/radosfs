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

#ifndef __RADOS_IMPL_HH__
#define __RADOS_IMPL_HH__

#include <pthread.h>
#include <map>
#include <vector>
#include <set>
#include <string>
#include <tr1/memory>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "DirCache.hh"
#include "RadosFsIO.hh"
#include "RadosFsLogger.hh"
#include "RadosFsFinder.hh"

RADOS_FS_BEGIN_NAMESPACE

class RadosFs;

typedef struct _LinkedList LinkedList;

struct _LinkedList
{
  std::tr1::shared_ptr<DirCache> cachePtr;
  int lastNumEntries;
  LinkedList *previous;
  LinkedList *next;
};

class PriorityCache
{
public:
  PriorityCache();
  ~PriorityCache();

  void update(std::tr1::shared_ptr<DirCache> cache);

  void adjustCache(void);

  size_t size(void) { return cacheSize + cacheMap.size(); }

  void cleanCache();

  void moveToFront(LinkedList *link);

  void removeCache(LinkedList *link, bool freeMemory = true);

  void removeCache(const std::string &path, bool freeMemory = true);

  std::map<std::string, LinkedList *> cacheMap;
  LinkedList *head;
  LinkedList *tail;
  size_t cacheSize;
  size_t maxCacheSize;
};

class RadosFsPriv
{
public:
  RadosFsPriv(RadosFs *radosFs);
  ~RadosFsPriv();

  int createCluster(const std::string &userName,
                    const std::string &confFile);

  void setUid(const uid_t uid) { this->uid = uid; }
  void setGid(const gid_t gid) { this->gid = gid; }

  uid_t getUid(void) const { return uid; }
  gid_t getGid(void) const { return gid; }

  int addPool(const std::string &name,
              const std::string &prefix,
              std::map<std::string, RadosFsPool> *map,
              pthread_mutex_t *mutex,
              int size = 0);

  int createPrefixDir(const RadosFsPool &pool, const std::string &prefix);

  const RadosFsPool * getPool(const std::string &path,
                              std::map<std::string, RadosFsPool> *map,
                              pthread_mutex_t *mutex);

  const RadosFsPool * getMetadataPoolFromPath(const std::string &path);

  const RadosFsPool * getDataPoolFromPath(const std::string &path);

  std::string poolPrefix(const std::string &pool,
                         std::map<std::string, RadosFsPool> *map,
                         pthread_mutex_t *mutex) const;

  int removePool(const std::string &name,
                 std::map<std::string, RadosFsPool> *map,
                 pthread_mutex_t *mutex);

  std::string poolFromPrefix(const std::string &prefix,
                             std::map<std::string, RadosFsPool> *map,
                             pthread_mutex_t *mutex) const;

  std::vector<std::string> pools(std::map<std::string, RadosFsPool> *map,
                                 pthread_mutex_t *mutex) const;

  const std::string getParentDir(const std::string &obj, int *pos);

  std::tr1::shared_ptr<DirCache> getDirInfo(const char *path,
                                            bool addToCache = true);

  std::tr1::shared_ptr<RadosFsIO> getRadosFsIO(const std::string &path);

  void setRadosFsIO(std::tr1::shared_ptr<RadosFsIO> sharedFsIO);
  void removeRadosFsIO(std::tr1::shared_ptr<RadosFsIO> sharedFsIO);

  void updateDirCache(std::tr1::shared_ptr<DirCache> &cache);

  void removeDirCache(std::tr1::shared_ptr<DirCache> &cache);

  int stat(const std::string &path,
           RadosFsStat *stat);

  int statLink(rados_ioctx_t dataIoctx,
               rados_ioctx_t mtdIoctx,
               RadosFsStat *stat);

  rados_t radosCluster;
  static __thread uid_t uid;
  static __thread gid_t gid;
  std::vector<rados_completion_t> completionList;
  std::map<std::string, RadosFsPool> poolMap;
  pthread_mutex_t poolMutex;
  std::map<std::string, RadosFsPool> mtdPoolMap;
  pthread_mutex_t mtdPoolMutex;
  PriorityCache dirCache;
  pthread_mutex_t dirCacheMutex;
  std::map<std::string, std::tr1::weak_ptr<RadosFsIO> > operations;
  pthread_mutex_t operationsMutex;
  float dirCompactRatio;
  RadosFsLogger logger;
  RadosFsFinder finder;
  size_t fileStripeSize;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_IMPL_HH__ */
