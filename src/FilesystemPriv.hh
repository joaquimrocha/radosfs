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

#ifndef __RADOS_FS_FILESYSTEM_PRIV_HH__
#define __RADOS_FS_FILESYSTEM_PRIV_HH__

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <map>
#include <vector>
#include <set>
#include <rados/librados.hpp>
#include <string>
#include <tr1/memory>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "DirCache.hh"
#include "FileIO.hh"
#include "Logger.hh"
#include "Finder.hh"

RADOS_FS_BEGIN_NAMESPACE

class Filesystem;

typedef std::vector<PoolSP> PoolList;

typedef std::map<std::string, PoolSP> PoolMap;
typedef std::map<std::string, PoolList>  PoolListMap;

typedef struct _LinkedList LinkedList;

struct _LinkedList
{
  std::tr1::shared_ptr<DirCache> cachePtr;
  size_t lastNumEntries;
  LinkedList *previous;
  LinkedList *next;
};

typedef struct {
  Stat stat;
  std::map<std::string, std::pair<int, struct stat> > entryStats;
  const std::vector<std::string> *entries;
  uint64_t psize;
  time_t pmtime;
  int statRet;
} StatAsyncInfo;

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

class FilesystemPriv
{
public:
  FilesystemPriv(Filesystem *radosFs);
  ~FilesystemPriv();

  int createCluster(const std::string &userName,
                    const std::string &confFile);

  void setUid(const uid_t uid) { this->uid = uid; }
  void setGid(const gid_t gid) { this->gid = gid; }

  uid_t getUid(void) const { return uid; }
  gid_t getGid(void) const { return gid; }

  int addPool(const std::string &name,
              const std::string &prefix,
              PoolMap *map,
              boost::mutex &mutex,
              size_t size = 0);

  int createPrefixDir(PoolSP pool, const std::string &prefix);

  PoolSP getPool(const std::string &path, PoolMap *map, boost::mutex &mutex);

  PoolSP getMetadataPoolFromPath(const std::string &path);

  PoolSP getMtdPoolFromName(const std::string &name);

  PoolSP getDataPool(const std::string &path,
                     const std::string &poolName = "");

  PoolSP getDataPoolFromName(const std::string &poolName);

  PoolList getDataPools(const std::string &path);

  std::string poolPrefix(const std::string &pool,
                         PoolMap *map,
                         boost::mutex &mutex) const;

  int removePool(const std::string &name,
                 PoolMap *map,
                 boost::mutex &mutex);

  std::string poolFromPrefix(const std::string &prefix,
                             PoolMap *map,
                             boost::mutex &mutex) const;

  std::vector<std::string> pools(PoolMap *map, boost::mutex &mutex) const;

  const std::string getParentDir(const std::string &obj, int *pos);

  std::tr1::shared_ptr<DirCache> getDirInfo(const std::string &inode,
                                            PoolSP pool,
                                            bool addToCache = true);

  FileIOSP getFileIO(const std::string &path);

  FileIOSP getOrCreateFileIO(const std::string &path, const Stat *stat);

  void setFileIO(FileIOSP sharedFileIO);
  void removeFileIO(FileIOSP sharedFileIO);

  void updateDirCache(std::tr1::shared_ptr<DirCache> &cache);

  void removeDirCache(std::tr1::shared_ptr<DirCache> &cache);

  int stat(const std::string &path, Stat *stat);

  void parallelStat(
      const std::map<std::string, std::vector<std::string> > &paths,
      std::map<std::string, std::pair<int, struct stat> > *stats);

  void statAsync(StatAsyncInfo *info);

  void statEntries(StatAsyncInfo *info,
                   std::map<std::string, std::string> &xattrs);

  int statLink(PoolSP mtdPool,
               Stat *stat,
               std::string &pool);

  int statFile(PoolSP mtdPool, Stat *stat);

  int statDir(PoolSP mtdPool, Stat *stat);

  int getRealPath(const std::string &path, Stat *stat,
                  std::string &realPath);

  void updateDirInode(const std::string &oldPath, const std::string &newPath);

  int getDirInode(const std::string &path, Inode &inode,
                  PoolSP &pool);

  void setDirInode(const std::string &path, const Inode &inode);

  void removeDirInode(const std::string &path);

  std::vector<Stat> getParentsForTMTimeUpdate(const std::string &path);

  void updateTMTime(Stat *stat, timespec *spec = 0);

  void updateDirTimes(Stat *stat, timespec *spec = 0);

  void statEntryInThread(std::string path, std::string entry,
                         size_t inlineBufferSize, Stat *stat, int *ret,
                         boost::mutex *mutex, boost::condition_variable *cond,
                         int *numJobs);

  int statAsyncInfoInThread(const std::string path, StatAsyncInfo *info,
                            boost::mutex *mutex, boost::condition_variable *cond,
                            int *numJobs);

  void generalWorkerThread(boost::shared_ptr<boost::asio::io_service> ioService);

  void launchThreads(void);

  void checkFileLocks(void);

  boost::shared_ptr<boost::asio::io_service> getIoService();

  Filesystem *radosFs;
  librados::Rados radosCluster;
  bool initialized;
  static __thread uid_t uid;
  static __thread gid_t gid;
  std::vector<rados_completion_t> completionList;
  PoolListMap poolMap;
  boost::mutex poolMutex;
  PoolMap mtdPoolMap;
  boost::mutex mtdPoolMutex;
  PriorityCache dirCache;
  boost::mutex dirCacheMutex;
  std::map<std::string, std::tr1::shared_ptr<FileIO> > operations;
  boost::mutex operationsMutex;
  std::map<std::string, Inode> dirPathInodeMap;
  boost::mutex dirPathInodeMutex;
  float dirCompactRatio;
  Logger logger;
  size_t fileStripeSize;
  bool lockFiles;
  boost::shared_ptr<boost::asio::io_service> ioService;
  boost::shared_ptr<boost::asio::io_service::work> asyncWork;
  boost::thread_group generalWorkerThreads;
  boost::thread fileOpsIdleChecker;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_FS_FILESYSTEM_PRIV_HH__ */
