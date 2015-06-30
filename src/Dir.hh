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

#ifndef RADOS_FS_DIR_HH
#define RADOS_FS_DIR_HH

#include <cstdlib>
#include <set>

#include "Filesystem.hh"
#include "Quota.hh"
#include "FsObj.hh"

RADOS_FS_BEGIN_NAMESPACE

class DirPriv;

class Dir : public FsObj
{
public:
  Dir(Filesystem *radosFs, const std::string &path);

  Dir(Filesystem *radosFs, const std::string &path, bool cacheable);

  Dir(const Dir &otherDir);

  virtual ~Dir();

  Dir & operator=(const Dir &otherDir);

  static std::string getParent(const std::string &path, int *pos=0);

  int remove(void);

  int create(int mode = -1,
             bool mkPath = false,
             int ownerUid = -1,
             int ownerGid = -1);

  int entryList(std::set<std::string> &entries, bool withAbsolutePath=false);

  void refresh(void);

  int entry(int entryIndex, std::string &path);

  void setPath(const std::string &path);

  bool isWritable(void);

  bool isReadable(void);

  int stat(struct stat *buff);

  int compact(void);

  int setMetadata(const std::string &entry,
                  const std::string &key,
                  const std::string &value);

  int getMetadata(const std::string &entry,
                  const std::string &key,
                  std::string &value);

  int getMetadataMap(const std::string &entry,
                     std::map<std::string, std::string> &mtdMap);

  int removeMetadata(const std::string &entry, const std::string &key);

  int find(const std::string args, std::set<std::string> &results);

  int chmod(long int permissions);

  int chown(uid_t uid, gid_t gid);

  int setUid(uid_t uid);

  int setGid(gid_t gid);

  int rename(const std::string &newName);

  int useTMId(bool useTMId);

  bool usingTMId(void);

  int getTMId(std::string &id);

  int addToQuota(const Quota &quota, bool applyRecursively=false);

  int removeFromQuota(const Quota &quota, bool applyRecursively=false);

  int getQuotas(std::vector<Quota> &quota) const;

  bool hasQuota(void) const;

private:
  DirPriv *mPriv;

  friend class ::RadosFsTest;
  friend class DirPriv;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_DIR_HH */
