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

#ifndef RADOS_FS_DIR_HH
#define RADOS_FS_DIR_HH

#include <cstdlib>
#include <set>

#include "RadosFs.hh"
#include "RadosFsInfo.hh"

RADOS_FS_BEGIN_NAMESPACE

class DirPriv;

class Dir : public FsObj
{
public:
  Dir(Fs *radosFs, const std::string &path);

  Dir(Fs *radosFs, const std::string &path, bool cacheable);

  Dir(const Dir &otherDir);

  Dir(const Dir *otherDir);

  virtual ~Dir();

  Dir & operator=(const Dir &otherDir);

  static std::string getParent(const std::string &path, int *pos=0);

  int remove(void);

  int create(int mode = -1,
             bool mkPath = false,
             int ownerUid = -1,
             int ownerGid = -1);

  int entryList(std::set<std::string> &entries);

  void update(void);

  int entry(int entryIndex, std::string &path);

  void setPath(const std::string &path);

  bool isWritable(void);

  bool isReadable(void);

  int stat(struct stat *buff);

  int stat(struct stat *buff, timespec *tmtime);

  int compact(void);

  int setMetadata(const std::string &entry,
                  const std::string &key,
                  const std::string &value);

  int getMetadata(const std::string &entry,
                  const std::string &key,
                  std::string &value);

  int removeMetadata(const std::string &entry, const std::string &key);

  int find(std::set<std::string> &results, const std::string args);

  int chmod(long int permissions);

  int rename(const std::string &newName);

  int useTMTime(bool useTMTime);

  bool usingTMTime(void);

private:
  std::auto_ptr<DirPriv> mPriv;

  friend class ::RadosFsTest;
  friend class DirPriv;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_DIR_HH */
