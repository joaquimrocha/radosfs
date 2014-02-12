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

#ifndef __RADOS_FS_HH__
#define __RADOS_FS_HH__

#include <string>
#include <vector>

#define RADOS_FS_BEGIN_NAMESPACE namespace radosfs {
#define RADOS_FS_END_NAMESPACE }

RADOS_FS_BEGIN_NAMESPACE

class RadosFsPriv;
class RadosFsFilePriv;
class RadosFsDirPriv;

class RadosFs
{
public:
  RadosFs();
  ~RadosFs();

  int init(const std::string &userName = "",
           const std::string &configurationFile = "");

  int addPool(const std::string &name, const std::string &prefix, int size = 0);

  int removePool(const std::string &name);

  std::vector<std::string> pools() const;

  std::string poolPrefix(const std::string &pool) const;

  void setIds(uid_t uid, gid_t gid);

  void getIds(uid_t *uid, gid_t *gid) const;

  uid_t uid(void) const;

  uid_t gid(void) const;

  int stat(const std::string &path, struct stat *buff);

  std::vector<std::string> allPoolsInCluster(void) const;

private:
  RadosFsPriv *mPriv;

friend class RadosFsInfoPriv;
friend class RadosFsFilePriv;
friend class RadosFsDirPriv;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_FS_HH__ */
