/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2015 CERN, Switzerland
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

#ifndef __RADOS_FS_QUOTA_PRIV_HH__
#define __RADOS_FS_QUOTA_PRIV_HH__

#include "Dir.hh"

RADOS_FS_BEGIN_NAMESPACE

class QuotaPriv
{
public:
  QuotaPriv(Filesystem *fs, const std::string &pool, const std::string &name);

  ~QuotaPriv(void);

  int updateQuota(const std::string &key, int64_t diff);

  int create(int64_t maxSize);

  int remove(void);

  int load(void);

  Filesystem *fs;
  PoolSP pool;
  std::string name;
  QuotaSize quotaSize;
  std::map<uid_t, QuotaSize> users;
  std::map<gid_t, QuotaSize> groups;
  bool exists;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_QUOTA_PRIV_HH__

