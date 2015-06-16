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

#ifndef __RADOS_FS_QUOTA_HH__
#define __RADOS_FS_QUOTA_HH__

#include <boost/scoped_ptr.hpp>
#include <map>

#include "Filesystem.hh"
#include "radosfscommon.h"
#include "radosfsdefines.h"

RADOS_FS_BEGIN_NAMESPACE

class QuotaPriv;
class DirPriv;
class FilePriv;

struct QuotaSize
{
  int64_t max;
  int64_t current;

  QuotaSize(void)
    : max(-1),
      current(0)
  {}
};

class Quota
{
public:
  Quota(void);

  Quota(Filesystem *fs, const std::string &pool);

  Quota(Filesystem *fs, const std::string &pool, const std::string &name);

  Quota(const Quota &quota);

  Quota & operator=(const Quota &otherQuota);

  virtual ~Quota(void);

  int create(int64_t maxSize=-1);

  int remove(void);

  int setMaxSize(int64_t maxSize);

  int setUserMaxSize(uid_t uid, int64_t difference);

  int setGroupMaxSize(gid_t uid, int64_t difference);

  int updateCurrentSize(int64_t difference);

  int updateUserCurrentSize(uid_t uid, int64_t difference);

  int updateGroupCurrentSize(gid_t gid, int64_t difference);

  int updateCurrentSizes(int64_t currentSize,
                         const std::map<uid_t, int64_t> *userCurrentSize,
                         const std::map<uid_t, int64_t> *groupCurrentSize);

  int setQuotaSizes(const QuotaSize *quotaSize,
                    const std::map<uid_t, QuotaSize> *userQuota,
                    const std::map<gid_t, QuotaSize> *groupQuota);

  int update(void);

  QuotaSize getQuotaSize(void) const;

  int getUserQuota(uid_t uid, QuotaSize &quotaSize);

  int getGroupQuota(gid_t gid, QuotaSize &quotaSize);

  int getQuotaSizes(QuotaSize *quotaSize,
                    std::map<uid_t, QuotaSize> *userQuota,
                    std::map<gid_t, QuotaSize> *groupQuota) const;

  std::map<uid_t, QuotaSize> getUsersExceedingQuotas(int64_t difference = 0) const;

  std::map<gid_t, QuotaSize> getGroupsExceedingQuotas(int64_t difference = 0) const;

  std::string name(void) const;

  std::string pool(void) const;

  bool exists(void) const;

private:
  boost::scoped_ptr<QuotaPriv> mPriv;

  friend class ::RadosFsTest;
  friend class DirPriv;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_QUOTA_HH__
