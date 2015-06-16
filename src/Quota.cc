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

#include <climits>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <rados/librados.hpp>

#include "FilesystemPriv.hh"
#include "Quota.hh"
#include "QuotaPriv.hh"
#include "radosfscommon.h"
#include "radosfsdefines.h"

RADOS_FS_BEGIN_NAMESPACE

QuotaPriv::QuotaPriv(Filesystem *fs, const std::string &pool,
                     const std::string &name)
  : fs(fs),
    name(name),
    exists(false)
{
  if (fs && !pool.empty())
  {
    this->pool = fs->mPriv->getMtdPoolFromName(pool);
    load();
  }
}

QuotaPriv::~QuotaPriv(void)
{}

int
QuotaPriv::create(int64_t maxSize)
{
  if (!pool)
    return -ENODEV;

  std::stringstream stream;
  stream << maxSize;

  std::map<std::string, librados::bufferlist> omap;

  omap[XATTR_QUOTA_MAX_SIZE].append(stream.str());

  librados::ObjectWriteOperation op;

  op.create(true);
  op.omap_set(omap);

  int ret = pool->ioctx.operate(name, &op);

  if (ret == 0)
  {
    exists = true;
    quotaSize.max = maxSize;
  }

  return ret;
}

int
QuotaPriv::remove(void)
{
  if (!pool)
    return -ENODEV;

  if (!exists)
    return -ENOENT;

  return pool->ioctx.remove(name);
}

int
QuotaPriv::load()
{
  exists = false;

  if (!pool)
    return -ENODEV;

  std::map<std::string, librados::bufferlist> omap;
  int ret = this->pool->ioctx.omap_get_vals(name, "", XATTR_QUOTA_SIZE_PREFIX,
                                            UINT_MAX, &omap);

  if (ret != 0)
    return ret;

  users.clear();
  groups.clear();
  quotaSize = QuotaSize();

  std::map<std::string, librados::bufferlist>::iterator it;

  const size_t userCurSizePrefixLen =
      strlen(XATTR_QUOTA_CURRENT_SIZE_USER_PREFIX);
  const size_t userMaxSizePrefixLen =
      strlen(XATTR_QUOTA_MAX_SIZE_USER_PREFIX);
  const size_t groupCurSizePrefixLen =
      strlen(XATTR_QUOTA_CURRENT_SIZE_GROUP_PREFIX);
  const size_t groupMaxSizePrefixLen =
      strlen(XATTR_QUOTA_MAX_SIZE_GROUP_PREFIX);

  for (it = omap.begin(); it != omap.end(); it++)
  {
    const std::string &key = (*it).first;
    librados::bufferlist bl = (*it).second;
    std::string value(bl.c_str(), bl.length());

    if (key.compare(XATTR_QUOTA_CURRENT_SIZE) == 0)
    {
      quotaSize.current = (int64_t) strtod(value.c_str(), 0);
    }
    else if (key.compare(XATTR_QUOTA_MAX_SIZE) == 0)
    {
      quotaSize.max = (int64_t) strtod(value.c_str(), 0);
    }
    else if (key.compare(0, userCurSizePrefixLen,
                         XATTR_QUOTA_CURRENT_SIZE_USER_PREFIX) == 0)
    {
      std::string uidStr(key, userCurSizePrefixLen);
      uid_t uid = (uid_t) atoi(uidStr.c_str());

      if (uid == 0 && uidStr != "0")
        continue;

      users[uid].current = strtoul(value.c_str(), 0, 10);
    }
    else if (key.compare(0, userMaxSizePrefixLen,
                         XATTR_QUOTA_MAX_SIZE_USER_PREFIX) == 0)
    {
      std::string uidStr(key, userMaxSizePrefixLen);
      uid_t uid = (uid_t) atoi(uidStr.c_str());

      if (uid == 0 && uidStr != "0")
        continue;

      users[uid].max = strtoul(value.c_str(), 0, 10);
    }
    else if (key.compare(0, groupCurSizePrefixLen,
                         XATTR_QUOTA_CURRENT_SIZE_GROUP_PREFIX) == 0)
    {
      std::string gidStr(key, groupCurSizePrefixLen);
      gid_t gid = (gid_t) atoi(gidStr.c_str());

      if (gid == 0 && gidStr != "0")
        continue;

      groups[gid].current = strtoul(value.c_str(), 0, 10);
    }
    else if (key.compare(0, groupMaxSizePrefixLen,
                         XATTR_QUOTA_MAX_SIZE_GROUP_PREFIX) == 0)
    {
      std::string gidStr(key, groupMaxSizePrefixLen);
      gid_t gid = (gid_t) atoi(gidStr.c_str());

      if (gid == 0 && gidStr != "0")
        continue;

      groups[gid].max = strtoul(value.c_str(), 0, 10);
    }
  }

  exists = true;

  return ret;
}

int
QuotaPriv::updateQuota(const std::string &key, int64_t diff)
{
  librados::bufferlist in, out;

  size_t size = key.length();

  in.append((char *) &size, sizeof(size));
  in.append(key);

  std::stringstream stream;
  stream << diff;

  size = stream.str().length();

  in.append((char *) &size, sizeof(size));
  in.append(stream.str());

  int ret = pool->ioctx.exec(name, "numops", "add", in, out);

  return ret;
}

Quota::Quota()
  : mPriv(new QuotaPriv(0, "", QUOTA_OBJ_PREFIX + generateUuid()))
{
  mPriv->load();
}

Quota::Quota(Filesystem *fs, const std::string &pool)
  : mPriv(new QuotaPriv(fs, pool, QUOTA_OBJ_PREFIX + generateUuid()))
{
  mPriv->load();
}

Quota::Quota(Filesystem *fs, const std::string &pool, const std::string &name)
  : mPriv(new QuotaPriv(fs, pool, name))
{}

Quota::Quota(const Quota &quota)
  : mPriv(new QuotaPriv(quota.mPriv->fs, quota.mPriv->pool->name,
                        quota.mPriv->name))
{
  mPriv->quotaSize = quota.mPriv->quotaSize;
  mPriv->users = quota.mPriv->users;
  mPriv->groups = quota.mPriv->groups;
}

Quota &
Quota::operator=(const Quota &otherQuota)
{
  if (this != &otherQuota)
  {
    mPriv->fs = otherQuota.mPriv->fs;
    mPriv->pool = otherQuota.mPriv->pool;
    mPriv->name = otherQuota.mPriv->name;
    mPriv->quotaSize = otherQuota.mPriv->quotaSize;
    mPriv->users = otherQuota.mPriv->users;
    mPriv->groups = otherQuota.mPriv->groups;
  }

  return *this;
}

Quota::~Quota(void)
{}

int
Quota::create(int64_t maxSize)
{
  if (!mPriv->pool)
  {
    radosfs_debug("Error creating quota object '%s': does not have a valid "
                  "pool.", mPriv->name.c_str());
    return -ENODEV;
  }

  return mPriv->create(maxSize);
}

int
Quota::remove(void)
{
  return mPriv->remove();
}

int
Quota::update()
{
  return mPriv->load();
}

template<typename T>
static std::string
getKeyWithPrefix(const char *prefix, const T &key)
{
  std::stringstream stream;
  stream << prefix << key;
  return stream.str();
}

int
Quota::updateUserCurrentSize(uid_t uid, int64_t difference)
{
  const std::string key = getKeyWithPrefix(XATTR_QUOTA_CURRENT_SIZE_USER_PREFIX,
                                           uid);

  return mPriv->updateQuota(key, difference);
}

int
Quota::updateGroupCurrentSize(gid_t gid, int64_t difference)
{
  const std::string key = getKeyWithPrefix(XATTR_QUOTA_CURRENT_SIZE_GROUP_PREFIX,
                                           gid);

  return mPriv->updateQuota(key, difference);
}

int
Quota::setMaxSize(int64_t maxSize)
{
  QuotaSize size;
  size.max = maxSize;

  return setQuotaSizes(&size, 0, 0);
}

template<typename T>
static void encode(librados::bufferlist *in, const std::string &arg1, T arg2)
{
  int length = arg1.length();
  in->append((char *) &length, sizeof(length));
  in->append(arg1);

  std::stringstream stream;
  stream << arg2;

  length = stream.str().length();

  in->append((char *) &length, sizeof(length));
  in->append(stream.str());
}

int
Quota::updateCurrentSize(int64_t diff)
{
  std::stringstream stream;
  librados::bufferlist bl1, bl2;

  int length = strlen(XATTR_QUOTA_CURRENT_SIZE);
  bl1.append((char *) &length, sizeof(length));
  bl1.append(XATTR_QUOTA_CURRENT_SIZE);

  stream.str("");
  stream << diff;

  length = stream.str().length();

  bl1.append((char *) &length, sizeof(length));
  bl1.append(stream.str());

  int addRet;

  librados::ObjectWriteOperation op;
  op.exec("numops", "add", bl1, &bl2, &addRet);

  return mPriv->pool->ioctx.operate(mPriv->name, &op);
}

int
Quota::setUserMaxSize(uid_t uid, int64_t difference)
{
  const std::string key = getKeyWithPrefix(XATTR_QUOTA_MAX_SIZE_USER_PREFIX,
                                           uid);

  return mPriv->updateQuota(key, difference);
}

int
Quota::setGroupMaxSize(gid_t gid, int64_t difference)
{
  const std::string key = getKeyWithPrefix(XATTR_QUOTA_MAX_SIZE_GROUP_PREFIX,
                                           gid);

  return mPriv->updateQuota(key, difference);
}

int
Quota::updateCurrentSizes(int64_t currentSize,
                         const std::map<uid_t, int64_t> *userCurrentSize,
                         const std::map<uid_t, int64_t> *groupCurrentSize)
{
  librados::ObjectWriteOperation op;
  const size_t numUsers = userCurrentSize ? userCurrentSize->size() : 0;
  const size_t numGroups = groupCurrentSize ? groupCurrentSize->size() : 0;
  const size_t totalOps = 1 + numUsers + numGroups;

  librados::bufferlist *bls = new librados::bufferlist[totalOps * 2];
  size_t opCounter = 0;

  if (currentSize != 0)
  {
    librados::bufferlist *in, *out;
    in = &bls[opCounter];
    out = &bls[opCounter + 1];

    encode(in, XATTR_QUOTA_CURRENT_SIZE, currentSize);
    op.exec("numops", "add", *in, out, 0);
  }

  if (userCurrentSize)
  {
    std::map<uid_t, int64_t>::const_iterator it;
    librados::bufferlist *in, *out;

    for (it = userCurrentSize->begin(); it != userCurrentSize->end(); it++)
    {
      opCounter += 2;
      in = &bls[opCounter];
      out = &bls[opCounter + 1];

      uid_t user = (*it).first;
      int64_t size = (*it).second;

      encode(in, getKeyWithPrefix(XATTR_QUOTA_CURRENT_SIZE_USER_PREFIX, user),
             size);
      op.exec("numops", "add", *in, out, 0);
    }
  }

  if (groupCurrentSize)
  {
    std::map<gid_t, int64_t>::const_iterator it;
    librados::bufferlist *in, *out;

    for (it = groupCurrentSize->begin(); it != groupCurrentSize->end(); it++)
    {
      opCounter += 2;
      in = &bls[opCounter];
      out = &bls[opCounter + 1];

      gid_t group = (*it).first;
      int64_t size = (*it).second;

      encode(in, getKeyWithPrefix(XATTR_QUOTA_CURRENT_SIZE_GROUP_PREFIX, group),
             size);
      op.exec("numops", "add", *in, out, 0);
    }
  }

  delete [] bls;

  return mPriv->pool->ioctx.operate(mPriv->name, &op);
}

template<typename T>
static void
setOmapFromQuotasMap(std::map<std::string, librados::bufferlist> &omap,
                     const std::map<T, QuotaSize> *quota,
                     const char *maxSizePrefix, const char *currentSizePrefix)
{
  std::stringstream stream;
  typename std::map<T, QuotaSize>::const_iterator it;

  for (it = quota->begin(); it != quota->end(); it++)
  {
    T uid = (*it).first;
    const QuotaSize &size = (*it).second;

    if (size.max >= 0)
    {
      const std::string key = getKeyWithPrefix(maxSizePrefix, uid);
      stream << size.max;
      omap[key].append(stream.str());
      stream.str("");
    }

    if (size.current >= 0)
    {
      const std::string key = getKeyWithPrefix(currentSizePrefix, uid);
      stream << size.current;
      omap[key].append(stream.str());
      stream.str("");
    }
  }
}

int
Quota::setQuotaSizes(const QuotaSize *quotaSize,
                     const std::map<uid_t, QuotaSize> *userQuota,
                     const std::map<gid_t, QuotaSize> *groupQuota)
{
  std::map<std::string, librados::bufferlist> omap;
  std::stringstream stream;

  if (quotaSize)
  {
    if (quotaSize->max >= 0)
    {
      mPriv->quotaSize.max = quotaSize->max;
      stream << mPriv->quotaSize.max;
      omap[XATTR_QUOTA_MAX_SIZE].append(stream.str());
    }

    if (quotaSize->current >= 0)
    {
      stream.str("");
      mPriv->quotaSize.current = quotaSize->current;
      stream << mPriv->quotaSize.current;
      omap[XATTR_QUOTA_CURRENT_SIZE].append(stream.str());
    }
  }

  if (userQuota)
  {
    setOmapFromQuotasMap(omap, userQuota, XATTR_QUOTA_MAX_SIZE_USER_PREFIX,
                         XATTR_QUOTA_CURRENT_SIZE_USER_PREFIX);
  }

  if (groupQuota)
  {
    setOmapFromQuotasMap(omap, groupQuota, XATTR_QUOTA_MAX_SIZE_GROUP_PREFIX,
                         XATTR_QUOTA_CURRENT_SIZE_GROUP_PREFIX);
  }

  if (omap.empty())
    return -EINVAL;

  return mPriv->pool->ioctx.omap_set(mPriv->name, omap);
}

QuotaSize
Quota::getQuotaSize(void) const
{
  return mPriv->quotaSize;
}

int
Quota::getUserQuota(uid_t uid, QuotaSize &quotaSize)
{
  std::map<uid_t, QuotaSize>::const_iterator it;

  if ((it = mPriv->users.find(uid)) != mPriv->users.end())
  {
    quotaSize = (*it).second;
    return 0;
  }

  return -ENOENT;
}

int
Quota::getGroupQuota(gid_t gid, QuotaSize &quotaSize)
{
  std::map<gid_t, QuotaSize>::const_iterator it;

  if ((it = mPriv->groups.find(gid)) != mPriv->groups.end())
  {
    quotaSize = (*it).second;
    return 0;
  }

  return -ENOENT;
}

int
Quota::getQuotaSizes(QuotaSize *quotaSize,
                     std::map<uid_t, QuotaSize> *userQuota,
                     std::map<gid_t, QuotaSize> *groupQuota) const
{
  bool didSomething = false;

  if (quotaSize)
  {
    *quotaSize = mPriv->quotaSize;
    didSomething = true;
  }

  if (userQuota)
  {
    *userQuota = mPriv->users;
    didSomething = true;
  }

  if (groupQuota)
  {
    *groupQuota = mPriv->groups;
    didSomething = true;
  }

  if (!didSomething)
    return -EINVAL;

  return 0;
}

std::string
Quota::name(void) const
{
  return mPriv->name;
}

std::string
Quota::pool(void) const
{
  if (!mPriv->pool)
    return "";
  return mPriv->pool->name;
}

bool
Quota::exists(void) const
{
  return mPriv->exists;
}

template<typename T>
static std::map<T, QuotaSize>
getExceedingQuotas(const std::map<T, QuotaSize> &omap, int64_t difference)
{
  typename std::map<T, QuotaSize> quotas;
  typename std::map<T, QuotaSize>::const_iterator it;

  for (it = omap.begin(); it != omap.end(); it++)
  {
    const QuotaSize &size = (*it).second;
    if ((size.max - size.current) <= difference)
      quotas.insert(*it);
  }

  return quotas;
}

std::map<uid_t, QuotaSize>
Quota::getUsersExceedingQuotas(int64_t difference) const
{
  return getExceedingQuotas(mPriv->users, difference);
}

std::map<gid_t, QuotaSize>
Quota::getGroupsExceedingQuotas(int64_t difference) const
{
  return getExceedingQuotas(mPriv->groups, difference);
}

RADOS_FS_END_NAMESPACE
