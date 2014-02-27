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

#include <cstdio>
#include <errno.h>

#include "radosfsdefines.h"
#include "RadosFsIO.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsIO::RadosFsIO(const RadosFsPool *pool,
                     const std::string &path)
  : mPool(pool),
    mPath(path),
    mLazyRemoval(false)
{}

RadosFsIO::~RadosFsIO()
{
  if (mLazyRemoval)
    rados_remove(mPool->ioctx, mPath.c_str());
}

ssize_t
RadosFsIO::read(char *buff, off_t offset, size_t blen)
{
  sync();

  return rados_read(mPool->ioctx, mPath.c_str(), buff, blen, offset);
}

ssize_t
RadosFsIO::writeSync(const char *buff, off_t offset, size_t blen)
{
  sync();

  return rados_write(mPool->ioctx, mPath.c_str(), buff, blen, offset);
}

ssize_t
RadosFsIO::write(const char *buff, off_t offset, size_t blen)
{
  int ret;
  size_t compIndex, readBytes;
  readBytes = blen;

  if (((size_t) offset + blen) > mPool->size)
    return -EFBIG;

  rados_completion_t comp;
  mCompletionList.push_back(comp);
  compIndex = mCompletionList.size() - 1;

  rados_aio_create_completion(0, 0, 0, &mCompletionList[compIndex]);
  ret = rados_aio_write(mPool->ioctx, mPath.c_str(),
                        mCompletionList[compIndex], (const char *) buff,
                        blen, offset);

  // remove the completion object if something failed
  if (ret != 0)
  {
    std::vector<rados_completion_t>::iterator it = mCompletionList.begin();
    std::advance(it, compIndex);
    mCompletionList.erase(it);
    readBytes = 0;
  }

  return readBytes;
}

void
RadosFsIO::sync()
{
  std::vector<rados_completion_t>::iterator it;
  it = mCompletionList.begin();
  while (it != mCompletionList.end())
  {
    rados_aio_wait_for_complete(*it);
    rados_aio_release(*it);

    it = mCompletionList.erase(it);
  }
}

RADOS_FS_END_NAMESPACE
