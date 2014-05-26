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

#ifndef RADOS_FS_OP_HH
#define RADOS_FS_OP_HH

#include <cstdlib>
#include <rados/librados.h>
#include <string>
#include <vector>

#include "RadosFs.hh"
#include "radosfscommon.h"

RADOS_FS_BEGIN_NAMESPACE

class RadosFsIO
{
public:
  RadosFsIO(const RadosFsPool *pool, const std::string &path);
  ~RadosFsIO();

  ssize_t read(char *buff, off_t offset, size_t blen);
  ssize_t write(const char *buff, off_t offset, size_t blen);
  ssize_t writeSync(const char *buff, off_t offset, size_t blen);

  std::string path(void) const { return mPath; }

  void setLazyRemoval(bool remove) { mLazyRemoval = remove; }
  bool lazyRemoval(void) const { return mLazyRemoval; }

private:
  const RadosFsPool *mPool;
  const std::string mPath;
  bool mLazyRemoval;
  std::vector<rados_completion_t> mCompletionList;

  void sync(void);
  void cleanCompletion(bool sync = false);
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_OP_HH */
