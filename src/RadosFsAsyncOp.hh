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

#ifndef __RADOS_FS_ASYNC_OP_HH__
#define __RADOS_FS_ASYNC_OP_HH__

#include <map>
#include <string>
#include <sys/types.h>
#include <stdint.h>
#include <vector>
#include <memory>

#define RADOS_FS_BEGIN_NAMESPACE namespace radosfs {
#define RADOS_FS_END_NAMESPACE }

RADOS_FS_BEGIN_NAMESPACE

class RadosFsAsyncOpPriv;

class RadosFsAsyncOp
{
public:
  RadosFsAsyncOp(const std::string &id);
  ~RadosFsAsyncOp(void);

  std::string id(void);
  bool isFinished(void);
  int returnValue(void);
  int waitForCompletion(void);

private:
  std::auto_ptr<RadosFsAsyncOpPriv> mPriv;

friend class RadosFsIO;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_FS_ASYNC_OP_HH__ */
