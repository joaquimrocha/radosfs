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

#include <boost/scoped_ptr.hpp>
#include <map>
#include <string>
#include <sys/types.h>
#include <stdint.h>
#include <vector>
#include <memory>

#include "Filesystem.hh"

RADOS_FS_BEGIN_NAMESPACE

class AyncOpPriv;
class FileIO;

class AsyncOp
{
public:
  AsyncOp(const std::string &id);
  ~AsyncOp(void);

  std::string id(void);
  bool isFinished(void);
  int returnValue(void);
  int waitForCompletion(void);
  void setCallback(AsyncOpCallback callback, void *arg);

private:
  boost::scoped_ptr<AyncOpPriv> mPriv;

  friend class FileIO;
};

RADOS_FS_END_NAMESPACE

#endif /* __RADOS_FS_ASYNC_OP_HH__ */
