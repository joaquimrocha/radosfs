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

#ifndef __RADOS_FS_ASYNC_OP_PRIV_HH__
#define __RADOS_FS_ASYNC_OP_PRIV_HH__

#include <boost/thread/mutex.hpp>
#include <rados/librados.hpp>

#include "Filesystem.hh"
#include "radosfsdefines.h"

RADOS_FS_BEGIN_NAMESPACE

typedef std::vector<librados::AioCompletion *> CompletionList;

class AsyncOp;

class AyncOpPriv
{
public:
  AyncOpPriv(const std::string &id);
  ~AyncOpPriv(void);

  int waitForCompletion(void);
  void addCompletion(librados::AioCompletion *comp);
  void setReady(void);
  void setPartialReady(void);

  std::string id;
  bool complete;
  int returnCode;
  int ready;
  AsyncOpCallback callback;
  void *callbackArg;

private:
  boost::mutex mOpMutex;
  CompletionList mOperations;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_ASYNC_OP_PRIV_HH__
