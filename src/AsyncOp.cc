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

#include <rados/librados.hpp>

#include "AsyncOp.hh"
#include "AsyncOpPriv.hh"
#include "Logger.hh"

RADOS_FS_BEGIN_NAMESPACE

AyncOpPriv::AyncOpPriv(const std::string &id)
  : id(id),
    complete(false),
    returnCode(-EINPROGRESS),
    ready(-1),
    callback(0),
    callbackArg(0)
{}

AyncOpPriv::~AyncOpPriv()
{
  boost::unique_lock<boost::mutex> lock(mOpMutex);

  CompletionList::iterator it = mOperations.begin();
  while(it != mOperations.end())
  {
    (*it)->release();
    it = mOperations.erase(it);
  }
}

int
AyncOpPriv::waitForCompletion(void)
{
  while (returnCode == -EINPROGRESS)
  {
    boost::unique_lock<boost::mutex> lock(mOpMutex);

    if (ready != 0)
      continue;

    radosfs_debug("Async op with id='%s' will now wait for completion...",
                  id.c_str());

    if (mOperations.size() == 0)
    {
      radosfs_debug("Async op with id='%s' had no operations to complete. "
                    "Setting as complete.", id.c_str());
      returnCode = 0;
    }

    CompletionList::iterator it = mOperations.begin();
    while(it != mOperations.end())
    {
      librados::AioCompletion *completion = *it;
      completion->wait_for_complete();

      if (returnCode == -EINPROGRESS || returnCode == 0)
      {
        returnCode = completion->get_return_value();
      }

      completion->release();
      it = mOperations.erase(it);
    }
    radosfs_debug("Async op with id='%s' finished waiting for completion. "
                  "retcode=%d (%s)",
                  id.c_str(), returnCode, strerror(abs(returnCode)));
  }

  complete = true;

  if (callback)
  {
    callback(id, returnCode, callbackArg);
  }

  return returnCode;
}

void
AyncOpPriv::addCompletion(librados::AioCompletion *comp)
{
  boost::unique_lock<boost::mutex> lock(mOpMutex);
  mOperations.push_back(comp);

  if (ready < 0)
    ready = 1;
  else
    ready++;
}

void
AyncOpPriv::setReady()
{
  boost::unique_lock<boost::mutex> lock(mOpMutex);
  ready = 0;
}

void
AyncOpPriv::setPartialReady()
{
  boost::unique_lock<boost::mutex> lock(mOpMutex);
  if (ready > 0)
    ready--;
}

AsyncOp::AsyncOp(const std::string &id)
  : mPriv(new AyncOpPriv(id))
{}

AsyncOp::~AsyncOp()
{}

std::string
AsyncOp::id(void)
{
  return mPriv->id;
}

bool
AsyncOp::isFinished(void)
{
  return mPriv->complete;
}

int
AsyncOp::returnValue(void)
{
  return mPriv->returnCode;
}

int
AsyncOp::waitForCompletion(void)
{
  return mPriv->waitForCompletion();
}

void
AsyncOp::setCallback(AsyncOpCallback callback, void *arg)
{
  mPriv->callback = callback;
  mPriv->callbackArg = arg;
}

RADOS_FS_END_NAMESPACE
