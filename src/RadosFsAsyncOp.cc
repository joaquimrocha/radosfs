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

#include "RadosFsAsyncOp.hh"
#include "RadosFsAsyncOpPriv.hh"
#include "RadosFsLogger.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFsAsyncOpPriv::RadosFsAsyncOpPriv(const std::string &id)
  : id(id),
    complete(false),
    returnCode(-EINPROGRESS),
    ready(false)
{}

RadosFsAsyncOpPriv::~RadosFsAsyncOpPriv()
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
RadosFsAsyncOpPriv::waitForCompletion(void)
{
  while (returnCode == -EINPROGRESS)
  {
    if (!ready)
      continue;

    boost::unique_lock<boost::mutex> lock(mOpMutex);
    radosfs_debug("Async op with id='%s' will now wait for completion...",
                  id.c_str());

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

  return returnCode;
}

void
RadosFsAsyncOpPriv::addCompletion(librados::AioCompletion *comp)
{
  boost::unique_lock<boost::mutex> lock(mOpMutex);
  mOperations.push_back(comp);
}

void
RadosFsAsyncOpPriv::setReady()
{
  boost::unique_lock<boost::mutex> lock(mOpMutex);
  ready = true;
}

RadosFsAsyncOp::RadosFsAsyncOp(const std::string &id)
  : mPriv(new RadosFsAsyncOpPriv(id))
{}

RadosFsAsyncOp::~RadosFsAsyncOp()
{}

std::string
RadosFsAsyncOp::id(void)
{
  return mPriv->id;
}

bool
RadosFsAsyncOp::isFinished(void)
{
  return mPriv->complete;
}

int
RadosFsAsyncOp::returnValue(void)
{
  return mPriv->returnCode;
}

int
RadosFsAsyncOp::waitForCompletion(void)
{
  return mPriv->waitForCompletion();
}

RADOS_FS_END_NAMESPACE
