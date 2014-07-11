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

#include <errno.h>
#include <regex.h>
#include <sys/stat.h>
#include <utility>

#include "radosfscommon.h"
#include "RadosFsFinder.hh"
#include "RadosFsLogger.hh"

#define DEFAULT_NUM_FINDER_THREADS 100
#define MAX_INACTIVE_THREAD_TIME 5

RADOS_FS_BEGIN_NAMESPACE

RadosFsFinder::RadosFsFinder(RadosFs *fs)
  : radosFs(fs),
    numThreads(0),
    availableThreads(0),
    maxNumThreads(DEFAULT_NUM_FINDER_THREADS)
{
  pthread_mutex_init(&dirsMutex, 0);
  pthread_mutex_init(&threadCountMutex, 0);
}

RadosFsFinder::~RadosFsFinder()
{
  pthread_mutex_destroy(&dirsMutex);
  pthread_mutex_destroy(&threadCountMutex);
}

void *
findInThread(void *finder)
{
  RadosFsFinder *finderObj = (RadosFsFinder *) finder;
  FinderData *data;

  struct timespec lastFind, currentTime;

  clock_gettime(CLOCK_REALTIME, &lastFind);

  while (true)
  {
    pthread_mutex_lock(&finderObj->dirsMutex);

    bool empty = finderObj->findQueue.empty();
    if (!empty)
    {
      data = finderObj->findQueue.front();
      finderObj->findQueue.pop();
    }

    pthread_mutex_unlock(&finderObj->dirsMutex);

    if (empty)
    {
      clock_gettime(CLOCK_REALTIME, &currentTime);

      if (currentTime.tv_sec - lastFind.tv_sec > MAX_INACTIVE_THREAD_TIME)
      {
        finderObj->updateNumThreads(-1);
        pthread_exit(0);
      }
      continue;
    }

    finderObj->updateAvailThreads(-1);

    pthread_mutex_lock(data->mutex);

    *data->numberRelatedJobs = *data->numberRelatedJobs - 1;

    if (data->retCode >= 0)
      *data->retCode = finderObj->realFind(data);

    if (*data->numberRelatedJobs == 0)
    {
      pthread_cond_signal(data->cond);
    }

    pthread_mutex_unlock(data->mutex);

    finderObj->updateAvailThreads(1);

    clock_gettime(CLOCK_REALTIME, &lastFind);
  }
}

int
makeRegexFromExp(const std::string &expression,
                 regex_t regex,
                 const std::string &entry)
{
  int ret = regexec(&regex, entry.c_str(), 0, 0, 0);

  if (ret != 0)
  {
    if (ret != REG_NOMATCH)
      radosfs_debug("Error looking for regex: %s", expression.c_str());
  }

  return ret;
}

int
checkEntryNameRegex(FinderData *data,
                    const RadosFsFinder::FindOptions option,
                    std::string &exp,
                    regex_t *regex)
{
  int ret = 0;
  int flags = 0;

  exp = data->args->at(option).valueStr;

  if (exp == "")
    return -EINVAL;

  if (data->args->at(option).valueInt == 1)
    flags = REG_ICASE;

  ret = regcomp(regex, exp.c_str(), flags);

  return ret;
}

int
RadosFsFinder::checkEntrySize(FinderData *data,
                              const std::string &entry,
                              const RadosFsDir &dir,
                              struct stat &buff)
{
  int ret = 0;
  const int sizeOptions[] = {FIND_SIZE_EQ,
                             FIND_SIZE_NE,
                             FIND_SIZE_GE,
                             FIND_SIZE_GT,
                             FIND_SIZE_LE,
                             FIND_SIZE_LT,
                             0
                            };

  for (int i = 0; sizeOptions[i] != 0; i++)
  {
    FindOptions option = static_cast<FindOptions>(sizeOptions[i]);

    if (data->args->count(option) == 0)
      continue;

    size_t size = data->args->at(option).valueInt;

    if (buff.st_nlink == 0)
    {
      ret = radosFs->stat(dir.path() + entry, &buff);

      if (ret != 0)
      {
        radosfs_debug("Error stating %s", (dir.path() + entry).c_str());
        break;
      }
    }

    ret = -1;

    switch (option)
    {
      case FIND_SIZE_EQ:
        if (buff.st_size == size)
          ret = 0;
        break;
      case FIND_SIZE_GE:
        if (buff.st_size >= size)
          ret = 0;
        break;
      case FIND_SIZE_GT:
        if (buff.st_size > size)
          ret = 0;
        break;
      case FIND_SIZE_LE:
        if (buff.st_size <= size)
          ret = 0;
        break;
      case FIND_SIZE_LT:
        if (buff.st_size < size)
          ret = 0;
        break;
      default:
        break;
    }

    if (ret != 0)
      break;
  }

  return ret;
}

int
RadosFsFinder::realFind(FinderData *data)
{
  int ret;
  std::string nameExpEQ = "", nameExpNE = "";
  regex_t nameRegexEQ, nameRegexNE;
  std::set<std::string> entries;
  RadosFsDir dir(radosFs, data->dir);
  std::set<std::string>::iterator it;

  dir.update();

  if (dir.isLink())
    return 0;

  if ((ret = dir.entryList(entries)) != 0)
    return ret;

  if (data->args->count(FIND_NAME_EQ))
  {
    ret = checkEntryNameRegex(data, FIND_NAME_EQ, nameExpEQ, &nameRegexEQ);

    if (ret != 0)
      goto bailout;
  }

  if (data->args->count(FIND_NAME_NE))
  {
    ret = checkEntryNameRegex(data,
                              FIND_NAME_NE,
                              nameExpNE,
                              &nameRegexNE);

    if (ret != 0)
      goto bailout;
  }

  for (it = entries.begin(); it != entries.end(); it++)
  {
    struct stat buff;
    buff.st_nlink = 0;

    const std::string &entry = *it;

    bool isDir = isDirPath(entry);

    if (isDir)
      data->dirEntries.insert(dir.path() + entry);

    if (nameExpEQ != "")
    {
      ret = makeRegexFromExp(nameExpEQ, nameRegexEQ, entry);

      if (ret != 0)
      {
        ret = 0;

        continue;
      }
    }

    if (nameExpNE != "" &&
        makeRegexFromExp(nameExpNE, nameRegexNE, entry) != REG_NOMATCH)
    {
        continue;
    }

    ret = checkEntrySize(data, entry, dir, buff);

    if (ret != 0)
    {
      ret = 0;
      continue;
    }

    data->results.insert(dir.path() + entry);
  }

bailout:
  if (nameExpEQ != "")
    regfree(&nameRegexEQ);

  if (nameExpNE != "")
    regfree(&nameRegexNE);

  return ret;
}

void
RadosFsFinder::updateNumThreads(int diff)
{
  pthread_mutex_lock(&threadCountMutex);

  numThreads += diff;
  availableThreads += diff;

  pthread_mutex_unlock(&threadCountMutex);
}

void
RadosFsFinder::updateAvailThreads(int diff)
{
  pthread_mutex_lock(&threadCountMutex);

  availableThreads += diff;

  pthread_mutex_unlock(&threadCountMutex);
}

void
RadosFsFinder::find(FinderData *data)
{
  pthread_mutex_lock(&dirsMutex);

  findQueue.push(data);

  pthread_mutex_unlock(&dirsMutex);

  pthread_mutex_lock(&threadCountMutex);

  bool createNewThread = numThreads < maxNumThreads && availableThreads == 0;

  pthread_mutex_unlock(&threadCountMutex);

  if (createNewThread)
  {
    updateNumThreads(1);

    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread, &attr, findInThread, this);
  }
}

RADOS_FS_END_NAMESPACE
