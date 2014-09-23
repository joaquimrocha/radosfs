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
#include <cstdarg>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "RadosFsLogger.hh"

RADOS_FS_BEGIN_NAMESPACE

RadosFs::LogLevel RadosFsLogger::level = RadosFs::LOG_LEVEL_DEBUG;

void *
readConfiguredLogLevel(void *fsLogger)
{
  FILE *fp;
  struct stat statBuff;
  size_t fileSize = 0;
  const int levelMaxChars = 10;
  char level[levelMaxChars];
  level[0] = '\0';
  RadosFsLogger *logger = (RadosFsLogger *) fsLogger;

  while (true)
  {
    if (stat(LOG_LEVEL_CONF_FILE, &statBuff) != 0)
      break;

    if (fileSize != (size_t) statBuff.st_size)
    {
      fileSize = statBuff.st_size;

      fp = fopen(LOG_LEVEL_CONF_FILE, "r");
      fgets(level, levelMaxChars, fp);

      fclose(fp);

      RadosFs::LogLevel previousLevel, newLevel;

      newLevel = logger->logLevel();
      previousLevel = newLevel;

      const char *levelNames[] = {"NONE", "DEBUG", 0};
      const RadosFs::LogLevel levels[] = {RadosFs::LOG_LEVEL_NONE,
                                          RadosFs::LOG_LEVEL_DEBUG};

      for (int i = 0; levelNames[i] != 0; i++)
      {
        if (strlen(level) < 2)
        {
          newLevel = RadosFs::LOG_LEVEL_NONE;
          break;
        }

        if (strncmp(level, levelNames[i], strlen(levelNames[i])) == 0)
        {
          newLevel = levels[i];
          break;
        }
      }

      if (newLevel != previousLevel)
      {
        logger->setLogLevel(newLevel);

        radosfs_debug("Logger level changed to %s", newLevel);
      }
    }

    sleep(2);
  }

  pthread_exit(0);
}

RadosFsLogger::RadosFsLogger()
{
  pthread_mutex_init(&mLevelMutex, 0);

  int ret = pthread_create(&thread, 0, readConfiguredLogLevel, this);

  if (ret != 0)
  {
    radosfs_debug("Could not create thread: %s", strerror(ret));
    return;
  }
}

RadosFsLogger::~RadosFsLogger()
{
  void *status;
  pthread_cancel(thread);
  pthread_join(thread, &status);
  pthread_mutex_destroy(&mLevelMutex);
}

void
RadosFsLogger::log(const char *file,
                   const int line,
                   const RadosFs::LogLevel msgLevel,
                   const char *msg,
                   ...)
{
  RadosFs::LogLevel currentLevel = RadosFsLogger::level;

  if (currentLevel == RadosFs::LOG_LEVEL_NONE || (currentLevel & msgLevel) == 0)
    return;

  va_list args;

  va_start(args, msg);

  char *buffer = new char[mBufferMaxSize];

  vsnprintf(buffer, mBufferMaxSize, msg, args);

  time_t _time;
  time(&_time);
  struct tm *currentTime = localtime(&_time);

  fprintf(stderr, "RADOSFS DEBUG %d-%.2d-%.2d %.2d:%.2d:%.2d, %s:%.2d -- %s\n",
          currentTime->tm_year + 1900,
          currentTime->tm_mon,
          currentTime->tm_mday,
          currentTime->tm_hour,
          currentTime->tm_min,
          currentTime->tm_sec,
          file,
          line,
          buffer);

  va_end(args);

  delete[] buffer;
}

void
RadosFsLogger::setLogLevel(const RadosFs::LogLevel newLevel)
{
  pthread_mutex_lock(levelMutex());

  level = newLevel;

  pthread_mutex_unlock(levelMutex());
}

RadosFs::LogLevel
RadosFsLogger::logLevel()
{
  RadosFs::LogLevel currentLevel;

  pthread_mutex_lock(levelMutex());

  currentLevel = level;

  pthread_mutex_unlock(levelMutex());

  return currentLevel;
}

RADOS_FS_END_NAMESPACE
