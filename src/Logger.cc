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
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "Logger.hh"

RADOS_FS_BEGIN_NAMESPACE

Filesystem::LogLevel Logger::level = Filesystem::LOG_LEVEL_DEBUG;

void
readConfiguredLogLevel(void *fsLogger)
{
  FILE *fp;
  struct stat statBuff;
  size_t fileSize = 0;
  const int levelMaxChars = 10;
  char level[levelMaxChars];
  level[0] = '\0';
  Logger *logger = (Logger *) fsLogger;

  while (true)
  {
    boost::this_thread::interruption_point();

    if (stat(LOG_LEVEL_CONF_FILE, &statBuff) != 0)
      break;

    if (fileSize != (size_t) statBuff.st_size)
    {
      fileSize = statBuff.st_size;

      fp = fopen(LOG_LEVEL_CONF_FILE, "r");
      fgets(level, levelMaxChars, fp);

      fclose(fp);

      Filesystem::LogLevel previousLevel, newLevel;

      newLevel = logger->logLevel();
      previousLevel = newLevel;

      const char *levelNames[] = {"NONE", "DEBUG", 0};
      const Filesystem::LogLevel levels[] = {Filesystem::LOG_LEVEL_NONE,
                                     Filesystem::LOG_LEVEL_DEBUG};

      for (int i = 0; levelNames[i] != 0; i++)
      {
        if (strlen(level) < 2)
        {
          newLevel = Filesystem::LOG_LEVEL_NONE;
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

    boost::this_thread::interruption_point();
    boost::this_thread::sleep_for(boost::chrono::seconds(2));
  }
}

Logger::Logger()
  : thread(&readConfiguredLogLevel, this)
{}

Logger::~Logger()
{
  thread.interrupt();
  thread.join();
}

void
Logger::log(const char *file, const int line, const Filesystem::LogLevel msgLevel,
            const char *msg,
            ...)
{
  Filesystem::LogLevel currentLevel = Logger::level;

  if (currentLevel == Filesystem::LOG_LEVEL_NONE || (currentLevel & msgLevel) == 0)
    return;

  va_list args;

  va_start(args, msg);

  char *buffer = new char[mBufferMaxSize];

  vsnprintf(buffer, mBufferMaxSize, msg, args);

  struct timespec times;
  clock_gettime(CLOCK_REALTIME, &times);

  time_t _time = times.tv_sec;
  struct tm *currentTime = localtime(&_time);

  int milliseconds = round(static_cast<double>(times.tv_nsec / 1000000));

  fprintf(stderr, "RADOSFS DEBUG %d-%.2d-%.2d %.2d:%.2d:%.2d.%.03d, %s:%.2d -- %s\n",
          currentTime->tm_year + 1900,
          currentTime->tm_mon,
          currentTime->tm_mday,
          currentTime->tm_hour,
          currentTime->tm_min,
          currentTime->tm_sec,
          milliseconds,
          file,
          line,
          buffer);

  va_end(args);

  delete[] buffer;
}

void
Logger::setLogLevel(const Filesystem::LogLevel newLevel)
{
  boost::unique_lock<boost::mutex> lock(mLevelMutex);
  level = newLevel;
}

Filesystem::LogLevel
Logger::logLevel()
{
  Filesystem::LogLevel currentLevel;
  boost::unique_lock<boost::mutex> lock(mLevelMutex);

  currentLevel = level;

  return currentLevel;
}

RADOS_FS_END_NAMESPACE
