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
#include <limits>
#include <pthread.h>
#include <sstream>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

#include "BenchmarkMgr.hh"

#define CONF_ENV_VAR "RADOSFS_BENCHMARK_CLUSTER_CONF"
#define CLUSTER_CONF_ARG "conf"
#define DEFAULT_NUM_THREADS 10
#define LINES_PER_HEADER 30
#define CREATE_IN_DIR_CONF_ARG "create-in-dir"
#define CREATE_IN_DIR_CONF_ARG_CHAR 'd'

typedef struct
{
  int threadId;
  BenchmarkMgr *benchmark;
  std::vector<int> creationTimes;
  float minCreationTime;
  float maxCreationTime;
  bool shouldExit;
  bool exited;
} BenchmarkInfo;

void *
createFiles(void *bInfo)
{
  BenchmarkInfo *benchmarkInfo = (BenchmarkInfo *) bInfo;
  int threadId = benchmarkInfo->threadId;
  BenchmarkMgr *benchmark = benchmarkInfo->benchmark;
  benchmarkInfo->minCreationTime = std::numeric_limits<float>::max();
  benchmarkInfo->maxCreationTime = .0;

  std::stringstream prefix;
  prefix << "/t" << threadId;

  if (benchmark->createInDir())
  {
    prefix << "/";

    radosfs::RadosFsDir dir(&benchmark->radosFs, prefix.str());
    int ret = dir.create();

    if (ret != 0)
    {
      fprintf(stderr, "\nProblem creating directory %s: %s ... "
              "Exiting thread %d\n",
              prefix.str().c_str(), strerror(ret), threadId);

      goto exitThread;
    }
  }
  else
  {
    prefix << "-";
  }

  for (int i = 0; !benchmarkInfo->shouldExit; i++)
  {
    std::stringstream stream;
    stream << prefix.str() << i;

    struct timespec timeBefore, timeAfter;

    clock_gettime(CLOCK_REALTIME, &timeBefore);

    radosfs::RadosFsFile file(&benchmark->radosFs,
                              stream.str(),
                              radosfs::RadosFsFile::MODE_WRITE);

    int ret = file.create();

    clock_gettime(CLOCK_REALTIME, &timeAfter);

    if (ret == 0)
      benchmark->incFiles();
    else
    {
      fprintf(stderr, "Problem in thread %d: %s\n", threadId, strerror(ret));
      continue;
    }

    float diffTime = (float) (timeAfter.tv_sec - timeBefore.tv_sec);

    if (diffTime < benchmarkInfo->minCreationTime)
      benchmarkInfo->minCreationTime = diffTime;

    if (diffTime > benchmarkInfo->maxCreationTime)
      benchmarkInfo->maxCreationTime = diffTime;

  }

  exitThread:

  benchmarkInfo->exited = true;

  pthread_exit(0);
}

static void
showUsage(const char *name)
{
  fprintf(stderr, "Usage:\n%s DURATION [NUM_THREADS] [%sCLUSTER_CONF] "
          "[--%s]\n"
          "\tDURATION     - duration of the benchmark in seconds "
          "(has to be > 0)\n"
          "\tNUM_THREADS  - number of concurrent threads\n"
          "\tCLUSTER_CONF - path to the cluster's configuration file\n"
          "\t--%s, -%c - make each thread work inside its own directory "
          "instead of /\n",
          name,
          CLUSTER_CONF_ARG,
          CREATE_IN_DIR_CONF_ARG,
          CREATE_IN_DIR_CONF_ARG,
          CREATE_IN_DIR_CONF_ARG_CHAR);
}

static int
parseArguments(int argc, char **argv,
               std::string &confPath,
               int *runTime,
               int *numThreads,
               bool *createInDir,
               size_t *bufferSize,
               size_t *bufferDivision)
{
  confPath = "";
  const char *confFromEnv(getenv(CONF_ENV_VAR));
  int workers = -1;
  int duration = 0;
  int bufSize = 0;
  int bufDiv = 1;

  if (confFromEnv != 0)
    confPath = confFromEnv;

  int optionIndex = 0;
  struct option options[] =
  {{CLUSTER_CONF_ARG, required_argument, 0, CLUSTER_CONF_ARG[0]},
   {CREATE_IN_DIR_CONF_ARG, no_argument, 0, CREATE_IN_DIR_CONF_ARG_CHAR},
   {BUFFER_SIZE_ARG, required_argument, 0, BUFFER_SIZE_ARG_CHAR},
   {BUFFER_DIVISION_ARG, required_argument, 0, BUFFER_DIVISION_CHAR},
   {0, 0, 0, 0}
  };

  int c;
  std::string args;

  for (int i = 0; options[i].name != 0; i++)
  {
    args += options[i].val;

    if (options[i].has_arg != no_argument)
      args += ":";
  }

  while ((c = getopt_long(argc, argv, args.c_str(), options, &optionIndex)) != -1)
  {
    if (c == 'c')
      confPath = optarg;
    else if (c == CREATE_IN_DIR_CONF_ARG_CHAR)
      *createInDir = true;
    else if (c == BUFFER_SIZE_ARG_CHAR)
      bufSize = atoi(optarg);
    else if (c == BUFFER_DIVISION_CHAR)
      bufDiv = atoi(optarg);
  }

  if (confPath == "")
  {
    fprintf(stderr, "Error: Please specify the " CONF_ENV_VAR " environment "
            "variable or use the --" CLUSTER_CONF_ARG "=... argument.\n");

    return -1;
  }

  optionIndex = optind;

  if (optionIndex < argc)
    duration = atoi(argv[optionIndex]);

  if (duration <= 0)
  {
    fprintf(stderr, "Error: Please specify the duration of the benchmark\n");
    return -1;
  }

  if (bufSize < 0)
    bufSize = 0;

  if (bufDiv <= 0)
  {
    fprintf(stderr, "Error: The buffer needs to be written a positive number "
            "of times\n");
    return -1;
  }

  optionIndex++;

  if (optionIndex < argc)
    workers = atoi(argv[optionIndex]);

  *runTime = duration;

  if (workers <= 0)
    workers = DEFAULT_NUM_THREADS;

  *numThreads = workers;

  *bufferSize = bufSize;
  *bufferDivision = bufDiv;

  return 0;
}

int
main(int argc, char **argv)
{
  std::string confPath;
  int runTime, numThreads;
  bool createInDir;
  size_t bufferSize, bufferDivision;

  int ret = parseArguments(argc, argv, confPath, &runTime, &numThreads,
                           &createInDir, &bufferSize, &bufferDivision);

  if (ret != 0)
  {
    showUsage(argv[0]);
    return ret;
  }

  fprintf(stderr, "\n*** RadosFs Benchmark ***\n\n"
          "Running on cluster configured by %s "
          "for %d seconds with %d threads %s...\n",
          confPath.c_str(),
          runTime,
          numThreads,
          (createInDir ? "(using their own directory)": "(all writing to / )"));

  BenchmarkMgr benchmark(confPath.c_str());

  benchmark.radosFs.addPool(TEST_POOL, "/", 1000);
  benchmark.setCreateInDir(createInDir);

  pthread_attr_t attr;
  pthread_t threads[numThreads];
  BenchmarkInfo *infos[numThreads];

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  int i;

  for(i = 0; i < numThreads; i++)
  {
    BenchmarkInfo *info = new BenchmarkInfo;
    info->benchmark = &benchmark;
    info->exited = false;
    info->shouldExit = false;
    info->threadId = i;

    infos[i] = info;

    int ret = pthread_create(&threads[i], 0, createFiles, (void *) info);

    if (ret != 0)
    {
      fprintf(stderr, "ERROR: %d : %s", ret, strerror(ret));
      return ret;
    }

  }

  pthread_attr_destroy(&attr);

  time_t initialTime, currentTime;
  time(&initialTime);

  int countDown = runTime;
  float avgFilesPerSecond = .0;
  float avgFilesPerThread = .0;
  int currentNumFiles = 0;
  int numFiles = 0;

  while(countDown > 0)
  {
    time(&currentTime);

    if ((currentTime - initialTime) >= 1)
    {
      if (((runTime - countDown) % LINES_PER_HEADER) == 0)
      {
        fprintf(stdout, "\n%4s | %10s | %10s | %10s\n",
                "sec", "# files", "files/sec", "files/thread");
      }

      currentNumFiles = benchmark.numFiles();
      float totalCreated = currentNumFiles - numFiles;
      avgFilesPerSecond += totalCreated;
      avgFilesPerThread += totalCreated / numThreads;
      fprintf(stdout, "%4d | %10d | %10d | %8.2f\n",
              (runTime - countDown + 1),
              currentNumFiles,
              (int) totalCreated,
              totalCreated / numThreads);

      initialTime = currentTime;
      numFiles = currentNumFiles;
      countDown--;
    }
  }

  fprintf(stdout, "\nResult:\n\n");
  fprintf(stdout, "\tNumber of files:      %10d\n", currentNumFiles);
  fprintf(stdout, "\tAverage files/sec:    %10.2f\n", avgFilesPerSecond / runTime);
  fprintf(stdout, "\tAverage files/thread: %10.2f\n", avgFilesPerThread / runTime);

  float minCreationTime = std::numeric_limits<float>::max();
  float maxCreationTime = std::numeric_limits<float>::min();;

  for(i = 0; i < numThreads; i++)
  {
    void *status;
    infos[i]->shouldExit = true;
    int ret = pthread_join(threads[i], &status);

    if (infos[i]->minCreationTime < minCreationTime)
      minCreationTime = infos[i]->minCreationTime;

    if (infos[i]->maxCreationTime > maxCreationTime)
      maxCreationTime = infos[i]->maxCreationTime;

    if (ret != 0)
    {
      fprintf(stderr, "ERROR joining thread: %d : %s", ret, strerror(ret));
      return -1;
    }
  }

  fprintf(stdout, "\tMin creation time:    %10.2f sec\n", minCreationTime);
  fprintf(stdout, "\tMax creation time:    %10.2f sec\n", maxCreationTime);

  pthread_exit(0);

  return 0;
}
