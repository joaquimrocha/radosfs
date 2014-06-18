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
#include <cstdlib>
#include <errno.h>
#include <getopt.h>
#include <rados/librados.h>
#include <set>
#include <sstream>
#include <sys/stat.h>

#include "RadosFs.hh"
#include "RadosFsDir.hh"
#include "RadosFsFile.hh"
#include "radosfscommon.h"

#define CONF_ENV_VAR "RADOSFS_CLUSTER_CONF"
#define CLUSTER_CONF_ARG "conf"
#define CLUSTER_CONF_ARG_CHAR 'c'
#define FIX_ARG "fix"
#define FIX_ARG_CHAR 'f'
#define VERBOSE_ARG "verbose"
#define VERBOSE_ARG_CHAR 'v'
#define DRY_ARG "dry"
#define DRY_ARG_CHAR 'n'

static int
getObjectsFromCluster(rados_ioctx_t ioctx, std::set<std::string> &entries)
{
  int ret;
  rados_list_ctx_t list;

  if ((ret = rados_objects_list_open(ioctx, &list)) != 0)
    return ret;

  const char *obj, *c;
  while (rados_objects_list_next(list, &obj, &c) == 0 && obj != 0)
  {
    entries.insert(obj);
  }

  rados_objects_list_close(list);

  return 0;
}

static int
getListsOfIssues(radosfs::RadosFs &radosFs,
                 rados_ioctx_t ioctx,
                 std::set<std::string> &notIndexed,
                 std::set<std::string> &nonExistent)
{
  int ret;
  std::set<std::string>::iterator it;
  std::set<std::string> entries;
  std::map<std::string, std::set<std::string> > contentsMap;

  ret = getObjectsFromCluster(ioctx, entries);

  if (ret != 0)
    return ret;

  for(it = entries.begin(); it != entries.end(); it++)
  {
    std::string path(*it);
    const std::string &dirPath = radosfs::RadosFsDir::getParent(*it);
    std::string basePath(path, dirPath.length());
    std::set<std::string> dirContents;

    if (path[path.length() - 1] == PATH_SEP && contentsMap.count(path) == 0)
    {
      radosfs::RadosFsDir dir(&radosFs, path);

      dir.update();
      dir.entryList(dirContents);
      contentsMap[path] = dirContents;
    }

    if (dirPath == "")
      continue;

    if (contentsMap.count(dirPath) == 0)
    {
      nonExistent.insert(dirPath);
      notIndexed.insert(path);
      continue;
    }

    std::set<std::string> &contents = contentsMap[dirPath];
    if (contents.count(basePath) == 0)
    {
      notIndexed.insert(path);
    }
    else
    {
      contents.erase(basePath);
    }
  }

  std::map<std::string, std::set<std::string> >::iterator mapIt;
  for(mapIt = contentsMap.begin(); mapIt != contentsMap.end(); mapIt++)
  {
    std::set<std::string> cont = (*mapIt).second;
    for(it = cont.begin(); it != cont.end(); it++)
      nonExistent.insert((*mapIt).first + *it);
  }

  return 0;
}

static int
fsck(radosfs::RadosFs &radosFs,
     rados_t radosCluster,
     const std::string &poolName,
     bool fix,
     bool dry,
     bool verbose)
{
  std::set<std::string> entries;
  rados_ioctx_t ioctx;

  int ret = rados_ioctx_create(radosCluster, poolName.c_str(), &ioctx);

  if (ret != 0)
    return ret;

  std::set<std::string> notIndexed, nonExistent;

  ret = getListsOfIssues(radosFs, ioctx, notIndexed, nonExistent);

  if (ret != 0)
    return ret;

  const int numNonExistent = nonExistent.size();
  const int numNotIndexed = notIndexed.size();
  const int numIssues = numNonExistent + numNotIndexed;

  if (numIssues > 0)
    fprintf(stdout, "%d issues found\n", numIssues);
  else
    fprintf(stdout, "No issues found\n");

  if (fix)
    fprintf(stdout, "Fixing...\n");

  std::set<std::string>::iterator it;

  if (verbose && numNonExistent > 0)
    fprintf(stdout, "\n%d non-existent objects:\n", numNonExistent);

  for(it = nonExistent.begin(); it != nonExistent.end(); it++)
  {
    const char *action = "";
    const std::string &path(*it);
    bool isDir = path[path.length() - 1] == PATH_SEP;

    if (fix)
    {
      if (dry)
      {
        if (isDir)
          action = "Would create";
        else
          action = "Would deindex";
      }
      else
      {
        if (isDir)
        {
          radosfs::RadosFsDir dir(&radosFs, path);
          if (dir.create(-1, true) == 0)
            action = "Created";
          else
            action = "Failed to create";
        }
        else
        {
//          if (indexObject(ioctx, path, '-') >= 0)
//            action = "Deindexed";
//          else
//            action = "Failed to deindex";
        }
      }
    }

    if (verbose)
      fprintf(stdout, "\t%s %s\n", action, path.c_str());
  }

  if (verbose && numNotIndexed)
    fprintf(stdout, "\n%d not-indexed objects:\n", numNotIndexed);

  for(it = notIndexed.begin(); it != notIndexed.end(); it++)
  {
    const char *action = "";
    const std::string &path(*it);

    if (fix)
    {
      if (dry)
        action = "Would index";
      else
      {
//        if (indexObject(ioctx, path, '+') >= 0)
//          action = "Indexed";
//        else
//          action = "Failed to index";
      }
    }

    if (verbose)
      fprintf(stdout, "\t%s %s\n", action, path.c_str());
  }

  rados_ioctx_destroy(ioctx);

  return ret;
}

static void
showUsage(const char *name)
{
  fprintf(stderr, "Usage:\n%s [OPTIONS] --%s=CLUSTER_CONF POOL "
          "[POOL_PREFIX [POOL1 POOL_PREFIX1]]\n",
          name,
          CLUSTER_CONF_ARG
         );
  fprintf(stderr,
          "CLUSTER_CONF - path to the cluster's configuration file\n"
          "POOL         - is the name of the pool to be checked\n"
          "  if POOL_PREFIX is not specified, the root (/) will be used\n"
          "  more pools and prefixes can also be specified.\n"
          "\nOPTIONS can be:\n"
         );
  fprintf(stderr, "\t--%s, -%c \t fix the issues found\n",
          FIX_ARG, FIX_ARG_CHAR);
  fprintf(stderr, "\t--%s, -%c \t dry run (to use with the fix option), shows "
          "what would be done to fix the issues\n",
          DRY_ARG, VERBOSE_ARG_CHAR);
  fprintf(stderr, "\t--%s, -%c \t display more details about the issues\n",
          VERBOSE_ARG, VERBOSE_ARG_CHAR);
}

static int
parseArguments(int argc, char **argv,
               std::string &confPath,
               bool *fix,
               bool *dry,
               bool *verbose,
               int *position)
{
  confPath = "";
  const char *confFromEnv(getenv(CONF_ENV_VAR));

  if (confFromEnv != 0)
    confPath = confFromEnv;

  int optionIndex = 0;
  struct option options[] =
  {{CLUSTER_CONF_ARG, required_argument, 0, CLUSTER_CONF_ARG_CHAR},
   {FIX_ARG, no_argument, 0, FIX_ARG_CHAR},
   {DRY_ARG, no_argument, 0, DRY_ARG_CHAR},
   {VERBOSE_ARG, no_argument, 0, VERBOSE_ARG_CHAR},
   {0, 0, 0, 0}
  };

  int c;
  while ((c = getopt_long(argc, argv, "fnvc:", options, &optionIndex)) != -1)
  {
    switch(c)
    {
      case CLUSTER_CONF_ARG_CHAR:
        confPath = optarg;
        break;
      case DRY_ARG_CHAR:
        *dry = true;
        break;
      case FIX_ARG_CHAR:
        *fix = true;
        break;
      case VERBOSE_ARG_CHAR:
        *verbose = true;
        break;
      default:
        return -1;
    }
  }

  if (confPath == "")
  {
    fprintf(stderr, "Error: Please specify the " CONF_ENV_VAR " environment "
            "variable or use the --" CLUSTER_CONF_ARG "=... argument.\n");

    showUsage(argv[0]);

    return -1;
  }

  int numPosArgs = argc - optind;
  if (numPosArgs == 1 || (numPosArgs > 1 && (numPosArgs % 2) == 0))
  {
    *position = optind;

    return 0;
  }

  fprintf(stderr, "Please specify the pool name and prefix pairs...\n");

  showUsage(argv[0]);

  return -1;
}

int
main(int argc, char **argv)
{
  int ret;
  bool fix, dry, verbose;
  int position;
  std::string confPath;

  ret = parseArguments(argc, argv,
                       confPath,
                       &fix,
                       &dry,
                       &verbose,
                       &position);

  if (ret != 0)
    return ret;

  radosfs::RadosFs radosFs;
  radosFs.init("", confPath.c_str());

  int numPosArgs = argc - position;
  int i = position;

  while (i < position + numPosArgs)
  {
    const char *poolName, *pathPrefix;

    poolName = argv[i];

    if (numPosArgs == 1)
    {
      char root[] = {PATH_SEP, '\0'};
      pathPrefix = root;
    }
    else
      pathPrefix = argv[i + 1];


    if ((ret = radosFs.addPool(poolName, pathPrefix)) != 0)
    {
      fprintf(stderr, "Problem adding pool '%s'\n", poolName);
      showUsage(argv[0]);
      return ret;
    }

    i += 2;
  }

  rados_t cluster;

  rados_create(&cluster, 0);
  rados_conf_read_file(cluster, confPath.c_str());
  rados_connect(cluster);

  srand((unsigned int) clock());

  const std::vector<std::string> &pools = radosFs.pools();
  std::vector<std::string>::const_iterator it;
  for (it = pools.begin(); it != pools.end(); it++)
  {
    fprintf(stdout, "Checking pool %s... ", (*it).c_str());
    fsck(radosFs, cluster, *it, fix, dry, verbose);
  }

  rados_shutdown(cluster);

  return 0;
}
